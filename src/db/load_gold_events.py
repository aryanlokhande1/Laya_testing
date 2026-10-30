"""Bounded binary export and client-side COPY; atomic, never replace raw data."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import duckdb

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data.combine_parquets import ROOT, digest, ident, literal, save_json
from db.postgres import configuration, ensure_database, execute, query, command, safe_error, DatabaseUnavailable
from db.binary_copy import HEADER,TRAILER,binary_record

PG_TYPES = {'VARCHAR':'text','TIMESTAMP_NS':'timestamp without time zone',
            'BIGINT':'bigint','DOUBLE':'double precision'}


def load(report_path: Path, run_dir: Path, *, reuse: bool = False) -> dict:
    audit = json.loads(report_path.read_text(encoding='utf-8'))
    if audit.get('status') != 'verified' or not audit.get('exact_multiset_verified'):
        raise ValueError('A successful exact consolidation audit is required')
    source = Path(audit['consolidated_path'])
    if digest(source) != audit['consolidated_sha256']:
        raise ValueError('Consolidated file no longer matches its audit')
    for item in audit['sources']:
        if digest(ROOT / 'input' / item['filename']) != item['sha256_after']:
            raise ValueError('Original Parquet hash changed')
    env = configuration()
    status = ensure_database(env)
    c = duckdb.connect()
    c.execute("SET memory_limit='1GB'")
    c.read_parquet(str(source)).create_view('events')
    schema = [(r[0], r[1]) for r in c.execute('DESCRIBE events').fetchall()]
    if any(t not in PG_TYPES for _,t in schema):
        raise ValueError('Unreviewed PostgreSQL type conversion')
    if c.execute('SELECT count(*) FROM events WHERE epoch_ns(event_datetime)%1000<>0').fetchone()[0]:
        raise ValueError('Sub-microsecond timestamps require an explicit lossless storage design before loading')
    if query("SELECT to_regclass('raw.gold_events') IS NOT NULL present",env=env)[0]['present']:
        if not reuse:
            raise FileExistsError('raw.gold_events already exists; explicit --reuse verifies its manifest and contents')
        metadata = query("SELECT obj_description('raw.gold_events'::regclass) value",env=env)[0]['value']
        if not metadata or json.loads(metadata).get('sha256') != audit['consolidated_sha256']:
            raise ValueError('Existing raw table is not this audited ingestion; refusing reuse')
        # Full validation occurs again below, never relying on a comment alone.
    else:
        stage = 'gold_events_stage'
        definitions = ','.join(ident(n)+' '+PG_TYPES[t] for n,t in schema)
        prefix = f'''BEGIN;
SET LOCAL statement_timeout='90min';
SET LOCAL lock_timeout='10s';
SELECT pg_advisory_xact_lock(726251630);
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS features;
CREATE TABLE raw.{stage} ({definitions});
'''
        copy_sql=f'COPY raw.{stage} ({",".join(ident(n) for n,_ in schema)}) FROM STDIN WITH (FORMAT BINARY);'
        metadata = json.dumps({'sha256':audit['consolidated_sha256'],'rows':audit['combined_rows'],
                               'sources':[{k:s[k] for k in ('filename','sha256_after','rows')} for s in audit['sources']],
                               'deduplicated':False,'timestamp_contract':'naive, unresolved timezone',
                               'copy_transport':'client-side binary file COPY; transient file removed'})
        suffix = f'''
DO $check$ BEGIN IF (SELECT count(*) FROM raw.{stage}) <> {int(audit['combined_rows'])} THEN
RAISE EXCEPTION 'raw row-count validation failed'; END IF; END $check$;
ALTER TABLE raw.{stage} ADD CONSTRAINT gold_events_source_pk PRIMARY KEY(source_filename,source_row_number);
CREATE INDEX gold_events_context_time_idx ON raw.{stage}(context_id,event_datetime);
CREATE INDEX gold_events_time_idx ON raw.{stage}(event_datetime);
ALTER TABLE raw.{stage} RENAME TO gold_events;
COMMENT ON TABLE raw.gold_events IS {literal(metadata)};
ANALYZE raw.gold_events;
COMMIT;
'''
        # psql binary stdin is translated by Windows CRT. Client-side \copy
        # opens an explicit binary file, preserving CR/LF, NULL and numeric bytes.
        # The transient file is written in bounded batches, never a DataFrame.
        import shutil
        if shutil.disk_usage(ROOT).free < 15*1024**3:
            raise OSError('At least 15 GiB free is required for transient binary COPY and indexes')
        with tempfile.TemporaryDirectory(prefix='laya_binary_copy_') as tmp:
            binary_path=Path(tmp)/'events.bin'
            with binary_path.open('wb') as target:
                target.write(HEADER)
                projection=','.join('cast(event_datetime AS VARCHAR)' if n=='event_datetime' else ident(n) for n,_ in schema)
                result=c.execute('SELECT '+projection+' FROM events')
                sent=0
                while batch:=result.fetchmany(4096):
                    target.write(b''.join(binary_record(row,schema) for row in batch))
                    sent+=len(batch)
                    if sent%(4096*128)==0:print(f'Prepared binary COPY {sent:,} rows',flush=True)
                target.write(TRAILER)
            if sent!=audit['combined_rows'] or digest(source)!=audit['consolidated_sha256']:
                raise ValueError('Binary export count/hash mismatch; database has not been changed')
            copy_command='\\copy raw.'+stage+' FROM '+literal(binary_path.as_posix())+' WITH (FORMAT BINARY)\n'
            execute(prefix+copy_command+suffix,env=env)
        print('PostgreSQL COPY and index creation committed', flush=True)
    # Full column-level, NULL, timestamp and row-count checks, independent of COPY counters.
    raw = query('SELECT count(*) rows FROM raw.gold_events',env=env)[0]['rows']
    if raw != audit['combined_rows']:
        raise AssertionError('PostgreSQL raw count mismatch')
    names = [n for n,_ in schema]
    columns = query("SELECT column_name FROM information_schema.columns WHERE table_schema='raw' AND table_name='gold_events' ORDER BY ordinal_position",env=env)
    if [x['column_name'] for x in columns] != names:
        raise AssertionError('PostgreSQL raw source fields missing/reordered')
    nulls = query('SELECT '+','.join(f'count(*) FILTER(WHERE {ident(n)} IS NULL) AS {ident(n)}' for n in names)+' FROM raw.gold_events',env=env)[0]
    expected_nulls = c.execute('SELECT '+','.join(f'count(*) FILTER(WHERE {ident(n)} IS NULL)' for n in names)+' FROM events').fetchone()
    if [nulls[n] for n in names] != list(expected_nulls):
        raise AssertionError('PostgreSQL raw NULL counts mismatch')
    bounds = query('SELECT min(event_datetime)::text earliest,max(event_datetime)::text latest FROM raw.gold_events',env=env)[0]
    expected_bounds = c.execute('SELECT min(event_datetime)::VARCHAR,max(event_datetime)::VARCHAR FROM events').fetchone()
    if [bounds['earliest'],bounds['latest']] != list(expected_bounds):
        raise AssertionError('PostgreSQL timestamp bounds mismatch')
    # Verify every original scalar value by streaming PG CSV back and exact daily multisets.
    # See verify_raw_values for the independent post-load comparison.
    verify_raw_values(c, env, names)
    certificate=json.loads(query("SELECT obj_description('raw.gold_events'::regclass) value",env=env)[0]['value'])
    certificate['exact_values_verified']=True
    execute('COMMENT ON TABLE raw.gold_events IS '+literal(json.dumps(certificate))+';',env=env)
    status.update(raw_rows=raw,raw_columns=names,null_counts_verified=True,timestamp_bounds_verified=True,
                  exact_raw_values_verified=True,table='raw.gold_events',consolidated_sha256=audit['consolidated_sha256'])
    c.close()
    save_json(run_dir/'database_load.json',status)
    return status


def verify_raw_values(c, env, names):
    """Export provenance partitions to transient CSV; compare exact multisets.

    Temporary export filenames are random and deleted by TemporaryDirectory.
    Text values are base64 encoded to protect CR/LF from Windows text stdout.
    Timestamp text uses ISO formatting; DuckDB reads it back with explicit types.
    """
    schema = dict(c.execute('DESCRIBE events').fetchall()[i][:2] for i in range(len(names)))
    files = c.execute('SELECT source_filename,count(*) FROM events GROUP BY 1 ORDER BY 1').fetchall()
    partitions=[(name,start,min(start+100000,count)) for name,count in files for start in range(0,count,100000)]
    # Windows psql text stdout can normalize embedded CR/LF. Encode text fields
    # before CSV export and decode in DuckDB; never normalize customer values.
    projection = ','.join("replace(encode(convert_to("+ident(n)+",'UTF8'),'base64'),chr(10),'') AS "+ident(n) if schema[n]=='VARCHAR' else ident(n) for n in names)
    with tempfile.TemporaryDirectory(prefix='laya_verify_') as tmp:
        path = Path(tmp)/'partition.csv'
        for name,start,end in partitions:
            condition=f'source_filename={literal(name)} AND source_row_number>{start} AND source_row_number<={end}'
            sql = f"SET DateStyle='ISO, YMD'; COPY (SELECT {projection} FROM raw.gold_events WHERE {condition}) TO STDOUT WITH (FORMAT CSV,HEADER TRUE,NULL '');"
            with path.open('wb') as target, tempfile.TemporaryFile() as err:
                process = subprocess.run(command(env), input=sql.encode(),stdout=target,stderr=err,env=env,timeout=600)
                if process.returncode:
                    err.seek(0)
                    raise DatabaseUnavailable(safe_error(err.read().decode(errors='replace')))
            types = '{'+','.join(literal(n)+':'+literal(t) for n,t in schema.items())+'}'
            decoding=','.join('decode(from_base64('+ident(n)+')) AS '+ident(n) if schema[n]=='VARCHAR' else ident(n) for n in names)
            c.execute('CREATE OR REPLACE TEMP VIEW postgres_values AS SELECT '+decoding+' FROM read_csv('+literal(str(path))+',header=true,auto_detect=false,nullstr=\'\',allow_quoted_nulls=false,delim=\',\',quote=\'"\',escape=\'"\',new_line=\'\\r\\n\',columns='+types+')')
            diff = c.execute('SELECT count(*) FROM ((SELECT * FROM events WHERE '+condition+' EXCEPT ALL SELECT * FROM postgres_values) UNION ALL (SELECT * FROM postgres_values EXCEPT ALL SELECT * FROM events WHERE '+condition+'))').fetchone()[0]
            if diff:
                raise AssertionError('PostgreSQL row values differ from consolidated Parquet')
            c.execute('DROP VIEW postgres_values')
            print('Exact PostgreSQL value verification partition',name,start,end,flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--consolidation-report',type=Path,required=True)
    p.add_argument('--report-dir',type=Path,required=True)
    p.add_argument('--reuse',action='store_true')
    a=p.parse_args()
    try:
        status=load(a.consolidation_report,a.report_dir,reuse=a.reuse)
        print(json.dumps(status,indent=2))
    except DatabaseUnavailable as e:
        save_json(a.report_dir/'database_blocker.json',{'status':'blocked','reason':str(e),'raw_load_verified':False})
        print(str(e),file=sys.stderr)
        raise SystemExit(2)


if __name__=='__main__':
    main()
