"""Read-only preservation, exact-payload duplicate and application-resolution audits."""
from datetime import datetime
import json
from pathlib import Path
import tempfile

import duckdb

from data.combine_parquets import ROOT,PROVENANCE,digest,ident,literal,save_json
from db.postgres import configuration,execute,query
from db.binary_copy import HEADER,TRAILER,binary_record

ORIGINALS={'raw.gold_events':('source_filename','source_row_number'),
           'features.customer_features':('context_id',),
           'features.feature_dictionary':('column_name',)}


def fingerprint_originals(report_dir: Path, label: str, env: dict) -> dict:
    """SHA256 of ordered SHA256(row::text) records, plus complete schema metadata.

    Row digests are exported through client-side COPY, never customer records.
    Fixed DateStyle/float settings make typed record serialization reproducible.
    File hashing is incremental; only aggregate digests enter generated reports.
    """
    result={}
    with tempfile.TemporaryDirectory(prefix='laya_state_fingerprint_') as tmp:
        path=Path(tmp)/'row_digests.csv'
        for table,keys in ORIGINALS.items():
            print('Fingerprinting protected table',table,label,flush=True)
            sql="SET DateStyle='ISO, YMD'; SET extra_float_digits=3;\n"
            sql+="\\copy (SELECT encode(sha256(convert_to(t::text,'UTF8')),'hex') FROM "+table+' t ORDER BY '+','.join(map(ident,keys))+') TO '+literal(path.as_posix())+' WITH (FORMAT CSV)\n'
            execute(sql,env=env)
            count=0
            with path.open('rb') as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):count+=block.count(b'\n')
            schema=query("SELECT a.attnum,a.attname,format_type(a.atttypid,a.atttypmod) sql_type,a.attnotnull,pg_get_expr(d.adbin,d.adrelid) default_expression FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid="+literal(table)+"::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",env=env)
            metadata=query("SELECT pg_get_userbyid(relowner) owner,relacl::text privileges,obj_description(oid) table_comment FROM pg_class WHERE oid="+literal(table)+'::regclass',env=env)[0]
            constraints=query("SELECT conname,pg_get_constraintdef(oid) definition FROM pg_constraint WHERE conrelid="+literal(table)+'::regclass ORDER BY conname',env=env)
            indexes=query("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname="+literal(table.split('.')[0])+' AND tablename='+literal(table.split('.')[1])+' ORDER BY indexname',env=env)
            result[table]={'rows':count,'ordered_row_digest_sha256':digest(path),'columns':schema,
                           'metadata':metadata,'constraints':constraints,'indexes':indexes}
    save_json(report_dir/f'protected_tables_{label}.json',result)
    return result


def duplicate_exposure(consolidated: Path, snapshot: str, binary_output: Path, report_dir: Path) -> dict:
    """Candidate hash grouping followed by exact equality of ALL 24 payload fields.

    Hash collisions can only add candidates; final equality is exact and NULL-safe.
    Provenance is excluded from equality, and no source event is removed.
    Only per-context aggregate duplicate exposure is exported for the SQL build.
    """
    c=duckdb.connect()
    c.execute("SET memory_limit='2GB'")
    c.execute('SET threads=2')
    spill=report_dir/'duckdb_spill';spill.mkdir(parents=True,exist_ok=True)
    c.execute('SET temp_directory='+literal(str(spill)))
    c.read_parquet(str(consolidated)).create_view('source')
    schema=c.execute('DESCRIBE source').fetchall()
    columns=[r[0] for r in schema if r[0] not in PROVENANCE]
    if len(columns)!=24:raise ValueError('Expected the preserved 24-field source payload')
    c.execute('CREATE TEMP VIEW historical AS SELECT * FROM source WHERE event_datetime < CAST('+literal(snapshot)+' AS TIMESTAMP_NS)')
    payload=','.join(map(ident,columns))
    hashed='hash(ROW('+payload+'))'
    print('Auditing exact source-payload duplicates with bounded candidate verification',flush=True)
    c.execute('CREATE TEMP TABLE candidates AS SELECT '+hashed+' h FROM historical GROUP BY 1 HAVING count(*)>1')
    c.execute('CREATE TEMP TABLE exact_duplicates AS SELECT '+payload+",count(*) n,count(DISTINCT source_filename) files FROM historical WHERE "+hashed+' IN(SELECT h FROM candidates) GROUP BY '+payload+' HAVING count(*)>1')
    summary=c.execute('SELECT count(*),coalesce(sum(n-1),0),coalesce(sum(n),0),count(*) FILTER(WHERE files>1),count(DISTINCT context_id) FROM exact_duplicates').fetchone()
    result=dict(zip(['exact_duplicate_groups','excess_rows','rows_in_groups','cross_file_groups','affected_contexts'],map(int,summary)))
    c.execute("CREATE TEMP TABLE exposure AS SELECT context_id,count(*) duplicate_groups,sum(n-1)::BIGINT excess_rows,sum(n)::BIGINT rows_in_groups,sum(CASE WHEN event_name IN('campaign_sent','campaign_open_read','campaign_clicked') THEN n-1 ELSE 0 END)::BIGINT communication_excess_rows,sum(CASE WHEN event_name IS NOT NULL AND trim(event_name)<>'' AND event_name NOT IN('campaign_sent','campaign_open_read','campaign_clicked') THEN n-1 ELSE 0 END)::BIGINT non_campaign_excess_rows,count(*) FILTER(WHERE files>1) cross_file_groups FROM exact_duplicates WHERE context_id IS NOT NULL AND trim(context_id)<>'' GROUP BY 1")
    cursor=c.execute('SELECT * FROM exposure ORDER BY context_id')
    transport_schema=[('context_id','VARCHAR')]+[(name,'BIGINT') for name in ['duplicate_groups','excess_rows','rows_in_groups','communication_excess_rows','non_campaign_excess_rows','cross_file_groups']]
    with binary_output.open('wb') as target:
        target.write(HEADER)
        while batch:=cursor.fetchmany(4096):
            target.write(b''.join(binary_record(row,transport_schema) for row in batch))
        target.write(TRAILER)
    c.close()
    result['method']='64-bit candidate hashes, then exact GROUP BY all 24 original fields; no hash-only equality assumption'
    result['deduplicated']=False
    save_json(report_dir/'duplicate_audit.json',result)
    return result


def resolve_application_context(application_id: str, *, snapshot='2026-05-01 00:00:00', env=None) -> dict:
    """Read-only exact lookup. Return all candidates, never choose a latest context.

    Identifiers are returned only to the local caller for lookup; CLI displays just
    status/count. A blank application identifier is not resolved to missing rows.
    """
    if not isinstance(application_id,str) or not application_id.strip():
        return {'status':'no_matching_context','context_count':0,'context_ids':[]}
    value=datetime.fromisoformat(snapshot)
    if value.tzinfo is not None:raise ValueError('Snapshot must remain timezone-naive')
    matches=query('SELECT DISTINCT context_id FROM raw.gold_events WHERE application_id='+literal(application_id)+
                  ' AND event_datetime<'+literal(snapshot)+"::timestamp AND nullif(btrim(context_id),'') IS NOT NULL ORDER BY context_id",env=env or configuration(prefer_application=False))
    ids=[r['context_id'] for r in matches]
    return {'status':'no_matching_context' if not ids else 'unique_context' if len(ids)==1 else 'multiple_contexts',
            'context_count':len(ids),'context_ids':ids}
