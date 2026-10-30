"""Lossless UNION ALL with physical source-row provenance and exact duplicate audit."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

import duckdb

ROOT = Path(__file__).resolve().parents[2]
SOURCES = [('2026-03', 'events_gold_1_bucket_000_20260331.parquet'),
           ('2026-04', 'events_gold_2_bucket_000_20260430.parquet')]
PROVENANCE = ['source_filename', 'source_month', 'source_row_number']


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def records(c, sql, params=None):
    result = c.execute(sql, params or [])
    names = [x[0] for x in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def save_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + '\n', encoding='utf-8')


def consolidate(root: Path, report_dir: Path, output: Path | None = None) -> dict:
    """Refuse an existing output. Verify row-level values against physical row keys."""
    output = output or root / 'data/processed/gold_events_202603_202604.parquet'
    if output.exists():
        raise FileExistsError('Consolidated output already exists; validate/reuse explicitly, never overwrite')
    if shutil.disk_usage(root).free < 15 * 1024**3:
        raise OSError('At least 15 GiB free is required before consolidation; PostgreSQL needs additional space')
    output.parent.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect()
    c.execute("SET memory_limit='2GB'")
    c.execute('SET threads=2')
    temp = report_dir / 'duckdb_spill'
    temp.mkdir(exist_ok=True)
    c.execute('SET temp_directory=' + literal(str(temp)))
    report = {'started_at_utc': datetime.now(timezone.utc).isoformat(), 'status': 'running',
              'timezone_contract': 'Unresolved: source TIMESTAMP_NS without timezone; no conversion',
              'source_row_number': 'DuckDB physical file_row_number + 1, one-based, not a business event ID',
              'sources': [], 'deduplication': 'None; all source rows retained'}
    projections = []
    schema = None
    try:
        for i, (month, name) in enumerate(SOURCES):
            path = root / 'input' / name
            before = digest(path)
            view = f'source_{i}'
            c.execute(f'CREATE VIEW {view} AS SELECT * FROM read_parquet({literal(str(path))},file_row_number=true)')
            observed = [(r[0], r[1]) for r in c.execute(f'DESCRIBE {view}').fetchall() if r[0] != 'file_row_number']
            if any(name in dict(observed) for name in PROVENANCE):
                raise ValueError('Source column conflicts with provenance names')
            if schema is None:
                schema = observed
            if observed != schema:
                raise ValueError('Source schemas differ; explicit reviewed compatibility mapping required')
            columns = ','.join(ident(n) for n, _ in schema)
            null_sql = ','.join(f'count(*) FILTER(WHERE {ident(n)} IS NULL) AS {ident(n)}' for n, _ in schema)
            info = {'filename': name, 'month': month, 'sha256_before': before,
                    'rows': c.execute(f'SELECT count(*) FROM {view}').fetchone()[0],
                    'nulls': records(c, f'SELECT {null_sql} FROM {view}')[0],
                    'schema': [{'name': n, 'type': t} for n, t in schema]}
            report['sources'].append(info)
            projections.append(f'SELECT {columns},{literal(name)} AS source_filename,{literal(month)} AS source_month,file_row_number+1 AS source_row_number FROM {view}')
        c.execute('CREATE VIEW combined_source AS ' + ' UNION ALL '.join(projections))
        partial = output.with_suffix('.partial.parquet')
        if partial.exists():
            raise FileExistsError('Partial consolidated file exists; inspect it before retrying')
        print('Writing consolidated Parquet; original inputs are read-only', flush=True)
        c.execute(f'COPY (SELECT * FROM combined_source) TO {literal(str(partial))} (FORMAT PARQUET,COMPRESSION ZSTD)')
        c.read_parquet(str(partial)).create_view('combined')
        combined_schema = [(r[0], r[1]) for r in c.execute('DESCRIBE combined').fetchall() if r[0] not in PROVENANCE]
        if combined_schema != schema:
            raise AssertionError('Consolidation changed original source types or fields')
        count = c.execute('SELECT count(*) FROM combined').fetchone()[0]
        if count != sum(s['rows'] for s in report['sources']):
            raise AssertionError('Consolidated count mismatch')
        # Process each calendar day independently to bound wide exact equality/duplicate groups.
        # NULL dates are a separate partition, so no row can evade the audit.
        c.execute("CREATE VIEW audit_source AS SELECT *,cast(event_datetime AS DATE) AS audit_day FROM combined_source")
        days = c.execute('SELECT DISTINCT audit_day FROM audit_source ORDER BY 1').fetchall()
        duplicates = {'groups': 0, 'rows_in_duplicate_groups': 0, 'excess_rows': 0,
                      'cross_file_groups': 0, 'cross_file_rows': 0, 'cross_file_matched_pairs': 0}
        verify_value_partitions(c,report['sources'])
        for index,(day,) in enumerate(days):
            predicate = 'cast(event_datetime AS DATE) IS NOT DISTINCT FROM ?'
            d = c.execute(f'''WITH d AS (SELECT {columns},count(*) n,
                count(*) FILTER(WHERE source_month='2026-03') m,
                count(*) FILTER(WHERE source_month='2026-04') a
                FROM combined WHERE {predicate} GROUP BY {columns} HAVING count(*)>1)
                SELECT count(*),coalesce(sum(n),0),coalesce(sum(n-1),0),
                count(*) FILTER(WHERE m>0 AND a>0),coalesce(sum(n) FILTER(WHERE m>0 AND a>0),0),
                coalesce(sum(m*a),0) FROM d''', [day]).fetchone()
            for key, value in zip(duplicates, d):
                duplicates[key] += int(value)
            if index%10==0:print('Exact duplicate audit partition',index+1,'of',len(days),flush=True)
        report['duplicate_audit'] = duplicates
        report['duplication_risk'] = 'Event-row features retain duplicates and may be inflated relative to unique physical/business events; no approved deduplication rule.'
        report['combined_rows'] = count
        report['schema'] = [{'name': n, 'duckdb_type': t} for n, t in schema]
        report['partial_sha256_after_exact_audit']=digest(partial)
        save_json(report_dir/'audit_checkpoint.json',report)
        report['quality'] = profile_consolidated(c, schema)
        for source in report['sources']:
            source['sha256_after'] = digest(root / 'input' / source['filename'])
            if source['sha256_after'] != source['sha256_before']:
                raise AssertionError('Original source hash changed')
        partial.rename(output)
        report['consolidated_path'] = str(output.resolve())
        report['consolidated_sha256'] = digest(output)
        report['exact_multiset_verified'] = True
        report['status'] = 'verified'
        return report
    except Exception as exc:
        report['status'] = 'failed'
        report['error_type'] = type(exc).__name__
        raise
    finally:
        report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        save_json(report_dir / 'consolidation.json', report)
        c.close()


def profile_consolidated(c, schema):
    result = {}
    for name in ('event_name','journey_stage','journey_substage','channel','initiated_by'):
        result[name + '_distribution'] = records(c, f'SELECT {ident(name)} AS value,count(*) AS rows FROM combined GROUP BY 1 ORDER BY 2 DESC,1')
    result['timestamps'] = records(c, '''SELECT min(event_datetime)::VARCHAR earliest,max(event_datetime)::VARCHAR latest,
        count(*) FILTER(WHERE event_datetime IS NULL) null_rows,
        count(*) FILTER(WHERE epoch_ns(event_datetime)%1000<>0) submicrosecond_rows FROM combined''')[0]
    result['nulls'] = records(c, 'SELECT ' + ','.join(f'count(*) FILTER(WHERE {ident(n)} IS NULL) AS {ident(n)}' for n, _ in schema) + ' FROM combined')[0]
    result['blanks'] = records(c, 'SELECT ' + ','.join(f'count(*) FILTER(WHERE trim({ident(n)})=\'\') AS {ident(n)}' for n,t in schema if t=='VARCHAR') + ' FROM combined')[0]
    result['identifiers'] = records(c, '''SELECT count(DISTINCT context_id) FILTER(WHERE nullif(trim(context_id),'') IS NOT NULL) contexts,
        count(DISTINCT application_id) FILTER(WHERE nullif(trim(application_id),'') IS NOT NULL) applications,
        count(DISTINCT session_id) FILTER(WHERE nullif(trim(session_id),'') IS NOT NULL) sessions,
        count(*) FILTER(WHERE nullif(trim(context_id),'') IS NULL) missing_context_rows,
        count(*) FILTER(WHERE visitnum IS NOT NULL) recorded_visitnum_rows FROM combined''')[0]
    result['application_relationships'] = records(c, '''WITH a AS (SELECT application_id,count(DISTINCT context_id) n FROM combined
        WHERE nullif(trim(application_id),'') IS NOT NULL GROUP BY 1)
        SELECT count(*) applications,count(*) FILTER(WHERE n>1) applications_with_multiple_contexts FROM a''')[0]
    result['timestamp_ties'] = records(c, '''WITH t AS (SELECT context_id,event_datetime,count(*) n FROM combined
        WHERE nullif(trim(context_id),'') IS NOT NULL GROUP BY 1,2 HAVING count(*)>1)
        SELECT count(*) tied_context_timestamp_groups,coalesce(sum(n),0) rows_in_ties FROM t''')[0]
    result['message_matching_audit'] = records(c, '''WITH k AS (SELECT context_id,channel,channel_id,
        count(*) FILTER(WHERE event_name='campaign_sent') sends,
        count(*) FILTER(WHERE event_name='campaign_open_read') opens FROM combined
        WHERE nullif(trim(channel_id),'') IS NOT NULL GROUP BY 1,2,3)
        SELECT count(*) nonblank_candidate_keys,count(*) FILTER(WHERE sends>1) keys_with_multiple_sends,
        count(*) FILTER(WHERE sends>0 AND opens>0) keys_with_sends_and_opens FROM k''')[0]
    result['message_matching_status'] = 'No verified message ID, unique send/open attribution, telemetry availability or complete response-window contract. channel_id is not assumed to identify a message.'
    result['unapproved_nonblank_channels'] = records(c, "SELECT channel,count(*) AS rows FROM combined WHERE nullif(trim(channel),'') IS NOT NULL AND channel NOT IN ('SMS','WA','GRCS') GROUP BY 1")
    return result


def verify_value_partitions(c,sources):
    """Equal per-key-range cardinalities plus exact EXCEPT ALL prove equality.

    This handles NULLs/duplicates, and bounds each wide hash comparison to 100k
    source rows. A global count separately detects any unexpected provenance key.
    """
    for source in sources:
        for start in range(0,source['rows'],100000):
            predicate='source_filename=? AND source_row_number>? AND source_row_number<=?'
            parameters=[source['filename'],start,start+100000]
            expected=c.execute('SELECT count(*) FROM combined_source WHERE '+predicate,parameters).fetchone()[0]
            observed=c.execute('SELECT count(*) FROM combined WHERE '+predicate,parameters).fetchone()[0]
            difference=c.execute('SELECT count(*) FROM (SELECT * FROM combined_source WHERE '+predicate+' EXCEPT ALL SELECT * FROM combined WHERE '+predicate+')',parameters*2).fetchone()[0]
            if observed!=expected or difference:
                raise AssertionError('Consolidated file does not exactly preserve source rows and provenance')
            if start%1000000==0:print('Verified provenance partition',source['month'],start,flush=True)


def resume_audited_partial(root: Path, previous_report: Path, report_dir: Path) -> dict:
    """Recover a completed row/duplicate audit whose later profiling step failed.

    Recheck all hashes/counts and a fresh exact multiset comparison. Never accept
    just the saved status or a matching row count. Preserve the failed report.
    """
    report=json.loads(previous_report.read_text(encoding='utf-8'))
    if report.get('status')!='failed' or not all(k in report for k in ['duplicate_audit','combined_rows','schema']):
        raise ValueError('Partial recovery requires a completed prior exact/duplicate audit')
    output=root/'data/processed/gold_events_202603_202604.parquet'
    partial=output.with_suffix('.partial.parquet')
    if output.exists() or not partial.exists():
        raise FileExistsError('Recovery requires a partial file and no published consolidated file')
    if (report_dir/'consolidation.json').exists():
        raise FileExistsError('Recovery report already exists')
    c=duckdb.connect()
    c.execute("SET memory_limit='2GB'")
    c.execute('SET threads=2')
    report_dir.mkdir(parents=True,exist_ok=True)
    spill=report_dir/'duckdb_spill';spill.mkdir(exist_ok=True)
    c.execute('SET temp_directory='+literal(str(spill)))
    parts=[]
    for source in report['sources']:
        path=root/'input'/source['filename']
        if digest(path)!=source['sha256_before']:
            raise ValueError('Original source hash differs from completed duplicate audit')
        parts.append('SELECT * EXCLUDE(file_row_number),'+literal(source['filename'])+' AS source_filename,'+
                     literal(source['month'])+' AS source_month,file_row_number+1 AS source_row_number FROM read_parquet('+
                     literal(str(path))+',file_row_number=true)')
    c.execute('CREATE VIEW combined_source AS '+' UNION ALL '.join(parts))
    c.read_parquet(str(partial)).create_view('combined')
    source_count=c.execute('SELECT count(*) FROM combined_source').fetchone()[0]
    count=c.execute('SELECT count(*) FROM combined').fetchone()[0]
    if count!=source_count or count!=report['combined_rows']:
        raise AssertionError('Recovery count mismatch')
    print('Rechecking exact full-row multiset after profiling failure',flush=True)
    verify_value_partitions(c,report['sources'])
    schema=[(r['name'],r['duckdb_type']) for r in report['schema']]
    if [(r[0],r[1]) for r in c.execute('DESCRIBE combined').fetchall() if r[0] not in PROVENANCE]!=schema:
        raise AssertionError('Recovery schema mismatch')
    report['quality']=profile_consolidated(c,schema)
    for source in report['sources']:
        source['sha256_after']=digest(root/'input'/source['filename'])
        if source['sha256_after']!=source['sha256_before']:raise AssertionError('Source changed during recovery')
    c.close()
    partial.rename(output)
    report.update(status='verified',recovered_from=str(previous_report.resolve()),
                  recovery_verification='Fresh exact EXCEPT ALL plus equal cardinalities; prior exact duplicate audit retained under unchanged source hashes',
                  consolidated_path=str(output.resolve()),consolidated_sha256=digest(output),exact_multiset_verified=True,
                  finished_at_utc=datetime.now(timezone.utc).isoformat())
    report.pop('error_type',None)
    save_json(report_dir/'consolidation.json',report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--resume-partial-audit',type=Path)
    args = parser.parse_args()
    result = resume_audited_partial(ROOT,args.resume_partial_audit,args.report_dir) if args.resume_partial_audit else consolidate(ROOT, args.report_dir, args.output)
    print(json.dumps({'status':result['status'],'combined_rows':result['combined_rows'],'duplicate_audit':result['duplicate_audit']}))


if __name__ == '__main__':
    main()
