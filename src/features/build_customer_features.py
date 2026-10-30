"""Build all approved families with explicit unavailable fields and atomic publication."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from data.combine_parquets import ROOT, digest, literal, ident, save_json
from db.postgres import configuration, execute, query


def validate_options(snapshot: str, sequence_length: int, trend_days: int, pressure_windows: list[int]):
    fractional=re.search(r'\.(\d+)',snapshot)
    if fractional and len(fractional[1])>6:
        raise ValueError('PostgreSQL snapshot precision is at most six fractional digits; no silent truncation')
    value=datetime.fromisoformat(snapshot)
    if value.tzinfo is not None:
        raise ValueError('Snapshot must use the unresolved source timezone-naive convention')
    if not 1<=sequence_length<=1000 or not 1<=trend_days<=3660:
        raise ValueError('Sequence length/window outside supported range')
    if not pressure_windows or len(set(pressure_windows))!=len(pressure_windows) or any(not 1<=d<=3660 for d in pressure_windows):
        raise ValueError('Pressure windows must be distinct positive day counts <=3660')
    return value.isoformat(sep=' ')


def dictionary_sql(definitions: list[dict], stage: str) -> str:
    fields=['column_name','meaning','source_fields','formula','implementation','feature_family','observability','limitations','relevance_to_laya']
    values=[]
    for item in definitions:
        row=[]
        for field in fields:
            value=item[field]
            row.append(literal(json.dumps(value))+'::jsonb' if field=='source_fields' else str(value) if field=='feature_family' else literal(value))
        values.append('('+','.join(row)+')')
    return '''CREATE TABLE features.feature_dictionary_stage AS
SELECT d.column_name,c.data_type AS sql_type,d.meaning,d.source_fields,d.formula,d.implementation,
d.feature_family,d.observability,d.limitations,d.relevance_to_laya
FROM (VALUES '''+','.join(values)+') AS d('+','.join(fields)+''')
JOIN information_schema.columns c ON c.table_schema='features' AND c.table_name='''+literal(stage)+''' AND c.column_name=d.column_name;
ALTER TABLE features.feature_dictionary_stage ALTER COLUMN column_name SET NOT NULL;
ALTER TABLE features.feature_dictionary_stage ADD PRIMARY KEY(column_name);
'''


def build(report_dir: Path, snapshot='2026-05-01 00:00:00', sequence_length=8,
          trend_days=7, pressure_windows=None, archive_existing=False):
    pressure_windows=[1,7,30] if pressure_windows is None else pressure_windows
    snapshot=validate_options(snapshot,sequence_length,trend_days,pressure_windows)
    env=configuration()
    if not query("SELECT to_regclass('raw.gold_events') IS NOT NULL present",env=env)[0]['present']:
        raise ValueError('Verified raw.gold_events must be loaded first')
    manifest=query("SELECT obj_description('raw.gold_events'::regclass) value",env=env)[0]['value']
    if not manifest or 'sha256' not in json.loads(manifest):
        raise ValueError('Raw table lacks the audited pipeline manifest')
    if not json.loads(manifest).get('exact_values_verified'):
        raise ValueError('Raw manifest does not certify completed exact PostgreSQL value verification')
    channels=query("SELECT count(*) n FROM raw.gold_events WHERE nullif(btrim(channel),'') IS NOT NULL AND channel NOT IN ('SMS','WA','GRCS')",env=env)[0]['n']
    if channels:
        raise ValueError('Unapproved nonblank channels exist; review rather than rename or omit them')
    stage='customer_features_stage'
    definitions=json.loads((ROOT/'sql/features/feature_definitions.json').read_text(encoding='utf-8'))
    run_id=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    sql="BEGIN; SET LOCAL statement_timeout='90min'; SET LOCAL lock_timeout='10s'; SELECT pg_advisory_xact_lock(726251630);\n"
    existing=query("SELECT to_regclass('features.customer_features') IS NOT NULL present",env=env)[0]['present']
    if existing:
        if not archive_existing:
            raise FileExistsError('Feature table exists. Use --archive-existing to preserve the old managed table and dictionary before publishing')
        comment=query("SELECT obj_description('features.customer_features'::regclass) value",env=env)[0]['value']
        if not comment or not json.loads(comment).get('laya_feature_pipeline'):
            raise ValueError('Existing feature table is not managed by this pipeline; refusing to rename it')
        sql+=f'ALTER TABLE features.customer_features RENAME TO customer_features_archive_{run_id};\n'
        sql+=f'ALTER TABLE features.feature_dictionary RENAME TO feature_dictionary_archive_{run_id};\n'
        # Their PK indexes retain original names, so new constraints use unique run names.
    sql+=(ROOT/'sql/features/customer_aggregates.sql').read_text(encoding='utf-8')
    sql+=f'ALTER TABLE features.{stage} ADD CONSTRAINT cf_pk_{run_id} PRIMARY KEY(context_id);\n'
    sql+=dictionary_sql(definitions,stage).replace('ADD PRIMARY KEY(column_name)',f'ADD CONSTRAINT fd_pk_{run_id} PRIMARY KEY(column_name)')
    sql+=(ROOT/'sql/features/verify_features.sql').read_text(encoding='utf-8')
    metadata={'laya_feature_pipeline':True,'snapshot':snapshot,'sequence_length':sequence_length,'trend_days':trend_days,
              'pressure_windows':pressure_windows,'raw_sha256':json.loads(manifest)['sha256'],
              'sql_sha256':digest(ROOT/'sql/features/customer_aggregates.sql'),
              'matched_features':'NULL: no verified linkage/window/telemetry contract; affinity parameters not approved'}
    sql+=f'ALTER TABLE features.{stage} RENAME TO customer_features;\nALTER TABLE features.feature_dictionary_stage RENAME TO feature_dictionary;\n'
    sql+='COMMENT ON TABLE features.customer_features IS '+literal(json.dumps(metadata))+';\nANALYZE features.customer_features;\nCOMMIT;\n'
    variables={'feature_stage':stage,'snapshot':snapshot,'sequence_length':sequence_length,'trend_days':trend_days,
               'pressure_windows':','.join(map(str,pressure_windows)),'dictionary_count':len(definitions)}
    print('Building historical features and running transactional validation',flush=True)
    output=execute(sql,env=env,variables=variables)
    checks=[json.loads(line) for line in output.splitlines() if line.startswith('{')]
    summary=query('SELECT count(*) feature_rows,min(feature_as_of) snapshot,sum(history_row_count) represented_history_rows FROM features.customer_features',env=env)[0]
    summary.update(metadata)
    summary['validation_checks']=checks
    summary['dictionary_columns']=query('SELECT count(*) n FROM features.feature_dictionary',env=env)[0]['n']
    summary['indexes']=query("SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes WHERE (schemaname='raw' AND tablename='gold_events') OR (schemaname='features' AND tablename IN('customer_features','feature_dictionary')) ORDER BY 1,2,3",env=env)
    nulls=query('SELECT '+','.join('count(*) FILTER(WHERE '+ident(d['column_name'])+' IS NULL) AS '+ident(d['column_name']) for d in definitions)+' FROM features.customer_features',env=env)[0]
    summary['column_missingness']=[{'column_name':name,'null_rows':nulls[name]} for name in sorted(nulls)]
    save_json(report_dir/'feature_build.json',summary)
    return summary


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report-dir',type=Path,required=True)
    p.add_argument('--snapshot',default='2026-05-01 00:00:00')
    p.add_argument('--sequence-length',type=int,default=8)
    p.add_argument('--trend-days',type=int,default=7)
    p.add_argument('--pressure-windows',type=int,nargs='+',default=[1,7,30])
    p.add_argument('--archive-existing',action='store_true')
    a=p.parse_args()
    result=build(a.report_dir,a.snapshot,a.sequence_length,a.trend_days,a.pressure_windows,a.archive_existing)
    print(json.dumps({k:result[k] for k in ('feature_rows','snapshot','dictionary_columns')},default=str))


if __name__=='__main__':
    main()
