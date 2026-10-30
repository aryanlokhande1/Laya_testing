"""Create a new, curated v1 state table; never replace any original table."""
import argparse
import contextlib
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

if __package__ in (None,''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from data.combine_parquets import ROOT,digest,literal,save_json
from db.postgres import configuration,execute,query
from features.state_audit_v1 import fingerprint_originals,duplicate_exposure,resolve_application_context
from features.state_projection_v1 import CHANNEL_SCALARS,SCALARS,TABLE_COLUMNS,model_sql,selected_lineage,validate_state

SNAPSHOT='2026-05-01 00:00:00'
NEW_FILES={'build_laya_state_v1.py','state_projection_v1.py','state_audit_v1.py','state_reports_v1.py',
           'test_laya_state_v1.py','build_laya_state_v1.sql','verify_laya_state_v1.sql'}


@contextlib.contextmanager
def temporary_awake_request():
    """Prevent automatic idle sleep only during this local batch; no power-plan edit."""
    if os.name!='nt':
        yield
        return
    import ctypes
    from ctypes import wintypes
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.SetThreadExecutionState.argtypes=[wintypes.DWORD]
    kernel.SetThreadExecutionState.restype=wintypes.DWORD
    accepted=bool(kernel.SetThreadExecutionState(0x80000001))
    try:yield
    finally:
        if accepted:kernel.SetThreadExecutionState(0x80000000)


def render_build_sql():
    text=(ROOT/'sql/features/build_laya_state_v1.sql').read_text(encoding='utf-8')
    if text.count('-- __MODEL_JSON__')!=1:raise ValueError('Missing unique projection marker')
    return text.replace('-- __MODEL_JSON__',model_sql())


def render_verification_sql():
    text=(ROOT/'sql/features/verify_laya_state_v1.sql').read_text(encoding='utf-8')
    reused=list(CHANNEL_SCALARS)+['history_row_count','missing_channel_rows','send_rows_7d',
       'distinct_application_count','distinct_product_count','hours_since_last_observed_event']
    check="INSERT INTO _state_checks SELECT 'reused_verified_scalars_unchanged',NOT EXISTS(SELECT 1 FROM features.laya_state_v1 s JOIN features.customer_features f USING(context_id) WHERE ROW("+','.join('s.'+n for n in reused)+') IS DISTINCT FROM ROW('+','.join('f.'+n for n in reused)+') OR s.observed_event_active_days_7d IS DISTINCT FROM f.active_days_7d OR s.observed_history_hours IS DISTINCT FROM extract(epoch FROM (f.last_observed_event_at-f.first_observed_event_at))/3600.0);'
    return text.replace('-- __REUSED_SCALAR_CHECK__',check)


def protected_files():
    paths=[]
    for folder in ['src','tests','sql','outputs','reports/feature_build']:
        paths.extend(p for p in (ROOT/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name not in NEW_FILES)
    paths.extend(ROOT/name for name in ['requirements.txt','Modelfile','.env.example'])
    paths.extend((ROOT/'input').glob('*.parquet'))
    paths.extend((ROOT/'data/processed').glob('*.parquet'))
    return {str(p.relative_to(ROOT)):digest(p) for p in paths}


def audit_repository(run_dir: Path, env: dict) -> dict:
    """Aggregate audit only: no customer/application/partner values in output."""
    old=query('SELECT column_name,sql_type,meaning,source_fields,formula,implementation,observability,limitations FROM features.feature_dictionary ORDER BY column_name',env=env)
    nulls=query('SELECT '+','.join('count(*) FILTER(WHERE "'+r['column_name']+'" IS NULL) AS "'+r['column_name']+'"' for r in old)+' FROM features.customer_features',env=env)[0]
    result={'field_population':[{'column':r['column_name'],'sql_type':r['sql_type'],'null_rows':nulls[r['column_name']]} for r in old]}
    save_json(run_dir/'original_feature_definitions.json',old)
    result['stage_audit']=query("SELECT count(*) rows,count(*) FILTER(WHERE latest_observed_journey_stage IN('campaign_sent','campaign_open_read','campaign_clicked')) campaign_valued_latest_stages,count(*) FILTER(WHERE distinct_application_count>1) contexts_with_multiple_applications FROM features.customer_features",env=env)[0]
    result['application_resolution']=query("WITH x AS(SELECT application_id,count(DISTINCT context_id) n FROM raw.gold_events WHERE event_datetime<"+literal(SNAPSHOT)+"::timestamp AND nullif(btrim(application_id),'') IS NOT NULL AND nullif(btrim(context_id),'') IS NOT NULL GROUP BY 1) SELECT count(*) applications,count(*) FILTER(WHERE n=1) unique_context_applications,count(*) FILTER(WHERE n>1) ambiguous_applications,max(n) maximum_contexts_per_application FROM x",env=env)[0]
    result['recent_sequence_dominance']=query("SELECT count(*) FILTER(WHERE n=total AND total>0) entirely_campaign,count(*) FILTER(WHERE n*2>total) majority_campaign FROM(SELECT f.context_id,count(*) FILTER(WHERE x->>'event_name' IN('campaign_sent','campaign_open_read','campaign_clicked')) n,count(*) total FROM features.customer_features f CROSS JOIN LATERAL jsonb_array_elements(f.recent_events) x GROUP BY f.context_id) t",env=env)[0]
    result['identity_is_verified_customer_mapping']=False
    result['raw_shape']=query("SELECT count(*) rows,count(*) FILTER(WHERE channel IS NULL) null_channel_rows,count(*) FILTER(WHERE channel IS NOT NULL AND btrim(channel)='') blank_channel_rows,count(*) FILTER(WHERE nullif(btrim(channel),'') IS NOT NULL AND channel NOT IN('SMS','WA','GRCS')) unapproved_channel_rows,count(*) FILTER(WHERE partner_id ~ '[|,;:/]') partner_rows_with_separators,count(*) FILTER(WHERE nullif(btrim(event_name),'') IS NULL) unclassified_event_labels,count(*) FILTER(WHERE event_datetime IS NULL) null_timestamp_rows,count(*) FILTER(WHERE event_datetime>="+literal(SNAPSHOT)+"::timestamp) at_or_after_snapshot_rows FROM raw.gold_events",env=env)[0]
    save_json(run_dir/'repository_audit.json',result)
    return result


def build(run_dir: Path, communication_limit=4, journey_limit=4, *, before_ready=False):
    if not all(isinstance(n,int) and not isinstance(n,bool) and 1<=n<=100 for n in [communication_limit,journey_limit]):
        raise ValueError('Sequence representation limits must be integers in [1,100]')
    env=configuration(prefer_application=False)  # Explicitly use the requested .env.postgres.
    if query("SELECT to_regclass('features.laya_state_v1') IS NOT NULL present",env=env)[0]['present']:
        raise FileExistsError('features.laya_state_v1 already exists; no overwrite or implicit replacement is permitted')
    if (run_dir/'execution.json').exists():raise FileExistsError('Run report already exists; use a fresh run directory')
    run_dir.mkdir(parents=True,exist_ok=True)
    baseline_path=run_dir/'protected_files_before.json'
    if baseline_path.exists():baseline=json.loads(baseline_path.read_text(encoding='utf-8'))
    else:
        baseline=protected_files();save_json(baseline_path,baseline)
    result={'status':'running','started_utc':datetime.now(timezone.utc).isoformat(),'snapshot':SNAPSHOT,
            'communication_limit':communication_limit,'journey_limit':journey_limit,'model_requests':0,
            'credential_source':'.env.postgres (process PG variables retain explicit precedence)',
            'scalar_behavioral_columns':len(SCALARS),'expected_total_columns':len(TABLE_COLUMNS)}
    save_json(run_dir/'execution.json',result)
    try:
        if before_ready:
            before=json.loads((run_dir/'protected_tables_before.json').read_text(encoding='utf-8'))
        else:before=fingerprint_originals(run_dir,'before',env)
        if before['raw.gold_events']['rows']!=9856993 or before['features.customer_features']['rows']!=172692 or len(before['features.customer_features']['columns'])!=104:
            raise ValueError('Source repository counts/schema differ; review before building v1')
        if query("SELECT count(*) n FROM features.customer_features WHERE feature_as_of<>"+literal(SNAPSHOT)+'::timestamp',env=env)[0]['n']:
            raise ValueError('Existing feature snapshot does not match the fixed v1 snapshot')
        repository_audit=audit_repository(run_dir,env)
        if repository_audit['raw_shape']['unapproved_channel_rows']:
            raise ValueError('Unapproved source channel values exist; no renaming or silent omission is allowed')
        result['repository_audit']=repository_audit
        private=run_dir/'private';private.mkdir(exist_ok=True)
        binary_path=private/'duplicate_exposure.bin'
        if (run_dir/'duplicate_audit.json').exists() and binary_path.exists():
            duplicates=json.loads((run_dir/'duplicate_audit.json').read_text(encoding='utf-8'))
        else:
            duplicates=duplicate_exposure(ROOT/'data/processed/gold_events_202603_202604.parquet',SNAPSHOT,binary_path,run_dir)
        if (duplicates['exact_duplicate_groups'],duplicates['excess_rows'],duplicates['cross_file_groups'])!=(17111,20628,0):
            raise ValueError('Exact duplicate audit differs from known source; review before publication')
        result['duplicate_audit']=duplicates
        sql="BEGIN; SET LOCAL statement_timeout='60min'; SET LOCAL lock_timeout='15s';\n"
        # Existing app role owns the source schemas. Credentials still come from
        # .env.postgres; SET LOCAL ROLE changes neither credentials nor grants.
        role=query("SELECT r.rolsuper,EXISTS(SELECT 1 FROM pg_roles WHERE rolname='laya_features_app') app_role_exists FROM pg_roles r WHERE r.rolname=current_user",env=env)[0]
        if role['rolsuper'] and role['app_role_exists']:sql+='SET LOCAL ROLE laya_features_app;\n'
        sql+='LOCK TABLE raw.gold_events,features.customer_features,features.feature_dictionary IN SHARE MODE;\n'
        sql+='CREATE TEMP TABLE _state_duplicates(context_id text PRIMARY KEY,duplicate_groups bigint,excess_rows bigint,rows_in_groups bigint,communication_excess_rows bigint,non_campaign_excess_rows bigint,cross_file_groups bigint) ON COMMIT DROP;\n'
        sql+='\\copy _state_duplicates FROM '+literal(binary_path.resolve().as_posix())+' WITH(FORMAT BINARY)\n'
        checks_file=run_dir/'validation_checks.jsonl'
        sql+=render_build_sql()+'\\o '+literal(checks_file.resolve().as_posix())+'\n'+render_verification_sql()+'\\o\n'
        metadata={'schema_version':'laya_state_v1','snapshot':SNAPSHOT,'communication_limit':communication_limit,'journey_limit':journey_limit,
                  'sql_sha256':digest(ROOT/'sql/features/build_laya_state_v1.sql'),'projection_sha256':digest(ROOT/'src/features/state_projection_v1.py'),
                  'parameters_are_provisional_representation_limits':True,'source_tables_read_only':True}
        sql+='COMMENT ON TABLE features.laya_state_v1 IS '+literal(json.dumps(metadata))+';\nANALYZE features.laya_state_v1; COMMIT;\n'
        variables={'snapshot':SNAPSHOT,'communication_limit':communication_limit,'journey_limit':journey_limit,'expected_rows':172692,
                   'duplicate_groups':duplicates['exact_duplicate_groups'],'duplicate_excess_rows':duplicates['excess_rows']}
        print('Building new compact state and running independent SQL validation',flush=True)
        start=time.perf_counter();output=execute(sql,env=env,variables=variables,timeout=4500)
        result['sql_build_and_validation_seconds']=time.perf_counter()-start
        result['sql_checks']=[json.loads(line) for line in checks_file.read_text(encoding='utf-8').splitlines() if line.startswith('{')]
        result['table_summary']=query("SELECT count(*) rows,count(DISTINCT context_id) distinct_contexts,count(*) FILTER(WHERE latest_non_campaign_event_name IS NOT NULL) contexts_with_non_campaign_history,count(*) FILTER(WHERE latest_non_campaign_journey_stage IS NOT NULL OR latest_non_campaign_journey_substage IS NOT NULL) contexts_with_observed_non_campaign_stage,count(*) FILTER(WHERE (quality_flags->'exact_payload_duplicate_exposure'->>'excess_rows')::bigint>0) duplicate_exposed_contexts,sum((quality_flags->>'partner_rows_with_separators')::bigint) partner_rows_with_separators,sum((quality_flags->'missing_channel_rows'->>'null')::bigint) null_channel_rows,sum((quality_flags->'missing_channel_rows'->>'blank')::bigint) blank_channel_rows,avg(octet_length(state_json::text)) average_model_json_bytes,max(octet_length(state_json::text)) max_model_json_bytes FROM features.laya_state_v1",env=env)[0]
        cols=query("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='features' AND table_name='laya_state_v1' ORDER BY ordinal_position",env=env)
        if [x['column_name'] for x in cols]!=list(TABLE_COLUMNS):raise ValueError('New table column contract mismatch')
        result['columns']=cols
        # Validate a bounded selection locally without logging any customer state.
        samples=query('SELECT state_json FROM features.laya_state_v1 ORDER BY context_id LIMIT 12',env=env)
        for sample in samples:validate_state(sample['state_json'],communication_limit,journey_limit)
        result['bounded_python_payloads_validated']=len(samples)
        after=fingerprint_originals(run_dir,'after',env)
        if before!=after:raise ValueError('A protected original table changed; new build is not declared verified')
        result['original_table_fingerprints_unchanged']=True
        test_env=dict(os.environ,LAYA_TEST_POSTGRES='1',PYTHONIOENCODING='utf-8')
        tests=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],cwd=ROOT,env=test_env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=300)
        log=tests.stdout.decode('utf-8',errors='replace');(run_dir/'tests.log').write_text(log,encoding='utf-8')
        match=re.search(r'Ran (\d+) tests?',log)
        result['tests']={'passed':tests.returncode==0,'count':int(match[1]) if match else None}
        if tests.returncode:raise ValueError('Regression suite failed; inspect tests.log')
        changed=[name for name,sha in baseline.items() if not (ROOT/name).is_file() or digest(ROOT/name)!=sha]
        if changed:raise ValueError('Protected source, input, model configuration or experiment artifacts changed')
        result['protected_files_unchanged']=True
        binary_path.unlink()  # Own transient transport; original data never deleted.
        result['status']='verified'
        return result
    except Exception as exc:
        result.update(status='failed',error_type=type(exc).__name__,error=str(exc))
        checks=run_dir/'validation_checks.jsonl'
        if checks.exists():result['sql_checks']=[json.loads(line) for line in checks.read_text(encoding='utf-8').splitlines() if line.startswith('{')]
        raise
    finally:
        result['finished_utc']=datetime.now(timezone.utc).isoformat()
        result['implementation_sha256']={str(p.relative_to(ROOT)):digest(p) for p in [ROOT/'src/features'/n for n in ['build_laya_state_v1.py','state_projection_v1.py','state_audit_v1.py','state_reports_v1.py']]+[ROOT/'sql/features'/n for n in ['build_laya_state_v1.sql','verify_laya_state_v1.sql']]+[ROOT/'tests/test_laya_state_v1.py'] if p.exists()}
        save_json(run_dir/'execution.json',result)
        from features.state_reports_v1 import generate_reports
        generate_reports(run_dir,result)
        print(json.dumps({'status':result['status'],'reports':str(run_dir),'table':result.get('table_summary'),'tests':result.get('tests')},indent=2),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report-dir',type=Path)
    p.add_argument('--communication-limit',type=int,default=4)
    p.add_argument('--journey-limit',type=int,default=4)
    p.add_argument('--before-fingerprint-ready',action='store_true',help='Use this run directory\'s already captured full before fingerprints')
    p.add_argument('--resolve-application',action='store_true',help='Read LAYA_APPLICATION_ID from process environment; print only resolution status/count')
    args=p.parse_args()
    if args.resolve_application:
        result=resolve_application_context(os.environ.get('LAYA_APPLICATION_ID',''))
        print(json.dumps({k:result[k] for k in ['status','context_count']}))
        return
    run=args.report_dir or ROOT/'reports/state_build'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')
    with temporary_awake_request():build(run,args.communication_limit,args.journey_limit,before_ready=args.before_fingerprint_ready)


if __name__=='__main__':main()
