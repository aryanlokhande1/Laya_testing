"""Run the PostgreSQL feature pipeline; no imports or calls to any model client."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

from data.combine_parquets import ROOT,consolidate,digest,save_json
from db.load_gold_events import load
from features.build_customer_features import build,validate_options
from features.reports import generate


def protected_hashes():
    paths=list((ROOT/'outputs').rglob('*'))
    paths+=list((ROOT/'src').glob('*.py'))+list((ROOT/'tests').glob('*.py'))
    excluded={'run_feature_pipeline.py','test_feature_pipeline.py'}
    return {str(p.relative_to(ROOT)):digest(p) for p in paths if p.is_file() and '__pycache__' not in p.parts and p.name not in excluded}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--snapshot',default='2026-05-01 00:00:00')
    p.add_argument('--sequence-length',type=int,default=8)
    p.add_argument('--trend-days',type=int,default=7)
    p.add_argument('--pressure-windows',type=int,nargs='+',default=[1,7,30])
    p.add_argument('--consolidation-report',type=Path)
    p.add_argument('--reuse-raw',action='store_true')
    p.add_argument('--archive-existing',action='store_true')
    a=p.parse_args()
    validate_options(a.snapshot,a.sequence_length,a.trend_days,a.pressure_windows)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')
    run_dir=ROOT/'reports/feature_build'/run_id
    run_dir.mkdir(parents=True,exist_ok=False)
    initial=protected_hashes()
    result={'status':'running','run_id':run_id,'started_utc':datetime.now(timezone.utc).isoformat(),
            'disk_free_before':shutil.disk_usage(ROOT).free,'python':sys.version,'new_model_calls':0}
    save_json(run_dir/'execution.json',result)
    try:
        if a.consolidation_report:
            audit=json.loads(a.consolidation_report.read_text(encoding='utf-8'))
            if audit.get('status')!='verified':raise ValueError('Consolidation report is not verified')
            report_path=a.consolidation_report.resolve()
        else:
            audit=consolidate(ROOT,run_dir)
            report_path=run_dir/'consolidation.json'
        result['consolidation_report']=str(report_path)
        result['consolidation']={k:audit[k] for k in ['combined_rows','consolidated_path','duplicate_audit','exact_multiset_verified']}
        result['database']=load(report_path,run_dir,reuse=a.reuse_raw)
        result['features']=build(run_dir,a.snapshot,a.sequence_length,a.trend_days,a.pressure_windows,a.archive_existing)
        env=dict(__import__('os').environ,LAYA_TEST_POSTGRES='1',PYTHONIOENCODING='utf-8')
        test=subprocess.run([sys.executable,'-m','unittest','discover','-s','tests','-v'],cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=300)
        log=test.stdout.decode('utf-8',errors='replace')
        (run_dir/'tests.log').write_text(log,encoding='utf-8')
        match=re.search(r'Ran (\d+) tests?',log)
        result['tests']={'passed':test.returncode==0,'count':int(match[1]) if match else None,'log':'tests.log'}
        if test.returncode:raise RuntimeError('Regression tests failed; inspect the saved log')
        if protected_hashes()!=initial:raise AssertionError('Previous inference source/tests or outputs changed')
        for source in audit['sources']:
            if digest(ROOT/'input'/source['filename'])!=source['sha256_after']:
                raise AssertionError('Original source changed during PostgreSQL build')
        result['original_parquet_hashes_verified']=True
        result['previous_experiments_unchanged']=True
        files=[ROOT/'src/run_feature_pipeline.py',ROOT/'tests/test_feature_pipeline.py']
        for folder in ['src/data','src/db','src/features','sql']:
            files.extend(p for p in (ROOT/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts)
        result['implementation_sha256']={str(p.relative_to(ROOT)):digest(p) for p in files}
        result['status']='verified'
    except Exception as exc:
        result.update(status='failed',error_type=type(exc).__name__,error=str(exc))
        raise
    finally:
        result['finished_utc']=datetime.now(timezone.utc).isoformat()
        result['disk_free_after']=shutil.disk_usage(ROOT).free
        save_json(run_dir/'execution.json',result)
        generate(run_dir,result)
        print(json.dumps({'status':result['status'],'reports':str(run_dir)},indent=2))


if __name__=='__main__':main()
