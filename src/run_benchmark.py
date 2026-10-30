"""Controlled local benchmark; original pipelines and outputs are never overwritten."""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import time

from benchmark_inputs import (STRATEGIES, CUTOFF, select_cohort, structured_features, recent_records,
                              strategy_state, laya_request, check_request_size, token_flags, digest)
from benchmark_report import input_audit, write_reports, assert_private_ids_absent
from http_client import request_json
from laya_client import validate_answer
from real_data import FILES, connect_inputs, file_digest
from run_one import save
from run_real import summarize_events

ROOT = Path(__file__).resolve().parents[1]


def protected_hashes(root):
    files = [Path(root)/'input'/name for name in FILES.values()]
    files += [Path(root)/'src'/name for name in ('state_builder.py','qwen_summary.py','laya_client.py',
              'http_client.py','run_one.py','real_data.py','run_real.py')]
    files += [Path(root)/'tests'/name for name in ('test_pipeline.py','test_real_data.py')]
    files += list((Path(root)/'outputs').glob('*.json'))
    files += [path for path in (Path(root)/'outputs/real').rglob('*') if path.is_file()]
    return {str(path.relative_to(root)):file_digest(path) for path in files}


def make_manifest(selected,policy):
    contexts = []
    for item in selected:
        candidate = item['candidate']
        contexts.append({'alias':item['alias'],'cutoff_exclusive':CUTOFF,
                         'scope':candidate['scope'],'history_rows':len(item['history']),
                         'history_sha256':digest(item['history']),'applications':candidate['applications'],
                         'channels':(candidate['channels'] or '').split(',') if candidate['channels'] else [],
                         'engagement':candidate['engagement']>0,'aip':candidate['aip']>0,
                         'identity_checks':'known apps unique context and single known product in selected historical scope; linked also shared app',
                         'independently_verified_customer_identity':False})
    return {'cutoff_exclusive':CUTOFF,'aliases':[item['alias'] for item in selected],
            'selection_policy':policy,'contexts':contexts,'strategies':list(STRATEGIES),
            'action_catalog_status':'DEMO ONLY; email unobserved; no execution'}


def model_metadata(base_url):
    tags,_ = request_json(base_url,'/api/tags',timeout=10)
    if not {'laya:latest','qwen2.5:1.5b'} <= {model['name'] for model in tags['models']}:
        raise RuntimeError('Required installed models unavailable; no models downloaded')
    show,_ = request_json(base_url,'/api/show',{'model':'laya'})
    parameters = show.get('parameters','')
    matched = re.search(r'\bnum_ctx\s+(\d+)',parameters)
    if not matched:
        raise ValueError('Cannot verify configured Laya context window; model configuration left unchanged')
    return {'laya_context_window':int(matched.group(1)),
            'installed_model_digests':{model['name']:model['digest'] for model in tags['models']},
            'laya_parameters':parameters,'ollama_version':request_json(base_url,'/api/version')[0],
            'residency_before_qwen':request_json(base_url,'/api/ps')[0]}


def schedule(aliases):
    planned = []
    for index,alias in enumerate(aliases):
        order = STRATEGIES[index%4:]+STRATEGIES[:index%4]
        for strategy in order:
            options = ['original','reversed'] if strategy in ('B','C') else ['original']
            if index%2:
                options.reverse()
            planned.extend((alias,strategy,option) for option in options)
    return planned


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-budget',type=int,default=512,help='Soft actual-token reporting target; never a truncation limit')
    parser.add_argument('--output-dir',type=Path,default=ROOT/'outputs/benchmark')
    args = parser.parse_args()
    if args.input_budget<1:
        parser.error('--input-budget must be positive')
    output = args.output_dir.resolve()
    if not output.is_relative_to((ROOT/'outputs').resolve()) or (output.exists() and any(output.iterdir())):
        print('Use a new, empty output directory beneath project outputs; existing runs are preserved.',file=sys.stderr)
        return 1
    output.mkdir(parents=True,exist_ok=True)
    private = output/'private'
    private.mkdir()
    baseline = protected_hashes(ROOT)
    results, qwen_runs, selected = [],{},[]
    manifest = {'aliases':[], 'contexts':[], 'cutoff_exclusive':CUTOFF,'selection_policy':{'rule':'Selection did not complete'}}
    metadata = {'status':'running','input_budget_tokens':args.input_budget,'started_utc':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
                'original_file_sha256':baseline,'benchmark_code_sha256':{name:file_digest(ROOT/'src'/name)
                    for name in ('benchmark_inputs.py','benchmark_report.py','run_benchmark.py')}}
    identifiers = set()
    audit = ''
    started = time.perf_counter()
    try:
        audit = input_audit(ROOT)
        with connect_inputs(ROOT/'input') as connection:
            selected,policy = select_cohort(connection)
            for item in selected:
                item['structured'] = structured_features(item,connection)
                item['recent20'] = recent_records(item['history'],item['aliases'],20)
        manifest = make_manifest(selected,policy)
        for item in selected:
            identifiers.update(str(row[key]) for row in item['history'] for key in ('context_id','application_id','session_id') if row.get(key))
        assert_private_ids_absent(json.dumps(manifest),identifiers)
        save(output/'selection_manifest.json',manifest)
        save(private/'identity_map.json',{item['alias']:{'context_id':item['candidate']['context_id'],'aliases':item['aliases']} for item in selected})
        for item in selected:
            directory = private/item['alias']
            directory.mkdir()
            save(directory/'history.json',item['history'])
            save(directory/'structured.json',item['structured'])
        print('Selected 10 fixed historical contexts; private histories saved.',flush=True)
        # Keep selection/audit available even if the service is unavailable.
        base_url = os.environ.get('OLLAMA_BASE_URL','http://127.0.0.1:11434')
        try:
            metadata.update(model_metadata(base_url))
            available = True
        except Exception as exc:
            available = False
            metadata['service_error_code'] = type(exc).__name__
            save(private/'service_error.json',{'type':type(exc).__name__,'detail':str(exc)})
        for index,item in enumerate(selected,1):
            directory = private/item['alias']/'qwen'
            directory.mkdir()
            qwen_started = time.perf_counter()
            try:
                if not available:
                    raise RuntimeError('Service unavailable')
                recent8 = item['recent20'][-8:]
                source = {'recent_events':recent8,'sequence_coverage':{'recent_limit':8,'included':len(recent8),
                           'older_rows_in_counts_only':len(item['history'])-len(recent8),
                           'all_rows_saved_locally':True,'equal_time_order':'deterministic; relative order unknown'}}
                summary,metrics = summarize_events(source,base_url,directory)
                qwen_runs[item['alias']] = {'status':'success','summary':summary,**metrics}
            except Exception as exc:
                raw_path = directory/'qwen_response.json'
                raw = json.loads(raw_path.read_text(encoding='utf-8')) if raw_path.exists() else {}
                qwen_runs[item['alias']] = {'status':'error','error_code':type(exc).__name__,
                    'input_tokens':raw.get('prompt_eval_count'),'output_tokens':raw.get('eval_count'),
                    'latency_ms':round((time.perf_counter()-qwen_started)*1000,3) if available else None}
                save(directory/'error.json',{'type':type(exc).__name__,'detail':str(exc)})
            save(directory/'validated_result.json',qwen_runs[item['alias']])
            print(f'Qwen summaries attempted: {index}/10 (no automatic retries).',flush=True)
        if available:
            metadata['residency_before_laya'] = request_json(base_url,'/api/ps')[0]
        by_alias = {item['alias']:item for item in selected}
        for call_index,(alias,strategy,option_order) in enumerate(schedule(manifest['aliases']),1):
            item = by_alias[alias]
            qwen = qwen_runs[alias]
            record = {'context_alias':alias,'strategy':strategy,'option_order':option_order,'status':'pending','probabilities':{},
                      'input_budget_tokens':args.input_budget,'context_window':metadata.get('laya_context_window'),
                      'qwen_input_tokens':qwen.get('input_tokens') if strategy=='C' else 0,
                      'qwen_output_tokens':qwen.get('output_tokens') if strategy=='C' else 0,
                      'qwen_latency_ms':qwen.get('latency_ms') if strategy=='C' else 0,
                      'qwen_summary_reused':strategy=='C' and option_order=='reversed',
                      'call_index':call_index,'history_sha256':digest(item['history'])}
            prefix = private/alias/f'{strategy}_{option_order}'
            call_started = time.perf_counter()
            try:
                if strategy=='C' and qwen['status']!='success':
                    record.update(status='qwen_failed',error_code=qwen['error_code'])
                else:
                    state = strategy_state(item['structured'],item['recent20'],strategy,qwen.get('summary'))
                    payload = laya_request(state,reverse=option_order=='reversed')
                    record['state_sha256'] = digest(state)
                    record['request_bytes'] = check_request_size(payload)
                    assert_private_ids_absent(json.dumps(payload),identifiers)
                    save(prefix.with_suffix('.request.json'),payload)
                    if not available:
                        record.update(status='service_unavailable',error_code=metadata['service_error_code'])
                    else:
                        raw,latency = request_json(base_url,'/v1/systemone',payload)
                        save(prefix.with_suffix('.response.json'),raw)
                        record.update(laya_latency_ms=latency,input_tokens=raw.get('usage',{}).get('input_tokens'),
                                      output_tokens=raw.get('usage',{}).get('output_tokens'))
                        record['over_input_budget'] = token_flags(record['input_tokens'],args.input_budget,record['context_window'])
                        valid = validate_answer(raw)
                        record.update(status='success',action_id=valid['action_id'],probabilities=valid['model_probabilities'])
                        record['total_inference_latency_ms'] = round(latency+(record['qwen_latency_ms'] or 0),3)
                        record['incremental_latency_ms'] = latency if option_order=='reversed' else record['total_inference_latency_ms']
            except Exception as exc:
                record.update(status='error',error_code=type(exc).__name__)
                record.setdefault('laya_latency_ms',round((time.perf_counter()-call_started)*1000,3) if available else None)
                save(prefix.with_suffix('.error.json'),{'type':type(exc).__name__,'detail':str(exc)})
            results.append(record)
            save(prefix.with_suffix('.result.json'),record)
            if call_index%6==0:
                print(f'Laya decision slots completed: {call_index}/60; successes: {sum(row["status"]=="success" for row in results)}.',flush=True)
                save(output/'run_metadata.json',metadata)
        metadata['status'] = 'complete' if all(row['status']=='success' for row in results) else 'complete_with_errors'
        metadata['actual_qwen_calls'] = sum((private/item['alias']/'qwen/qwen_request.json').exists() for item in selected) if available else 0
        metadata['unique_qwen_latency_sum_ms'] = sum(value.get('latency_ms') or 0 for value in qwen_runs.values())
        metadata['laya_latency_sum_ms'] = sum(value.get('laya_latency_ms') or 0 for value in results)
        metadata['all_strategies_use_same_selection'] = all({row['context_alias'] for row in results if row['strategy']==strategy}==set(manifest['aliases']) for strategy in STRATEGIES)
        if available:
            metadata['residency_after_laya'] = request_json(base_url,'/api/ps')[0]
    except Exception as exc:
        metadata.update(status='failed',error_code=type(exc).__name__)
        save(private/'fatal_error.json',{'type':type(exc).__name__,'detail':str(exc)})
        print('Benchmark failed; see private diagnostics. No customer-level error text printed.',file=sys.stderr,flush=True)
    finally:
        after = protected_hashes(ROOT)
        metadata['protected_files_unchanged'] = baseline==after
        if not metadata['protected_files_unchanged']:
            metadata['status'] = 'integrity_failure'
        metadata['elapsed_wall_ms'] = round((time.perf_counter()-started)*1000,3)
        save(output/'run_metadata.json',metadata)
        # Reports remain useful for a partial/unavailable-service run, without fabricated metrics.
        if len(results)==60:
            write_reports(output,results,manifest,metadata,qwen_runs,identifiers,audit)
        else:
            assert_private_ids_absent(audit,identifiers)
            (output/'input_audit.md').write_text(audit,encoding='utf-8')
        print(json.dumps({'status':metadata['status'],'successful_decisions':sum(row['status']=='success' for row in results),
                          'protected_files_unchanged':metadata['protected_files_unchanged'],'reports':str(output.relative_to(ROOT))}),flush=True)
    return 0 if metadata['status']=='complete' else 1


if __name__=='__main__':
    sys.exit(main())
