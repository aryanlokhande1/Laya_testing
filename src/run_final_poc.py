"""Final local technical demo; no Qwen calls, installs, training or prior-output edits."""
import argparse
import csv
import io
import json
import os
from pathlib import Path
import platform
import sys
import time

from benchmark_inputs import laya_request,check_request_size,digest
from benchmark_report import assert_private_ids_absent
from final_poc_data import real_examples,synthetic_stress,retrieval_fixture,validate_retrieval
from final_poc_reports import generate_reports
from http_client import request_json
from laya_client import validate_answer
from real_data import connect_inputs,file_digest
from robustness_runtime import supervise,utc_now,process_snapshot
from run_benchmark import protected_hashes,model_metadata
from run_one import save

ROOT=Path(__file__).resolve().parents[1]


def old_hashes(root):
    values=protected_hashes(root)
    for directory in ('benchmark','robustness'):
        for path in (root/'outputs'/directory).rglob('*'):
            if path.is_file(): values[str(path.relative_to(root))]=file_digest(path)
    for directory in ('src','tests'):
        for path in (root/directory).glob('*.py'):
            if 'final_poc' not in path.name: values[str(path.relative_to(root))]=file_digest(path)
    path=root/'input/sample_customer.json'
    values[str(path.relative_to(root))]=file_digest(path)
    return values


def actual_call(directory,payload,base_url,kind,expected=None):
    directory.mkdir(parents=True,exist_ok=True)
    save(directory/'request.json',payload)
    result={'status':'pending','kind':kind,'state_sha256':digest(payload['state'])}
    try:
        result['request_bytes']=check_request_size(payload)
        outcome=supervise(directory/'request.json',directory/'worker.json',base_url,30,45)
        save(directory/'supervised.json',outcome)
        result.update(status=outcome['status'],http_latency_ms=outcome.get('http_latency_ms'),
            total_local_inference_ms=outcome.get('http_latency_ms'),supervisor_elapsed_ms=outcome['supervisor_elapsed_ms'],
            started_utc=outcome.get('request_started_utc',outcome['supervisor_started_utc']),
            ended_utc=outcome.get('request_ended_utc',outcome['supervisor_ended_utc']),qwen_calls=0)
        if outcome['status']=='response':
            raw=outcome['response']; save(directory/'response.json',raw)
            result.update(input_tokens=raw.get('usage',{}).get('input_tokens'),output_tokens=raw.get('usage',{}).get('output_tokens'))
            if type(result['input_tokens']) is not int or result['input_tokens']<1: raise ValueError('Actual input token usage absent')
            if expected is None:
                valid=validate_answer(raw)
                result.update(action_id=valid['action_id'],probabilities=valid['model_probabilities'])
            else: result.update(validate_retrieval(raw,expected))
            result['status']='success'
            result['over_512_tokens']=result['input_tokens']>512
    except Exception as exc:
        result.update(status='error',error_code=type(exc).__name__)
        save(directory/'error.json',{'type':type(exc).__name__,'private_detail':str(exc)})
    save(directory/'result.json',result)
    return result


def write_csv(path,records,private_ids):
    keys=list(dict.fromkeys(key for row in records for key in row))
    stream=io.StringIO(newline=''); writer=csv.DictWriter(stream,fieldnames=keys,lineterminator='\n'); writer.writeheader()
    for row in records:
        writer.writerow({key:json.dumps(value,ensure_ascii=False) if isinstance(value,(dict,list)) else value for key,value in row.items()})
    text=stream.getvalue(); assert_private_ids_absent(text,private_ids)
    path.write_text(text,encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,default=ROOT/'outputs/final_poc')
    args=parser.parse_args(); output=args.output_dir.resolve()
    if not output.is_relative_to((ROOT/'outputs').resolve()) or output.exists() and any(output.iterdir()):
        parser.error('Use a fresh empty directory within outputs; previous measurements are never overwritten')
    output.mkdir(parents=True,exist_ok=True); private=output/'private'; private.mkdir()
    baseline=old_hashes(ROOT); private_ids=set(); real_results=[]; stress_results=[]; retrieval_results=[]
    metadata={'status':'running','started_utc':utc_now(),'protected_sha256':baseline,
        'python_version':platform.python_version(),'processes_before':process_snapshot(),
        'http_timeout_seconds':30,'supervisor_deadline_seconds':45,'new_qwen_calls':0,
        'fixed_catalog_order':['SMS_REMINDER','WA_SUPPORT','EMAIL_DETAILS'],
        'ordering_policy':'Reproducibility convention only, not an ordering-bias fix'}
    base_url=os.environ.get('OLLAMA_BASE_URL','http://127.0.0.1:11434')
    try:
        with connect_inputs(ROOT/'input') as connection:
            examples,totals=real_examples(connection,ROOT)
            metadata.update(totals)
            metadata['largest_unfiltered_april_history_rows']=connection.execute('''SELECT max(n) FROM
                (SELECT context_id,count(*) n FROM april WHERE event_datetime<CAST(? AS TIMESTAMP_NS) GROUP BY 1)''',['2026-04-15 00:00:00']).fetchone()[0]
            metadata['channel_event_evidence']=[dict(channel=row[0],event_name=row[1],rows=row[2]) for row in connection.execute('''SELECT channel,event_name,count(*) FROM
                (SELECT channel,event_name FROM march UNION ALL SELECT channel,event_name FROM april)
                WHERE event_name IN ('campaign_sent','campaign_open_read','campaign_clicked') GROUP BY 1,2 ORDER BY 1,2''').fetchall()]
        metadata['runtime']=model_metadata(base_url)
        metadata['model_configuration_unchanged_by_runner']=True
        for item in examples:
            alias=item['alias']; directory=private/alias; directory.mkdir()
            private_ids.update(str(row[key]) for row in item['history'] for key in ('context_id','application_id','session_id') if row.get(key))
            save(directory/'selected_history.json',item['history']); save(directory/'identifier_aliases.json',item['aliases'])
            save(directory/'state.json',item['state']); save(directory/'coverage_ledger.json',item['ledger'])
            request=laya_request(item['state']); assert_private_ids_absent(json.dumps(request),private_ids)
            result=actual_call(directory,request,base_url,'real_customer_demo')
            public={key:value for key,value in item.items() if key not in {'history','aliases','state','ledger'}}
            public.update(result); real_results.append(public)
            print(f'Real-history demo {alias}: {result["status"]}',flush=True)
        for n,vocabulary,apps,partners in ((1000,3,1,1),(10000,3,1,1),(1000000,3,1,1),(10000,40,20,20)):
            alias=f'SYN_N{n}_V{vocabulary}_A{apps}_P{partners}'
            started=time.perf_counter(); state=synthetic_stress(n,vocabulary,apps,partners)
            generation_ms=round((time.perf_counter()-started)*1000,3)
            directory=private/alias
            result=actual_call(directory,laya_request(state),base_url,'synthetic_stress')
            stress_results.append({'alias':alias,'category':'synthetic_stress_NOT_customer_observations',
                'generated_history_rows':n,'vocabulary':vocabulary,'applications':apps,'partners':partners,
                'individually_recent_events':8,'earlier_rows_in_aggregates':n-8,'fixture_generation_ms':generation_ms,
                'encoding':'explicit synthetic aggregate schema, distinct from real table-encoded compact input',**result})
            print(f'Synthetic stress {alias}: {result["status"]}',flush=True)
        # Counterfactual answers balance label priors: all three colors at all three positions.
        for filler in (4,48,192):
            for color in ('RED','BLUE','GREEN'):
                for position in ('beginning','middle','end'):
                    alias=f'RETRIEVAL_N{filler}_{color}_{position}'
                    payload,truth=retrieval_fixture(filler,position,color)
                    result=actual_call(private/alias,payload,base_url,'synthetic_factual_retrieval',color)
                    retrieval_results.append({'alias':alias,**truth,**result})
                print(f'Factual retrieval size={filler}, expected={color}: 3 positions recorded',flush=True)
        metadata['status']='complete' if all(r['status']=='success' for r in real_results+stress_results+retrieval_results) else 'complete_with_processing_failures'
        metadata['residency_after']=request_json(base_url,'/api/ps',timeout=3)[0]
        metadata['model_digests_after']={m['name']:m['digest'] for m in request_json(base_url,'/api/tags',timeout=3)[0]['models']}
        if metadata['model_digests_after']!=metadata['runtime']['installed_model_digests']: metadata['status']='model_digest_drift'
    except Exception as exc:
        metadata.update(status='failed',error_code=type(exc).__name__)
        save(private/'fatal_error.json',{'type':type(exc).__name__,'private_detail':str(exc)})
        print('Final demo failed; private diagnostic retained. No fabricated results.',file=sys.stderr,flush=True)
    finally:
        metadata.update(ended_utc=utc_now(),protected_files_unchanged=baseline==old_hashes(ROOT))
        if not metadata['protected_files_unchanged']: metadata['status']='integrity_failure'
        save(output/'run_metadata.json',metadata)
        write_csv(output/'compression_real.csv',real_results,private_ids)
        write_csv(output/'compression_synthetic_stress.csv',stress_results,private_ids)
        write_csv(output/'context_utilization.csv',retrieval_results,private_ids)
        generate_reports(ROOT,output,metadata,real_results,stress_results,retrieval_results,private_ids)
        print(json.dumps({'status':metadata['status'],'new_qwen_calls':0,'real_processing_successes':sum(r['status']=='success' for r in real_results),
            'factual_processing_successes':sum(r['status']=='success' for r in retrieval_results),
            'correct_factual_retrievals':sum(r.get('retrieval_correct',False) for r in retrieval_results),
            'protected_files_unchanged':metadata['protected_files_unchanged']}),flush=True)
    return 0 if metadata['status']=='complete' else 1


if __name__=='__main__': sys.exit(main())
