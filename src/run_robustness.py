"""Frozen B repeatability, option permutations and documented compact-input experiment."""
from collections import Counter
import argparse
import copy
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import statistics
import sys

from benchmark_inputs import digest,check_request_size
from benchmark_report import assert_private_ids_absent,percentile
from http_client import request_json
from laya_client import ACTION_CATALOG,validate_answer
from real_data import connect_inputs,rows,file_digest,FILES
from robustness_inputs import ALIASES,compact_state,reordered_request,schedule
from robustness_runtime import supervise,wire_bytes,utc_now,process_snapshot
from run_benchmark import protected_hashes,model_metadata
from run_one import save

ROOT=Path(__file__).resolve().parents[1]


def snapshot(base_url):
    start=utc_now()
    try:
        raw,latency=request_json(base_url,'/api/ps',timeout=3)
        return {'status':'available','started_utc':start,'ended_utc':utc_now(),'latency_ms':latency,'response':raw}
    except Exception as exc:
        return {'status':'unavailable','started_utc':start,'ended_utc':utc_now(),'error_code':type(exc).__name__}


def preserve_hashes():
    result=protected_hashes(ROOT)
    for directory in (ROOT/'outputs/benchmark',):
        for path in directory.rglob('*'):
            if path.is_file(): result[str(path.relative_to(ROOT))]=file_digest(path)
    for path in (ROOT/'src').glob('*.py'):
        if 'robustness' not in path.name: result[str(path.relative_to(ROOT))]=file_digest(path)
    path=ROOT/'tests/test_benchmark.py'
    result[str(path.relative_to(ROOT))]=file_digest(path)
    return result


def result_metrics(raw):
    valid=validate_answer(raw)
    probabilities=valid['model_probabilities']
    ordered=sorted(probabilities.values(),reverse=True)
    tokens=raw.get('usage',{}).get('input_tokens')
    if type(tokens) is not int or tokens<1:
        raise ValueError('Missing actual token usage')
    return {'action_id':valid['action_id'],'probabilities':probabilities,
            'top_two_margin':ordered[0]-ordered[1],'input_tokens':tokens,
            'output_tokens':raw.get('usage',{}).get('output_tokens')}


def summarize_groups(results):
    groups=[]
    for alias in ALIASES:
        for representation in ('original','compact'):
            for order_index in range(6 if representation=='original' else 1):
                group=[r for r in results if r['context_alias']==alias and r['representation']==representation and r['order_index']==order_index]
                successes=[r for r in group if r['status']=='success']
                spread=max((max(r['probabilities'][key] for r in successes)-min(r['probabilities'][key] for r in successes)
                            for key in ACTION_CATALOG),default=None) if successes else None
                groups.append({'alias':alias,'representation':representation,'order_index':order_index,
                    'attempts':len(group),'successes':len(successes),'actions':sorted({r['action_id'] for r in successes}),
                    'probability_spread':spread,'token_values':sorted({r['input_tokens'] for r in successes}),
                    'median_margin':statistics.median(r['top_two_margin'] for r in successes) if successes else None})
    return groups


def number(value,places=3):
    return 'unavailable' if value is None else f'{value:.{places}f}'


def reports(output,results,metadata,ledgers,channel_evidence,private_ids):
    groups=summarize_groups(results)
    full=[g for g in groups if g['representation']=='original' and g['successes']==3]
    unstable=[g for g in full if len(g['actions'])>1]
    failures=[r for r in results if r['status']!='success']
    changed_orders=0
    for alias in ALIASES:
        valid=[g for g in full if g['alias']==alias]
        if len(valid)==6 and len({tuple(g['actions']) for g in valid})>1: changed_orders+=1
    lines=['# Focused Laya robustness investigation','',
        f"Status: {metadata['status']}; successes {len(results)-len(failures)}/{len(results)}. No Qwen, training, model downloads, configuration updates or production execution.",'',
        'Five frozen contexts: CTX01 (multiple applications and AIP-labelled history), CTX02, CTX03, CTX08, CTX09. '
        'Every original B state, question, action ID and description is copied from the saved benchmark. '
        'Each of all six permutations is evaluated three times (90 original calls); compact original-order input is evaluated three times per context (15 more). '
        'Repeats are interleaved by permutation/context and cycle. No retries, fallback, warmup, unload or seed/temperature override is used. '
        'Compact order sensitivity is not tested here. Labels remain demo-only; no business accuracy or optimality is measured.','',
        f"Runtime: Ollama {metadata.get('runtime',{}).get('ollama_version',{})}; Laya context {metadata.get('runtime',{}).get('laya_context_window','unknown')}. "
        'Full installed model digests, parameters, residency and accessible process snapshots are in run_metadata.json and private per-call diagnostics. '
        'Architecture context is distinguished from the loaded/configured context; defaults are not assumed to imply determinism.','',
        f"Identical-request groups with all 3 successes: {len(full)}/30. Groups whose selected action varied: {len(unstable)}. "
        f"Contexts with different selected-action sets across six complete option orders: {changed_orders}/5.",'',
        '| Context | Order index | Successes/3 | Observed actions | Maximum same-request probability spread | Median top-two margin | Actual tokens |',
        '| --- | ---: | ---: | --- | ---: | ---: | --- |']
    for group in groups:
        if group['representation']=='original':
            lines.append(f"| {group['alias']} | {group['order_index']} | {group['successes']} | {', '.join(group['actions']) or 'none'} | {number(group['probability_spread'],9)} | {number(group['median_margin'],6)} | {group['token_values']} |")
    lines+=['','Order indices map to exact action-ID arrays in selection_manifest.json. Identical wire hashes are required within each context/order/repeat group. '
        'Between-order changes must not be conflated with within-order stochastic variation. Top-two margin is a ranking gap, not a correctness probability.','',
        '| Representation | Success n | Median actual input tokens | Median / p95 HTTP ms | Median top-two margin |',
        '| --- | ---: | ---: | --- | ---: |']
    for rep in ('original','compact'):
        successful=[r for r in results if r['representation']==rep and r['status']=='success']
        lines.append(f"| {rep} | {len(successful)} | {number(percentile([r['input_tokens'] for r in successful],.5))} | "
            f"{number(percentile([r['http_latency_ms'] for r in successful],.5))} / {number(percentile([r['http_latency_ms'] for r in successful],.95))} | "
            f"{number(percentile([r['top_two_margin'] for r in successful],.5),6)} |")
    lines+=['','Only 5 selected contexts and repeated/correlated requests: p95 is descriptive, not a reliable tail estimate. '
        'Original rows mix six orders whereas compact rows use only canonical order; use the paired canonical table for token/action comparisons.','',
        '## CTX08 and timeout diagnostics','',
        'Previous measurements are untouched: CTX08 B original recorded 10,092,428.628 ms and reversed 145,931.772 ms; prior diagnostic replays were about 10–12 seconds. '
        'Those historical samples have no request UTC boundaries or host telemetry sufficient to reconstruct the cause.','',
        f"This experiment uses {metadata['http_timeout_seconds']} s socket/HTTP timeout and {metadata['deadline_seconds']} s supervised subprocess deadline. "
        'Every request has supervisor/worker UTC start/end, elapsed timing, worker PID, available Ollama process CPU/memory/start time, '
        '/api/ps before/after and the complete original Ollama response. These do not prove a GPU/CPU execution duration. '
        'The decision endpoint reports no server compute-duration field.','',
        'A supervisor deadline includes worker startup; HTTP latency excludes startup. The HTTP socket timeout is not a total-service SLA. '
        'The supervisor rejects any elapsed-over-deadline response even if the worker wrote valid JSON. '
        'A suspended host cannot run a watchdog; on resume, a late response is retained privately but is a deadline failure. '
        'Terminating the client does not prove server computation stopped. No late/timeout result becomes a successful decision.','',
        '| CTX08 representation/order | Repeat | Status | HTTP ms | Supervisor ms | Start UTC | End UTC |',
        '| --- | ---: | --- | ---: | ---: | --- | --- |']
    for row in results:
        if row['context_alias']=='CTX08':
            lines.append(f"| {row['representation']}/{row['order_index']} | {row['repeat']} | {row['status']} | {number(row.get('http_latency_ms'))} | {number(row.get('supervisor_elapsed_ms'))} | {row['started_utc']} | {row['ended_utc']} |")
    lines+=['',f"Failure statuses: {dict(Counter(row['status'] for row in failures))}. All failures retained, never retried or converted into successes.",'',
        '## Training-readiness conclusion','']
    if len(full)==30 and not unstable:
        lines+=['The original requests were empirically repeatable for all 30 identical-request groups in this runtime (three observations each), '
            'not proof of model determinism on other states or configurations. Controlled domain-training research is mechanically feasible, '
            'but this is **not a robustness gate pass**: option-order dependence must be addressed and evaluated explicitly. '
            'An approved action contract and outcome objective are also required before meaningful domain-specific training.']
    else:
        lines+=['A training-readiness repeatability gate has not been established: identical-request variation or incomplete repeat groups remain. '
            'Resolve these failures/variations before attributing training effects to domain learning.']
    lines+=['','No fine-tuning was performed. The catalogue is synthetic; low margins or stable choices do not establish business correctness. '
        'No post-cutoff events were introduced. Gold-field availability/backfill leakage remains unverified without ingestion timestamps.','']
    compact=['# Experimental compact B comparison','',
        'Question, action IDs/descriptions and canonical option order are unchanged. The frozen original state is not edited. '
        'All original behavior fields can be reconstructed exactly (apart from the explicitly removed identity/limitation presentation strings). '
        'The reconstruction is asserted before any model call.','',
        '| Alias | Original actual tokens | Compact actual tokens | Token reduction % | Canonical paired action agreements |',
        '| --- | --- | --- | ---: | --- |']
    for alias in ALIASES:
        original=[r for r in results if r['context_alias']==alias and r['representation']=='original' and r['order_index']==0 and r['status']=='success']
        compressed=[r for r in results if r['context_alias']==alias and r['representation']=='compact' and r['status']=='success']
        o=statistics.median(r['input_tokens'] for r in original) if original else None
        c=statistics.median(r['input_tokens'] for r in compressed) if compressed else None
        pairs=[(a,b) for a in original for b in compressed if a['repeat']==b['repeat']]
        compact.append(f"| {alias} | {sorted({r['input_tokens'] for r in original})} | {sorted({r['input_tokens'] for r in compressed})} | {number(100*(o-c)/o if o and c else None)} | {sum(a['action_id']==b['action_id'] for a,b in pairs)}/{len(pairs)} |")
    compact+=['','## Preserved information and recorded changes','',
        '- All historical event counts and context first/last recency, all per-channel counts/recencies including zero and NULL, application row/count/latest-journey/product/partner aggregates, partner first/last/row counts, unassigned application rows, and exact last eight event occurrences are retained.',
        '- Per-table column labels replace repeated per-row keys; repeated event/stage/substage descriptions become a single event-type table. Table references remove duplicated presentation of a row, never distinct observed occurrences.',
        '- Existing application latest stage/substage remain intact. Latest observed noncampaign stage/substage and latest observation of each raw AIP label are explicitly referenced. These are historical observations, not inferred current truth or approval guarantees.',
        '- Remove only structured.identity and structured.limitations prose; scope/cutoff, demo-only/unauthoritative flags, the original unchanged safety question, exact nulls, missing channel group and sequence coverage remain. Missingness is explicitly described.',
        '- Some older journey/AIP observations absent individually from B are added from the saved pre-cutoff history to satisfy preservation requirements. These additions are listed below: in such cases this is not a pure encoding ablation. No new theme semantics or invented DPD/PTP/balance/note/intent are added.',
        '- Full application transitions were already compressed in B; their absence is not a new omission. Saved raw histories remain unchanged. No 512-token truncation target is enforced.','']
    for alias in ALIASES: compact.append(f"- {alias}: grounded supplements {json.dumps(ledgers[alias]['grounded_additions'],sort_keys=True)}")
    compact+=['','Actual request/response token counts and per-repeat probabilities, margins, latency and actions are in results.csv. '
        'A more compact encoding is not assumed to produce the same ranking; action agreements above are measured, not a quality score.','']
    gaps=['# Demo action contract gaps','',
        '| Observed channel label | What data establishes | What remains unverified |',
        '| --- | --- | --- |',
        '| SMS | Recognizable SMS communication label; campaign-labelled sends/engagement can be counted | Routing, delivery guarantee, consent, eligibility and approved themes |',
        '| WA | Observed channel code associated with campaign events; communication use evidenced | WhatsApp expansion/routing requires authoritative code dictionary, plus eligibility/consent/templates |',
        '| GRCS | Observed code; campaign association is evidence only | Transport meaning, whether separately actionable, eligibility and approved action-space membership; do not assume RCS or eligibility |',
        '| Missing/blank | Missing channel value, not an actionable channel | Source null/blank semantics and whether mapping is recoverable |',
        '| Email | Not observed in uploaded files | Availability, contactability, consent, action eligibility; demo option does not establish support |','',
        'All existing combinations SMS_REMINDER/sms/gentle_reminder, WA_SUPPORT/whatsapp/payment_support and '
        'EMAIL_DETAILS/email/balance_explanation remain DEMO ONLY. The theme names are merely existing catalogue strings; '
        'none has an approved business definition in this acquisition/event dataset. No new theme meaning is generated.','',
        'Required before replacing the demo catalog: authoritative action_space with immutable IDs, exact channel codes, '
        'approved theme/content/template definitions, supported channel/theme pairs, eligibility and suppression/consent rules, '
        'contactability, delivery/open/click definitions and deduplication, application/product/partner scope, scheduling/send caps, '
        'and the intended outcome/reward/attribution window. Identity and as-of availability contracts remain necessary. '
        'Catalog scoring cannot enforce SEND/WAIT, daily-send caps, timing or budget here; no production send occurs.','',
        'Full-month raw channel/event aggregate evidence (not eligibility proof):','',
        '| Channel | Event label | Rows |','| --- | --- | ---: |']
    gaps += [f"| {row['channel'] or '(missing)'} | {row['event_name']} | {row['count']} |" for row in channel_evidence]
    fields=['context_alias','representation','order_index','option_order','repeat','status','action_id',*ACTION_CATALOG,
        'top_two_margin','input_tokens','output_tokens','http_latency_ms','supervisor_elapsed_ms','started_utc','ended_utc',
        'http_timeout_seconds','deadline_seconds','worker_pid','request_sha256','state_sha256','availability_before','availability_after','error_code']
    stream=io.StringIO(newline='')
    writer=csv.DictWriter(stream,fieldnames=fields,lineterminator='\n'); writer.writeheader()
    for result in results:
        flat={key:result.get(key) for key in fields}; flat.update(result.get('probabilities',{})); writer.writerow(flat)
    artifacts={'results.csv':stream.getvalue(),'report.md':'\n'.join(lines),
        'compact_input_comparison.md':'\n'.join(compact),'action_contract_gaps.md':'\n'.join(gaps)}
    for name,text in artifacts.items():
        assert_private_ids_absent(text,private_ids)
        (output/name).write_text(text,encoding='utf-8')
    return {'complete_identical_groups':len(full),'variable_identical_groups':len(unstable),'order_sensitive_contexts':changed_orders}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,default=ROOT/'outputs/robustness')
    parser.add_argument('--http-timeout',type=float,default=30)
    parser.add_argument('--deadline',type=float,default=45)
    args=parser.parse_args()
    if not 0<args.http_timeout<args.deadline<=120: parser.error('Require 0 < timeout < deadline <=120 seconds')
    output=args.output_dir.resolve()
    if not output.is_relative_to((ROOT/'outputs').resolve()) or output.exists() and any(output.iterdir()):
        parser.error('Use a new empty directory within outputs; previous measurements never overwritten')
    output.mkdir(parents=True,exist_ok=True); (output/'private').mkdir()
    baseline=preserve_hashes(); results=[]; ledgers={}; private_ids=set(); channel_evidence=[]
    metadata={'status':'running','http_timeout_seconds':args.http_timeout,'deadline_seconds':args.deadline,
        'started_utc':utc_now(),'protected_sha256':baseline,'processes_before':process_snapshot()}
    base_url=os.environ.get('OLLAMA_BASE_URL','http://127.0.0.1:11434')
    try:
        identities=json.loads((ROOT/'outputs/benchmark/private/identity_map.json').read_text())
        originals={}; compact={}; histories={}
        for alias in ALIASES:
            source=ROOT/'outputs/benchmark/private'/alias
            originals[alias]=json.loads((source/'B_original.request.json').read_text())
            history=json.loads((source/'history.json').read_text()); histories[alias]=history
            private_ids.update(str(row[key]) for row in history for key in ('context_id','application_id','session_id') if row.get(key))
            compact[alias],ledgers[alias]=compact_state(originals[alias]['state'],history,identities[alias]['aliases'])
            save(output/'private'/f'{alias}.change_ledger.json',ledgers[alias])
        with connect_inputs(ROOT/'input') as connection:
            channel_evidence=rows(connection,'''SELECT channel,event_name,count(*) AS count FROM
                (SELECT channel,event_name FROM march UNION ALL SELECT channel,event_name FROM april)
                WHERE event_name IN ('campaign_sent','campaign_open_read','campaign_clicked') GROUP BY 1,2 ORDER BY 1,2''')
        try: metadata['runtime']=model_metadata(base_url)
        except Exception as exc:
            metadata['runtime_error_code']=type(exc).__name__
            save(output/'private/runtime_error.json',{'error':str(exc)})
        planned=schedule()
        orders={str(index):list(order) for _,rep,index,_,order in planned if rep=='original'}
        save(output/'selection_manifest.json',{'aliases':list(ALIASES),'option_orders':orders,'repeats_per_order':3,
            'original_calls':90,'compact_canonical_calls':15,'cutoff_exclusive':'2026-04-15 00:00:00',
            'source':'Frozen benchmark B requests; CTX01 added for multiple applications/AIP',
            'states':{alias:{'original_sha256':digest(originals[alias]['state']),'compact_sha256':digest(compact[alias])} for alias in ALIASES}})
        for index,(alias,representation,order_index,repeat,order) in enumerate(planned,1):
            prefix=output/'private'/f'{index:03d}_{alias}_{representation}_p{order_index}_r{repeat}'
            payload=reordered_request(originals[alias],order,compact[alias] if representation=='compact' else None)
            check_request_size(payload); assert_private_ids_absent(json.dumps(payload),private_ids)
            request_path=prefix.with_suffix('.request.json'); save(request_path,payload)
            before=snapshot(base_url); processes=process_snapshot()
            outcome=supervise(request_path,prefix.with_suffix('.worker.json'),base_url,args.http_timeout,args.deadline)
            after=snapshot(base_url)
            save(prefix.with_suffix('.diagnostics.json'),{'availability_before':before,'availability_after':after,
                'processes_before':processes,'processes_after':process_snapshot() if alias=='CTX08' else None,'outcome':outcome})
            record={'context_alias':alias,'representation':representation,'order_index':order_index,'option_order':'|'.join(order),
                'repeat':repeat,'status':outcome['status'],'http_latency_ms':outcome.get('http_latency_ms'),
                'supervisor_elapsed_ms':outcome['supervisor_elapsed_ms'],'started_utc':outcome.get('request_started_utc',outcome['supervisor_started_utc']),
                'ended_utc':outcome.get('request_ended_utc',outcome['supervisor_ended_utc']),
                'http_timeout_seconds':args.http_timeout,'deadline_seconds':args.deadline,'worker_pid':outcome['worker_pid'],
                'request_sha256':hashlib.sha256(wire_bytes(payload)).hexdigest(),'state_sha256':digest(payload['state']),
                'availability_before':before['status'],'availability_after':after['status']}
            if outcome['status']=='response':
                try:
                    record.update(result_metrics(outcome['response'])); record['status']='success'
                    window=metadata.get('runtime',{}).get('laya_context_window')
                    if window and record['input_tokens']>window: raise ValueError('Actual context exceeded')
                except Exception as exc:
                    record.update(status='validation_error',error_code=type(exc).__name__)
            else: record['error_code']=outcome.get('error_code',outcome['status'])
            results.append(record); save(prefix.with_suffix('.result.json'),record)
            if index%5==0:
                save(output/'run_metadata.json',metadata)
                print(f'Robustness slots: {index}/105; successes: {sum(r["status"]=="success" for r in results)}',flush=True)
        metadata['status']='complete' if all(r['status']=='success' for r in results) else 'complete_with_failures'
    except Exception as exc:
        metadata.update(status='failed',error_code=type(exc).__name__)
        save(output/'private/fatal_error.json',{'error':str(exc)})
        print('Robustness run failed; private diagnostic saved (no customer data printed).',file=sys.stderr)
    finally:
        metadata.update(ended_utc=utc_now(),processes_after=process_snapshot(),protected_files_unchanged=baseline==preserve_hashes())
        if not metadata['protected_files_unchanged']: metadata['status']='integrity_failure'
        metadata['summary']=reports(output,results,metadata,ledgers,channel_evidence,private_ids) if len(ledgers)==5 else {}
        save(output/'run_metadata.json',metadata)
        print(json.dumps({'status':metadata['status'],'successes':sum(r['status']=='success' for r in results),
                          'protected_files_unchanged':metadata['protected_files_unchanged'],'summary':metadata['summary']}),flush=True)
    return 0 if metadata['status']=='complete' else 1


if __name__=='__main__': sys.exit(main())
