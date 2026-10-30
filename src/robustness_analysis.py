"""Postprocessing only: separate stochastic variation, option effects and power evidence."""
from datetime import datetime
import statistics

from laya_client import ACTION_CATALOG
from robustness_inputs import ALIASES


def option_effects(results):
    summaries=[]
    for alias in ALIASES:
        groups=[]
        for order in range(6):
            group=[r for r in results if r['context_alias']==alias and r['representation']=='original'
                   and r['order_index']==order and r['status']=='success']
            if len(group)!=3: continue
            means={key:statistics.mean(r['probabilities'][key] for r in group) for key in ACTION_CATALOG}
            groups.append({'order':order,'mean_probabilities':means,
                'within_spread':max(max(r['probabilities'][k] for r in group)-min(r['probabilities'][k] for r in group) for k in ACTION_CATALOG),
                'actions':sorted({r['action_id'] for r in group})})
        summaries.append({'alias':alias,'complete_orders':len(groups),
            'max_identical_request_spread':max((g['within_spread'] for g in groups),default=None),
            'max_between_order_mean_spread':max((max(g['mean_probabilities'][k] for g in groups)-min(g['mean_probabilities'][k] for g in groups)
                for k in ACTION_CATALOG),default=None) if groups else None,
            'observed_actions':sorted({a for g in groups for a in g['actions']}),'groups':groups})
    return summaries


def standby_intervals(events,start_utc,end_utc):
    start=datetime.fromisoformat(start_utc.replace('Z','+00:00'))
    end=datetime.fromisoformat(end_utc.replace('Z','+00:00'))
    if start.tzinfo is None or end.tzinfo is None: raise ValueError('Explicit timezone required')
    parsed=[]
    for event in events:
        at=datetime.fromisoformat(event['time_created_utc'].replace('Z','+00:00'))
        if at.tzinfo is None or not start<=at<=end:
            raise ValueError('Power event outside requested UTC interval')
        parsed.append((at,event))
    entered=None; spans=[]
    for at,event in sorted(parsed,key=lambda item:item[0]):
        message=event.get('message_first_line','')
        if 'entering Modern Standby' in message: entered=(at,event['time_created_utc'])
        elif 'exiting Modern Standby' in message and entered is not None:
            spans.append({'entry_utc':entered[1],'exit_utc':event['time_created_utc'],
                          'duration_seconds':(at-entered[0]).total_seconds()})
            entered=None
    return spans


def enrich_reports(directory):
    """Postprocess saved observations only; never calls either model or changes measurements."""
    import json
    from pathlib import Path
    from benchmark_report import assert_private_ids_absent
    from real_data import file_digest
    directory=Path(directory)
    metadata=json.loads((directory/'run_metadata.json').read_text())
    results=[json.loads(path.read_text()) for path in (directory/'private').glob('*.result.json')]
    effects=option_effects(results)
    pairs=[]
    for alias in ALIASES:
        original={r['repeat']:r for r in results if r['context_alias']==alias and r['representation']=='original'
                  and r['order_index']==0 and r['status']=='success'}
        compact={r['repeat']:r for r in results if r['context_alias']==alias and r['representation']=='compact' and r['status']=='success'}
        pairs.extend((original[i],compact[i]) for i in original.keys()&compact.keys())
    lines=['<!-- robustness-postprocess -->','', '## Between-order probability effects (mean of three identical repeats)','',
        '| Alias | Complete orders | Max within-identical spread | Max between-order mean probability spread | Observed actions across orders |',
        '| --- | ---: | ---: | ---: | --- |']
    fmt=lambda value:'unavailable' if value is None else f'{value:.9f}'
    for item in effects:
        lines.append(f"| {item['alias']} | {item['complete_orders']}/6 | {fmt(item['max_identical_request_spread'])} | {fmt(item['max_between_order_mean_spread'])} | {', '.join(item['observed_actions']) or 'none'} |")
    lines+=['','The between-order statistic is the largest range of an action probability across six permutation means. '
        'The within-order statistic holds the exact request fixed. Neither is a business-accuracy score.','',
        '| Alias | Order | Mean SMS_REMINDER | Mean WA_SUPPORT | Mean EMAIL_DETAILS |',
        '| --- | ---: | ---: | ---: | ---: |']
    for item in effects:
        for group in item['groups']:
            probabilities=group['mean_probabilities']
            lines.append(f"| {item['alias']} | {group['order']} | {probabilities['SMS_REMINDER']:.9f} | {probabilities['WA_SUPPORT']:.9f} | {probabilities['EMAIL_DETAILS']:.9f} |")
    lines+=['',f"Canonical original/compact action agreements: {sum(a['action_id']==b['action_id'] for a,b in pairs)}/{len(pairs)} successful same-context/repeat pairs.",'',
        'Latency measurements were taken on a shared local Windows host; diagnostic queries and a unit-test run overlapped parts of collection. '
        'They are not an isolated CPU/GPU performance benchmark. No measurement is removed on that account.','']
    evidence_path=directory/'historical_power_analysis.json'
    evidence=json.loads(evidence_path.read_text()) if evidence_path.exists() else None
    if evidence:
        longest=max(evidence['standby_intervals'],key=lambda span:span['duration_seconds'])
        lines+=['## Additional historical CTX08 evidence','',
            'Windows System events were read with explicit UTC SystemTime XPath filters and validated against the prior benchmark interval. '
            'The event messages explicitly say entering/exiting Modern Standby; their IDs were not interpreted from an assumed dictionary.', '',
            f"Longest paired standby interval: **{longest['duration_seconds']:.6f} seconds**, {longest['entry_utc']} to {longest['exit_utc']}. "
            'The old CTX08 call recorded 10,092.428628 seconds elapsed. This close temporal/duration match strongly supports a '
            'host-standby contribution rather than interpreting the spike as continuous model compute. Exact old request boundaries '
            'are absent, so exclusive per-request causality is not proven. Original latency samples remain untouched.', '',
            'An initial FilterHashtable date query returned events outside the intended UTC window. It is retained separately as '
            'an invalid-range diagnostic, excluded from this analysis. Corrected events, actual message first lines and paired '
            'durations are saved in historical_power_events.json, historical_power_event_messages.json and historical_power_analysis.json.','']
    runtime_path=directory/'runtime_detail.json'
    runtime=json.loads(runtime_path.read_text()) if runtime_path.exists() else None
    if runtime:
        lines+=['## Additional runtime details','',
            f"Actual model tag: {runtime['laya_tag']['name']}; digest `{runtime['laya_tag']['digest']}`. "
            f"Parameters: `{runtime['parameters'].strip()}`. Python {runtime['python_version']}. "
            'The frozen request has no options/seed/temperature override. Runtime_detail.json preserves tag capabilities, '
            'quantization/family, selected model metadata and loaded context information. Logical CPU count is capacity, '
            'not an observed Ollama thread count; server environment/effective thread count remain unobserved.','']
    # Gather original raw customer keys solely for report privacy checks; never render them.
    root=Path(__file__).resolve().parents[1]
    identifiers=set()
    for alias in ALIASES:
        history=json.loads((root/'outputs/benchmark/private'/alias/'history.json').read_text())
        identifiers.update(str(row[key]) for row in history for key in ('context_id','application_id','session_id') if row.get(key))
    path=directory/'report.md'
    base=path.read_text(encoding='utf-8').split('<!-- robustness-postprocess -->')[0].rstrip()
    updated=base+'\n\n'+'\n'.join(lines)
    assert_private_ids_absent(updated,identifiers)
    path.write_text(updated,encoding='utf-8')
    metadata['postprocessing']={'option_effects':effects,'canonical_compact_pairs':len(pairs),
        'canonical_compact_action_agreements':sum(a['action_id']==b['action_id'] for a,b in pairs),
        'historical_power_evidence':evidence,'analysis_code_sha256':file_digest(Path(__file__)),
        'note':'Derived report statistics and optional independently collected power/runtime details only; primary request/response/result files unchanged.'}
    metadata['final_source_sha256']={path.name:file_digest(path) for path in Path(__file__).resolve().parent.glob('*robustness*.py')}
    (directory/'run_metadata.json').write_text(json.dumps(metadata,indent=2),encoding='utf-8')
    return metadata['postprocessing']


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description='Postprocess saved robustness experiment; no inference')
    parser.add_argument('--output-dir',default='outputs/robustness')
    args=parser.parse_args()
    result=enrich_reports(args.output_dir)
    print({'canonical_compact_pairs':result['canonical_compact_pairs'],
           'canonical_compact_action_agreements':result['canonical_compact_action_agreements']})
