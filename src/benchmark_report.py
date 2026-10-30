"""Sanitized audit, paired comparisons and observed small-sample statistics."""
from collections import Counter
import csv
import io
import itertools
import json
import math
from pathlib import Path

from benchmark_inputs import STRATEGIES


def percentile(values, fraction):
    values = sorted(value for value in values if value is not None)
    if not values:
        return None
    index = (len(values)-1)*fraction
    lower, upper = math.floor(index),math.ceil(index)
    return values[lower]+(values[upper]-values[lower])*(index-lower)


def compare(left, right):
    if left.get('status')!='success' or right.get('status')!='success':
        return None
    probabilities = ('SMS_REMINDER','WA_SUPPORT','EMAIL_DETAILS')
    return {'action_changed':left['action_id']!=right['action_id'],
            'max_probability_delta':max(abs(left['probabilities'][key]-right['probabilities'][key]) for key in probabilities),
            'probability_l1_delta':sum(abs(left['probabilities'][key]-right['probabilities'][key]) for key in probabilities)}


def assert_private_ids_absent(text, identifiers):
    for value in identifiers:
        if value and value in text:
            # Never echo the offending identifier in an exception.
            raise ValueError('Customer identifier detected in public benchmark artifact')


def field_paths(value, prefix='state'):
    result = []
    if isinstance(value,dict):
        for key,item in value.items():
            path = prefix+'.'+key
            result.append(path)
            result.extend(field_paths(item,path))
    elif isinstance(value,list) and value:
        for item in value:
            result.extend(field_paths(item,prefix+'[]'))
    return sorted(set(result))


def input_audit(root):
    directory = Path(root)/'outputs/real'
    request = json.loads((directory/'laya_request.json').read_text(encoding='utf-8'))
    state = request['state']
    decision = json.loads((directory/'decision.json').read_text(encoding='utf-8'))
    qwen = json.loads((directory/'qwen_summary.json').read_text(encoding='utf-8'))
    metrics = decision.get('raw_laya_response',{}).get('usage',{})
    recent = Counter((row['at'],row['event'],row['stage'],row['substage'],row.get('application'),row.get('product'),row.get('partner'))
                     for row in state['recent_events'])
    applications = Counter((row[0],row[1],row[2],row[3],app,row[4],row[5])
                           for app,sequence in state['application_sequences'].items() for row in sequence)
    overlap = sum((recent & applications).values())
    individual = sum((recent | applications).values())
    lines = ['# Existing real-input audit', '',
             'This audit reads the saved successful real run; it does not rerun or modify it.', '',
             f"Actual server-reported Laya input tokens: **{metrics.get('input_tokens')}**. Output tokens: {metrics.get('output_tokens')}. "
             f"Saved wall-clock Laya latency: {decision['metrics']['latency_ms']} ms.",
             f"Saved Qwen input/output tokens: {decision['qwen_metrics']['input_tokens']} / {decision['qwen_metrics']['output_tokens']}; latency {decision['qwen_metrics']['latency_ms']} ms.", '',
             'Request fields: `model`, `state`, `questions.action.type`, `questions.action.instructions`, '
             '`questions.action.criteria.SMS_REMINDER`, `.WA_SUPPORT`, `.EMAIL_DETAILS`. The criteria descriptions '
             'are `sms: gentle_reminder`, `whatsapp: payment_support`, `email: balance_explanation`; all are demo-only.', '',
             '| State field | Serialized UTF-8 bytes (NOT tokens) |', '| --- | ---: |']
    lines.extend(f'| `{key}` | {len(json.dumps(value,ensure_ascii=False).encode("utf-8"))} |' for key,value in state.items())
    lines += ['', 'Nested field inventory (array positions generalized):', '']
    lines.extend(f'- `{path}`' for path in field_paths(state))
    lines += ['', '## Actual history representation', '',
              f"The saved state has {state['history_rows']} historical rows. {sum(applications.values())} appear in application sequences and "
              f"{sum(recent.values())} in recent_events; {overlap} occurrences overlap. Thus {individual} distinct row occurrences "
              f"are represented individually in those arrays; {state['history_rows']-individual} have aggregate-only representation. "
              'It is neither a full 27-row dump nor aggregates alone. Identity is the simulation assumption, not source verification.', '',
              'Duplication: full-history event counts overlap channel event/send/open/click counts and sequence labels. '
              'Some events appear in both application_sequences and recent_events. Latest journey and first/last timestamps '
              'repeat facts available in those sequences. Campaign rows often repeat the same label in event, stage and substage. '
              'Application/product/partner alias references, coverage metadata and long limitation/instruction strings add input.', '',
              f"Qwen sees only the latest {len(state['recent_events'])} chronological statements. Its validated summary selected "
              f"{len(qwen['highlights'])} statements. Laya receives the selected indices, not a new free-form analysis; "
              'the corresponding recent-event rows are already present. Qwen cannot add new source facts by construction.'
              , '',
              f"Qwen does not see the earlier {state['history_rows']-len(state['recent_events'])} rows or full-history counts. "
              'Its statements omit partner aliases and fields such as campaign_name, channel_id, initiated_by, session_id, '
              'site_subsection, platform/UTM/trigger metadata and visitnum. The compact Laya input also excludes raw identifiers '
              'and most attribution/session metadata. No DPD/PTP/balance/note was supplied.', '',
              'The field name sequence_coverage.older_rows_in_counts_only is imprecise: some older rows also appear in '
              'application_sequences. The occurrence accounting above resolves that redundancy without changing the original run.', '',
              '## Time safety and actual token interpretation', '',
              'All source rows were filtered strictly before 2026-04-15 00:00:00 in file-naive timestamp convention. '
              'Observed AIP or approval events before the cutoff are historical observations, not future labels. '
              'Journey stage/substage and other gold fields may have retrospective enrichment; no availability/ingestion timestamp '
              'is provided to rule that out. Full-month profiling statistics are not included in the state. Recency uses the fixed cutoff only.', '',
              'The 1,593 tokens are the server measurement for the entire rendered state plus question/options/template. '
              'Repeated histories, timestamps, verbose keys and repeated labels explain the input volume structurally. '
              'The API does not provide per-field token accounting; byte lengths above are diagnostics, never token estimates. '
              'The benchmark measures all new token counts from usage.input_tokens; it does not infer counts from words or bytes.', '']
    # Keep the literal audit prose simple even when older metadata fields evolve.
    return '\n'.join(lines)


def fmt(value):
    return 'unavailable' if value is None else f'{value:.2f}'


def render_report(results, manifest, metadata, qwen_runs):
    normal = [row for row in results if row['option_order']=='original']
    lookup = {(row['context_alias'],row['strategy'],row['option_order']):row for row in results}
    lines = ['# Controlled 10-context local benchmark', '',
             f"Status: {metadata['status']}. Ten fixed selected contexts; {sum(row['status']=='success' for row in results)} / {len(results)} scheduled decisions succeeded.", '',
             'This is a descriptive paired experiment, not business accuracy or optimality. No ground-truth outcomes are used. '
             'The three actions are synthetic and DEMO ONLY. Email was not observed. Context identity remains a simulation assumption.', '',
             f"Cutoff: {manifest['cutoff_exclusive']} (exclusive, file-naive). Proposed soft input target: {metadata['input_budget_tokens']} actual input tokens; "
             'over-budget requests are measured and flagged, not truncated. Configured Laya context: '
             f"{metadata.get('laya_context_window','unknown')} tokens; endpoint context errors are retained as failures.", '',
             '## Fixed selection and input control', '', manifest['selection_policy']['rule'], '',
             '| Alias | Scope | Rows | Known applications | Raw channels | Engagement label | AIP label |',
             '| --- | --- | ---: | ---: | --- | --- | --- |']
    for item in manifest['contexts']:
        lines.append(f"| {item['alias']} | {item['scope']} | {item['history_rows']} | {item['applications']} | {', '.join(item['channels']) or '(missing)'} | {item['engagement']} | {item['aip']} |")
    lines += ['', 'All strategies use precisely these aliases, frozen histories and hashes, cutoff, structured-feature object, '
              'question and criteria descriptions. Only representation differs. Reverse checks change dictionary insertion order only.', '',
              '- A: structured full-history row counts, recency and application/channel/partner aggregates; no Qwen or individual sequence.',
              '- B: exactly A plus up to eight chronological event statements and explicit window coverage; no Qwen.',
              '- C: exactly A plus one validated Qwen extractive summary of the latest eight events; Qwen called once per context, reused for reverse checks.',
              '- D: exactly A plus up to twenty chronological event statements (all available if fewer); no Qwen.', '',
              'Qwen uses the existing real-run summarizer and grounding validator unchanged. Its source statements contain timestamp, '
              'event, stage/substage, channel and application/product aliases. Partner information remains in deterministic aggregates. '
              'Qwen can select source facts, not invent new ones. Failed summaries skip C for both option orders with no fallback.', '',
              '## Actual tokens and observed latency', '',
              'Median/p95 use linear interpolation on successful original-order cases only, at most n=10 per strategy. '
              'These tiny-sample p95 values are descriptive, not reliable tail-latency estimates. Times are milliseconds and include local '
              'HTTP/model loading in each call; no estimated token counts are used.', '',
              '| Strategy | Success n | Input tokens median (min–max) | Over target | Laya median / p95 ms | Qwen median / p95 ms | Standalone total median / p95 ms |',
              '| --- | ---: | --- | ---: | --- | --- | --- |']
    for strategy in STRATEGIES:
        group = [row for row in normal if row['strategy']==strategy and row['status']=='success']
        tokens = [row['input_tokens'] for row in group]
        token_text = f'{fmt(percentile(tokens,.5))} ({min(tokens)}–{max(tokens)})' if tokens else 'unavailable'
        times = lambda key: [row[key] for row in group]
        lines.append(f"| {strategy} | {len(group)} | {token_text} | {sum(row['over_input_budget'] is True for row in group)} | "
                     f"{fmt(percentile(times('laya_latency_ms'),.5))} / {fmt(percentile(times('laya_latency_ms'),.95))} | "
                     f"{fmt(percentile(times('qwen_latency_ms'),.5))} / {fmt(percentile(times('qwen_latency_ms'),.95))} | "
                     f"{fmt(percentile(times('total_inference_latency_ms'),.5))} / {fmt(percentile(times('total_inference_latency_ms'),.95))} |")
    qwen_inputs = [value['input_tokens'] for value in qwen_runs.values() if type(value.get('input_tokens')) is int]
    qwen_outputs = [value['output_tokens'] for value in qwen_runs.values() if type(value.get('output_tokens')) is int]
    if qwen_inputs and qwen_outputs:
        lines += ['', f"Actual Qwen input tokens: median {fmt(percentile(qwen_inputs,.5))}, range {min(qwen_inputs)}–{max(qwen_inputs)}; "
                  f"output tokens: median {fmt(percentile(qwen_outputs,.5))}, range {min(qwen_outputs)}–{max(qwen_outputs)}. "
                  'These are the summarizer calls, separate from C Laya input tokens.']
    flagged = [row for row in results if (row.get('laya_latency_ms') or 0)>60000]
    if flagged:
        lines += ['', '**Timing anomaly: retain these observations, but do not interpret the affected p95 as typical model inference latency.** '
                  'The following calls exceeded the explicit 60,000 ms diagnostic flag. This is a flag, not a statistical outlier test. '
                  'No measurement was removed or replaced. Host suspension/scheduling, queueing or a service stall are possible, '
                  'but the cause is not established: the Laya response has no server compute-duration field and host telemetry was not captured.', '',
                  '| Alias | Strategy/order | Observed elapsed ms |', '| --- | --- | ---: |']
        lines.extend(f"| {row['context_alias']} | {row['strategy']}/{row['option_order']} | {row['laya_latency_ms']:.3f} |" for row in flagged)
        replay = metadata.get('latency_diagnostic',{}).get('replays',[])
        if replay:
            lines += ['', 'Two separately recorded exact-state diagnostic replays (excluded from the primary 60 calls and all tables above):']
            lines.extend(f"- {row['context_alias']} {row['strategy']}/{row['option_order']}: {row['input_tokens']} actual input tokens, "
                         f"{row['latency_ms']:.3f} ms, {row['action_id']}." for row in replay)
            lines += ['The replays test whether the timing anomaly repeats; they do not explain its original cause. '
                      'See latency_diagnostic.json. Latency rankings and tails require another controlled run before being treated as reliable.']
    lines += ['', 'Non-C Qwen costs are zero because no Qwen call occurs. C standalone total is Qwen creation latency plus Laya latency. '
              'C reverse checks reuse the same summary: their incremental latency is Laya only. Shared Qwen timing/token fields appear '
              'on both C rows for traceability and must not be summed twice. Runtime metadata separately sums each actual Qwen call once.', '',
              '## Strategy agreement', '', '| Pair | Comparable contexts | Same action | Mean maximum component probability change |',
              '| --- | ---: | ---: | ---: |']
    for left,right in itertools.combinations(STRATEGIES,2):
        comparisons = [compare(lookup[(alias,left,'original')],lookup[(alias,right,'original')]) for alias in manifest['aliases']]
        comparisons = [value for value in comparisons if value is not None]
        lines.append(f"| {left}/{right} | {len(comparisons)} | {sum(not value['action_changed'] for value in comparisons)} | "
                     f"{fmt(sum(value['max_probability_delta'] for value in comparisons)/len(comparisons) if comparisons else None)} |")
    lines += ['', '## Option-order sensitivity', '',
              '| Alias | Strategy | Original action | Reversed action | Changed | Maximum absolute probability change | L1 probability change |',
              '| --- | --- | --- | --- | --- | ---: | ---: |']
    changes = Counter()
    for alias in manifest['aliases']:
        for strategy in ('B','C'):
            left,right = lookup[(alias,strategy,'original')],lookup[(alias,strategy,'reversed')]
            comparison = compare(left,right)
            if comparison:
                changes[strategy] += comparison['action_changed']
                lines.append(f"| {alias} | {strategy} | {left['action_id']} | {right['action_id']} | {comparison['action_changed']} | "
                             f"{comparison['max_probability_delta']:.6f} | {comparison['probability_l1_delta']:.6f} |")
            else:
                lines.append(f'| {alias} | {strategy} | unavailable | unavailable | not comparable | unavailable | unavailable |')
    lines += ['', f"Measured action changes on reversal: B={changes['B']}, C={changes['C']} (denominators depend on successful paired rows above).", '',
              'Each exact state/order was evaluated once; this measures representation/order sensitivity, not repeated-identical-request '
              'test–retest stability. All Qwen summaries are prepared first. Laya strategy order rotates by context, and B/C original versus '
              'reverse call order alternates by context to reduce systematic order effects. No forced unloading, warmup, model options '
              'or context configuration changes are made. First-use/model-residency and host load remain latency confounders.', '',
              '## Failures, omitted information and provenance', '',
              f"Qwen validation/inference failures: {sum(value['status']!='success' for value in qwen_runs.values())}. "
              f"Decision status counts: {dict(Counter(row['status'] for row in results))}. No automatic fallback or retry was used.", '',
              'Required structured fields are never truncated. For B/D, only the explicit recent sequence is bounded; full-history counts '
              'are retained, and omitted sequence-row counts are saved. A loses individual chronology; C loses unselected recent events '
              'as well as older chronology. All strategies replace full application sequences with per-application aggregates: '
              'older application transitions and event-to-partner chronology are not fully represented. Raw histories remain private locally. '
              'Session, campaign attribution, raw channel/application/product/partner IDs, UTM/platform/trigger details and visit metadata '
              'are excluded. No unsupported DPD/PTP/balance/note/intent/outcome features are introduced.', '',
              'No post-cutoff event can qualify selection, identity checks or model features. Gold stage/substage may be retrospective '
              'and ingestion timestamps are absent; availability-time leakage cannot be ruled out. Product/channel semantics, consent, '
              'action_space, conversion target, attribution and deduplication remain unverified. Counts are observed rows, not unique sends.', '',
              '## Recommendation', '']
    if all(any(row['strategy']==s and row['status']=='success' for row in normal) for s in STRATEGIES):
        lines += ['Use **B (structured + last eight events)** as the simple next-phase baseline: it exposes direct recent chronology '
                  'without a Qwen dependency. A is the cheaper compression candidate when recent ordering is deliberately dispensable; '
                  'D retains more observable chronology when the added history is needed. If application transitions matter, retain '
                  'those explicitly before treating any compressed strategy as sufficient.', '',
                  'Qwen has **no demonstrated necessity for the next phase** in this experiment. It selects at most three statements '
                  'already available to B, adds measured latency, and any agreement or disagreement lacks business ground truth. '
                  'This is not evidence that Qwen never helps, nor that unchanged actions imply unchanged utility or correct decisions. '
                  'Evaluate an approved action_space and outcome objective on a broader cohort before choosing a production representation.']
    else:
        lines += ['Actual coverage is incomplete; do not infer Qwen necessity or a measured best strategy from missing comparisons. '
                  'B is a simple candidate by construction, but the failed/incomplete experiment must be resolved first.']
    lines += ['', 'Full per-call probabilities, actions, actual tokens, sizes, budgets, time/order and errors are in results.csv. '
              'Exact requests, raw responses and Qwen artifacts are in private per-alias folders; only alias-based reports should be shared.', '']
    return '\n'.join(lines)


CSV_FIELDS = ('context_alias','strategy','option_order','status','action_id','SMS_REMINDER','WA_SUPPORT','EMAIL_DETAILS',
              'input_tokens','output_tokens','request_bytes','input_budget_tokens','over_input_budget','context_window',
              'qwen_input_tokens','qwen_output_tokens','qwen_latency_ms','laya_latency_ms','total_inference_latency_ms',
              'incremental_latency_ms','qwen_summary_reused','call_index','state_sha256','history_sha256','error_code')


def public_csv(results):
    stream = io.StringIO(newline='')
    # Windows text output will translate LF once; embedded CRLF here creates blank CSV rows.
    writer = csv.DictWriter(stream,fieldnames=CSV_FIELDS,lineterminator='\n')
    writer.writeheader()
    for result in results:
        flat = {key:result.get(key) for key in CSV_FIELDS}
        flat.update(result.get('probabilities',{}))
        writer.writerow(flat)
    return stream.getvalue()


def write_reports(directory, results, manifest, metadata, qwen_runs, identifiers, audit):
    texts = {'results.csv':public_csv(results),'input_audit.md':audit,
             'report.md':render_report(results,manifest,metadata,qwen_runs)}
    for text in texts.values():
        assert_private_ids_absent(text,identifiers)
    for name,text in texts.items():
        target = Path(directory)/name
        temporary = target.with_suffix(target.suffix+'.tmp')
        temporary.write_text(text,encoding='utf-8')
        temporary.replace(target)
