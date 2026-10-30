"""Evidence-based senior walkthrough generated from actual saved measurements."""
from collections import Counter
import csv
import json
import statistics

from benchmark_report import assert_private_ids_absent,percentile
from benchmark_inputs import recent_records


def display(value): return 'unavailable' if value is None else str(value)


def generate_reports(root,output,metadata,real,stress,retrieval,private_ids):
    benchmark=list(csv.DictReader((root/'outputs/benchmark/results.csv').open(newline='')))
    qwen={folder.name:json.loads((folder/'qwen/validated_result.json').read_text()) for folder in (root/'outputs/benchmark/private').glob('CTX*')}
    robust=json.loads((root/'outputs/robustness/run_metadata.json').read_text())
    profile=json.loads((root/'outputs/real/profile.json').read_text())
    groups={s:[r for r in benchmark if r['strategy']==s and r['option_order']=='original' and r['status']=='success'] for s in ('B','C')}
    paired=[(b,c) for b in groups['B'] for c in groups['C'] if b['context_alias']==c['context_alias']]
    agreement=sum(b['action_id']==c['action_id'] for b,c in paired)
    med=lambda values:percentile(values,.5)
    def metric(strategy,key):
        value=med([float(r[key]) for r in groups[strategy] if r.get(key) not in ('',None)])
        return round(value,3) if value is not None else None
    qwen_stats={'input':med([r.get('input_tokens') for r in qwen.values()]),
        'output':med([r.get('output_tokens') for r in qwen.values()]),
        'latency':med([r.get('latency_ms') for r in qwen.values()])}
    architecture=['# Current technical architecture and proposed production components','',
        'Current components are local and working. Proposed components below are requirements, not implemented production capabilities.','',
        '```mermaid','flowchart LR','  P[March and April Parquet] --> D[DuckDB projected scans]',
        '  D --> H[One context and strict historical cutoff]','  H --> F[Deterministic Python counts and recency]',
        '  H --> R[Last eight events plus explicit coverage]','  F --> S[Bounded recent sequence and cardinality-dependent aggregates]',
        '  R --> S','  S --> L[Local Laya System One]','  L --> V[Validate catalog choice and probabilities]',
        '  V --> J[Local JSON and evidence reports]','```','',
        'No summarization model is called by run_final_poc.py. Qwen remains installed and is used only through previously saved comparison artifacts. '
        'DuckDB scans millions of rows without pandas; only selected context history is fetched into Python. '
        'The real demo reuses connect_inputs, build_real_state, structured_features, compact_state and the working System One transport/validator. '
        'A 30-second HTTP timeout and 45-second supervised deadline bound each new inference; late/failed responses stay failures.','',
        '## Proposed production responsibilities (not delivered)','',
        '| Component | Required contract or validation |','| --- | --- |',
        '| Event ingestion and availability ledger | Event IDs/deduplication, event-time versus ingestion-time, backfill handling |',
        '| Entity resolver | Authoritative stable customer/application/product identities and cross-month linkage |',
        '| As-of feature service | Versioned counts, recency, missingness and coverage; explicit cardinality/context overflow policy |',
        '| Action/eligibility service | Approved action_space, channels/themes/content, consent, contactability and suppressions |',
        '| Model evaluation and serving | Domain labels, calibrated evaluation, order/format robustness, reproducible runtime |',
        '| Decision/execution separation | SEND/WAIT, daily cap, timing, budget, operational controls handled outside model |',
        '| Outcome and policy evaluation | Agreed reward and horizon, exposure/propensity logging, causal identification if lift is claimed |',
        '| Observability | Request boundaries, exact tokens, latency/timeout failures, drift and privacy-safe audit trail |','',
        'None of these proposed components is evidence of production readiness. Original action order is fixed solely for reproducibility; it does not remove ordering bias.','']
    compression=['# Raw rows versus exact Laya input tokens','',
        f"Available dataset: {metadata.get('dataset_rows','unavailable')} rows; strictly pre-cutoff: {metadata.get('dataset_rows_before_cutoff','unavailable')}. "
        'Cutoff is 2026-04-15 00:00:00 exclusive, in the files\' timezone-naive convention. Dataset-level/post-cutoff counts below are reporting diagnostics only and never model features.','',
        '## Actual real observations (separate from stress fixtures)','',
        '| Alias / scope | Context rows in scope, all times | Pre-cutoff history | Unique event labels | Apps / partners | Recent individual / total retained | Aggregate-only rows | Actual tokens | HTTP ms | Status |',
        '| --- | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | --- |']
    for row in real:
        compression.append(f"| {row['alias']} / {row['scope']} | {row['context_scope_rows_all_times']} | {row['historical_rows']} | {row['event_types']} | {row['applications']} / {row['partners']} | {row['recent_individual_events']} / {row['retained_individual_events']} | {row['aggregate_only_rows']} | {display(row.get('input_tokens'))} | {display(row.get('http_latency_ms'))} | {row['status']} |")
    compression+=['',f"The largest unfiltered historical April context has {metadata.get('largest_unfiltered_april_history_rows','unavailable')} rows. "
        'APRIL_MAX_VALID is the largest passing the existing strict historical application/context/single-known-product checks; '
        'it is not a claim that the largest raw context is a clean customer. Three other cases are frozen benchmark contexts with existing identity evidence. '
        'Selection is based on pre-cutoff observations; linked cases inherit the prior validated scope and simulation identity assumption.','',
        'All historical event-type counts, per-channel communication/engagement counts and recencies, per-application summaries, '
        'partner aggregates, missingness, source scope and cutoff are retained. Eight recent chronological occurrences are individually encoded; '
        'latest observed journey/AIP evidence can add referenced older rows. Source occurrences are not deduplicated. '
        'All selected pre-cutoff raw rows are saved privately. Older chronology, full application transitions, event-to-partner chronology, '
        'session/UTM/campaign attribution and other reporting fields are not all represented individually. These are explicit coverage limitations.','',
        '## Clearly synthetic stress fixtures: NOT customer observations','',
        'These fixtures stream deterministic generated events into Python counters without materializing a million-row list. '
        'They use a distinct explicit aggregate schema rather than the real compact-table encoder. Thus they demonstrate row-count scaling '
        'at fixed feature cardinality, not predictions of real-customer token counts or business behavior.','',
        '| Generated rows | Vocabulary | Applications / partners | Individually recent | Earlier rows in aggregates | Exact tokens | HTTP ms | Status |',
        '| ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |']
    for row in stress:
        compression.append(f"| {row['generated_history_rows']} | {row['vocabulary']} | {row['applications']} / {row['partners']} | 8 | {row['earlier_rows_in_aggregates']} | {display(row.get('input_tokens'))} | {display(row.get('http_latency_ms'))} | {row['status']} |")
    compression+=['','The recent sequence is bounded; the entire input is **not unconditionally bounded**. '
        'Repeated rows mainly change integer counts/recency values, but event vocabulary, application/partner counts, product sets and ties '
        'can expand aggregates. The high-cardinality fixture measures this separately. A production feature contract needs an explicit '
        'cardinality/context-overflow policy. This POC never drops fields or retries with a shorter state to make a request fit. '
        'Endpoint/timeout errors leave token counts unavailable rather than estimated. No words/bytes are converted into token estimates.','',
        'Inspect compression_real.csv and compression_synthetic_stress.csv for each measured action, probabilities and timings. '
        'Exact input/response, full selected raw history, identifier map and change ledger are in private per-alias folders. '
        'Only HTTP/model request latency is inference latency: selection/preprocessing/fixture-generation and worker startup are separate costs. '
        'All final direct cases have zero Qwen calls; this is not a multimodel pipeline latency.','']
    comparison=['# Qwen comparison: optional for this structured-event POC','',
        'This section reuses the saved Phase 2 benchmark. No new Qwen inference was performed and none of those observations was overwritten.','',
        '| Method | Source representation | Median actual Laya input tokens | Median Laya ms | Median Qwen ms | Median standalone inference total ms |',
        '| --- | --- | ---: | ---: | ---: | ---: |',
        f"| B | Deterministic full-history Python features + last eight event statements | {metric('B','input_tokens')} | {metric('B','laya_latency_ms')} | 0 | {metric('B','total_inference_latency_ms')} |",
        f"| C | Identical Python features + grounded extractive Qwen summary of last eight | {metric('C','input_tokens')} | {metric('C','laya_latency_ms')} | {qwen_stats['latency']} | {metric('C','total_inference_latency_ms')} |",'',
        f"Qwen actual median input/output tokens: {qwen_stats['input']} / {qwen_stats['output']}. "
        f"Valid Qwen summaries: {sum(q['status']=='success' for q in qwen.values())}/{len(qwen)}. B/C original-order action agreement: {agreement}/{len(paired)}.",'',
        'The existing summarize_events contract in src/run_real.py uses qwen2.5:1.5b, non-streaming JSON schema mode, temperature zero, '
        'and one to three highlights. Each highlight must carry a valid event_index and copy the entire supplied fact exactly. '
        'validate_event_summary enforces exact index/text matching, unique chronological indices and the 1..3 size bound. '
        'Qwen selected source statements; it did not generate new domain features, deterministic counts, payment outcomes, intent or causal reasoning.','',
        'Qwen **succeeded operationally**. The experiment does not demonstrate downstream business value: only one B/C action differs '
        'and no outcome ground truth determines which is better. A summary can focus information, but information selection is not new source evidence. '
        'Python already computes the structured facts and can pass bounded events directly. Qwen is optional here, not proven useless for every task '
        'or for future unstructured customer notes.','',
        'Latency caveat: original B p95 is affected by the preserved CTX08 host-standby outlier. Medians above are observed values, '
        'not controlled production timing estimates. C total includes Qwen creation once plus Laya; it is the median of per-case sums, '
        'not the sum of medians. Shared C reverse-check summary costs are not counted twice.','']
    # Aliased actual example: same eight source events and actual validated Qwen output.
    example_alias='CTX04'; source=root/'outputs/benchmark/private'/example_alias
    history=json.loads((source/'history.json').read_text()); identity=json.loads((root/'outputs/benchmark/private/identity_map.json').read_text())[example_alias]
    private_ids.update(str(row[key]) for row in history for key in ('context_id','application_id','session_id') if row.get(key))
    projections=recent_records(history,identity['aliases'],8)
    b=json.loads((source/'B_original.request.json').read_text()); c=json.loads((source/'C_original.request.json').read_text())
    q=qwen[example_alias]
    for name,value in (('example_B_request.json',b),('example_C_request.json',c),('example_Qwen_validated.json',q)):
        (output/'private'/name).write_text(json.dumps(value,indent=2),encoding='utf-8')
    comparison+=['## Same historical evidence through four stages: aliased CTX04','',
        f'This is an actual saved benchmark case, not a newly fabricated Qwen response. The private source has {len(history)} rows and 24 original columns; '
        'the following is a privacy-safe projection of its same latest eight rows. Raw customer/application/product/partner identifiers '
        'are replaced with local aliases. Timestamps and event labels are preserved; this projection is not the complete raw file.','',
        '### Raw-record projection','', '```json',json.dumps(projections,indent=2),'```','',
        f'### Deterministic Python aggregate excerpt (all {len(history)} rows)','',
        '```json',json.dumps({k:b['state']['structured'][k] for k in ('observed_rows','event_counts','channel_observations','application_unassigned_rows')},indent=2),'```','',
        'The aggregates retain whole-history counts/recency and missing groups but do not themselves retain row-by-row order. '
        'This excerpt omits other application/partner features for readability only; the full saved B request is copied under private/example_B_request.json.','',
        '### Actual Qwen output','', '```json',json.dumps(q['summary'],indent=2),'```','',
        f'Qwen saw the same eight chronological source statements but not the other {len(history)-8} historical rows or full-history counts. '
        'Its statement format excludes partner aliases. The validated output keeps three selected statements and drops five of those eight '
        'from its summary; it cannot preserve omitted chronology or create domain features.','',
        '### Actual final Laya input excerpt, direct B versus Qwen C','',
        '```json',json.dumps({'B':{'chronological_events':b['state']['chronological_events'],'sequence_coverage':b['state']['sequence_coverage']},
            'C':{'grounded_event_summary':c['state']['grounded_event_summary'],'summary_coverage':c['state']['summary_coverage']}},indent=2),'```','',
        'Both actual inputs also contain exactly the same full-history structured features and identical question/options. '
        'The complete C request is copied under private/example_C_request.json. These are existing benchmark requests, not newly measured '
        'final-demo cases. The new real demonstration uses the separate documented compact-table encoding and records its exact requests.','']
    utilization=['# Synthetic factual context-utilization diagnostic','',
        'These are synthetic, non-customer records. Each contains exactly one explicit badge-color fact and otherwise identical neutral filler. '
        'For each filler size, RED, BLUE and GREEN are balanced counterfactual answers at beginning/middle/end. The question and option order '
        'stay fixed. Position, expected answer and size metadata are stored outside model input, so they cannot leak the answer. '
        'Only the intended color fact differs across counterfactual colors; for a given color/size the state multiset is identical across positions.','',
        '| Filler records | Color | Position | Actual input tokens | >512 | Endpoint/validation status | Selected answer | Correct retrieval | Probabilities | HTTP ms |',
        '| ---: | --- | --- | ---: | --- | --- | --- | --- | --- | ---: |']
    for r in retrieval:
        utilization.append(f"| {r['filler_count']} | {r['correct_answer']} | {r['position']} | {display(r.get('input_tokens'))} | {display(r.get('over_512_tokens'))} | {r['status']} | {r.get('answer','unavailable')} | {r.get('retrieval_correct','not scored')} | {json.dumps(r.get('probabilities',{}),sort_keys=True)} | {display(r.get('http_latency_ms'))} |")
    successful=[r for r in retrieval if r['status']=='success']; correct=sum(r.get('retrieval_correct',False) for r in successful)
    utilization+=['',f"Successful endpoint processing plus schema validation: {len(successful)}/{len(retrieval)}. "
        f"Correct factual retrieval: {correct}/{len(successful)} processed cases. Processing success is **not** retrieval success.",'',
        '| Position | Processed | Correct |','| --- | ---: | ---: |']
    for pos in ('beginning','middle','end'):
        group=[r for r in successful if r['position']==pos]
        utilization.append(f"| {pos} | {len(group)} | {sum(r['retrieval_correct'] for r in group)} |")
    utilization+=['','| Filler records | Actual input token range | Processed | Correct |',
        '| ---: | --- | ---: | ---: |']
    for size in sorted({r['filler_count'] for r in retrieval}):
        group=[r for r in successful if r['filler_count']==size]
        tokens=[r['input_tokens'] for r in group]
        utilization.append(f"| {size} | {min(tokens) if tokens else 'unavailable'}–{max(tokens) if tokens else 'unavailable'} | {len(group)} | {sum(r['retrieval_correct'] for r in group)} |")
    misses=[r for r in successful if not r['retrieval_correct']]
    if misses:
        utilization+=['','Observed factual mistakes (processing still succeeded):']
        utilization.extend(f"- {r['input_tokens']} tokens, {r['position']} position, expected {r['correct_answer']}, returned {r['answer']}." for r in misses)
    if successful:
        utilization+=['',f"Largest factually tested input: {max(r['input_tokens'] for r in successful)} actual tokens. "
            'Successful retrieval at tested positions is limited to these fixtures. No factual retrieval evidence is supplied '
            'between the largest factual fixture and the configured 8192-token limit; larger processed business/stress requests do not fill that evidence gap.']
    utilization+=['','Sizes are classified using actual server usage, not word/byte estimates. Small and larger fixtures aim to bracket 512 tokens, '
        'but 512 is not an assumed model cutoff. Loaded/configured context is recorded in run_metadata.json. '
        'These small, repetitive, known-answer fixtures do not establish a universal effective window, uniform attention to every field, '
        'good business recommendations, calibrated outcome probabilities or correctness on arbitrary long context. '
        'Errors are kept as processing failures, not incorrect answers or fabricated token counts. No action/theme conclusions follow from this color task.','']
    evidence=['# Outcome and action-contract evidence','',
        '| Literal label | March rows | April rows | What is established / unresolved |','| --- | ---: | ---: | --- |']
    labels={'campaign_sent':'Send-labelled exposure candidate; not proof of delivery or unique action',
        'campaign_open_read':'Open/read-labelled engagement; channel-specific semantics/deduplication unknown',
        'campaign_clicked':'Click-labelled engagement; not causal attribution',
        'AIP':'Literal AIP label; multiple substages, not uniformly approval',
        'aip  ok':'Literal spelling linked to AIP Approved substage; business guarantee unverified',
        'aip ok':'Separate raw spelling linked to AIP Approved substage; do not silently merge',
        'aip not ok':'Literal label linked to AIP Declined; business scope unverified',
        'Disbursed':'Disbursement-labelled candidate outcome; contractual loan/payment/conversion meaning unverified'}
    for label,meaning in labels.items():
        counts=[next((r['count'] for r in profile['months'][month]['distributions']['event_name'] if r['value']==label),0) for month in ('march','april')]
        evidence.append(f'| `{label}` | {counts[0]} | {counts[1]} | {meaning} |')
    evidence+=['','## Observed channels and missingness','',
        '| Channel | March rows | April rows | Contract gap |','| --- | ---: | ---: | --- |']
    for channel in ('SMS','WA','GRCS',''):
        counts=[next((r['count'] for r in profile['months'][month]['distributions']['channel'] if r['value']==channel),0) for month in ('march','april')]
        gap={'SMS':'Recognizable communication label; consent/eligibility/themes still unknown',
             'WA':'Campaign-associated communication code; exact routing/WhatsApp expansion requires dictionary',
             'GRCS':'Campaign-associated code; transport meaning and action eligibility unknown',
             '':'Missing channel is not an action; raw blank/NULL semantics require confirmation'}[channel]
        evidence.append(f"| {channel or '(blank/missing)'} | {counts[0]} | {counts[1]} | {gap} |")
    evidence+=['','There are no NULL channel values in either file, but blank strings exist as shown. The model feature group `(missing)` '
        'combines blank/NULL by the established builder; raw distinctions remain in source files. No Email channel was observed. '
        'SMS_REMINDER/gentle_reminder, WA_SUPPORT/payment_support and EMAIL_DETAILS/balance_explanation are unchanged demo strings, '
        'not authoritative approved channel/theme combinations. GRCS is not assumed eligible; no new themes, consent or contactability are invented.','',
        '## Historical decision/outcome examples: possible design, not implemented labels','',
        'An approved exposure/action event could define a decision time. Construct features strictly from earlier available events; '
        'keep later candidate outcomes in a separate labelled evaluation table, never model input. Validate identity and application/product scope, '
        'event IDs/deduplication, ingestion/availability time, outcome definitions, attribution and observation/censoring windows first. '
        'AIP-related or Disbursed labels could be inspected as candidate targets only after the business owner defines their meaning. '
        'The current monthly data does not identify authoritative theme IDs, randomized assignments or policy propensities.', '',
        'No causal action lift is calculated from these frequencies. Observed engagement/disbursement associations are not treatment effects. '
        'Laya option probabilities are not PTP, payment, loan, conversion, reward or action-lift probabilities. '
        'A technical inference pipeline alone cannot establish XLNet representation replacement or replacement of an RL business objective.','']
    original_summary=robust['summary']
    senior=['# Five-minute senior-developer walkthrough','',
        'Use the prepared measurements; a full fresh experiment is not claimed to finish within five minutes.','',
        '1. **0:00–0:45 — Scope and source.** Open architecture.md. Point to 9,856,993 rows across the two local Parquet files and the exclusive April 15 cutoff. DuckDB filters/projects; no full pandas load and no cloud service.',
        '2. **0:45–1:45 — Compression.** Open compression_real.csv and compression_explanation.md. Compare context histories, event-type/application/partner cardinality, retained eight-event sequence and actual Laya tokens. Then show the clearly separate million-row synthetic fixture and cardinality stress.',
        '3. **1:45–2:30 — Actual request/output.** Inspect private/CTX01/state.json, request.json and response.json locally; do not share raw selected_history.json or identifier maps. Show demo choice probabilities and HTTP/total direct inference milliseconds. The fixed ordering is a reproducibility convention, not a bias fix.',
        '4. **2:30–3:15 — Why no small LLM is required.** Open qwen_comparison.md. B computes facts in Python and passes eight events; C selected at most three existing statements. Show 10/10 valid summaries and 9/10 action agreement without claiming either action is correct.',
        '5. **3:15–4:00 — Window diagnostic.** Open context_utilization.md. Compare actual tokens, processed cases and correct color retrieval by position/size. A valid response does not prove all information was read or good channel recommendations.',
        '6. **4:00–5:00 — Verdict and next contracts.** Open poc_verdict.md and outcome_action_evidence.md. Identical requests repeat, but all five tested contexts show ordering effects. No business outcomes, production readiness, causal lift or XLNet/RL replacement has been established.','',
        '## Reproduce without overwriting','',
        '```powershell',r'cd C:\Aryan\Attributics\Laya_POC',r'.\.venv\Scripts\python.exe -m unittest discover -s tests -v',
        r'.\.venv\Scripts\python.exe .\src\run_final_poc.py --output-dir outputs\final_poc_repeat','```','',
        'No automatic package installation, model pull, fine-tuning, Qwen call or original-output modification occurs. '
        'Full raw histories and worker errors stay private; reports/CSVs are alias-based.','']
    verdict=['# Final technical POC verdict','',f"New run status: {metadata['status']}. Prior files unchanged: {metadata['protected_files_unchanged']}.",'',
        '## Achieved (bounded by recorded evidence)','',
        f"- Direct real structured-data → Python features → local Laya: {sum(r['status']=='success' for r in real)}/{len(real)} cases processed and validated. No Qwen call was required.",
        '- Exact requests, server-reported tokens, actual choices/probabilities, local inference timings, selected histories and explicit missingness/source/cutoff coverage were saved.',
        '- A bounded recent-event sequence plus deterministic aggregates avoids sending millions of rows to the model. Synthetic fixed-cardinality stress and aggregate-cardinality growth are reported separately from customer observations.',
        f"- Prior Qwen operation succeeded ({sum(q['status']=='success' for q in qwen.values())}/{len(qwen)} validated summaries). Its additional downstream business value is not demonstrated by {agreement}/{len(paired)} action agreement without ground truth.",
        f"- Prior robustness evidence: {original_summary['complete_identical_groups']}/30 identical-request groups completed, {original_summary['variable_identical_groups']} variable-action groups; all {original_summary['order_sensitive_contexts']}/5 contexts changed decision under some option order. Those 105 calls were not rerun.",
        f"- New synthetic factual processing {len(successful)}/{len(retrieval)}; correct retrieval {correct}/{len(successful)} processed cases. These are distinct claims.",'',
        '## Unresolved','',
        '- Order/format sensitivity, low margins, domain accuracy/calibration, utility of alternative representations and general long-context retrieval.',
        '- Input is bounded in sequence length, not in arbitrary event/application/partner cardinality. Required facts must not be silently truncated; overflow needs an approved feature policy.',
        '- Source customer identity and gold-field availability/backfill contracts remain unverified. Event-time filtering is enforced but availability-time leakage cannot be ruled out.',
        '- Latencies are observed on a shared local host and small selected samples; previous CTX08 standby-related measurements remain preserved, not normal inference estimates.','',
        '## Blocked claims / required evidence','',
        '- Authoritative action_space, transport codes, eligibility/consent/templates and execution policy are needed before meaningful action decisions.',
        '- Business-defined outcome/reward windows, exposure/propensity records, deduplication and leakage-safe labelled evaluation are needed before quality or causal action-lift claims.',
        '- A direct technical interface does not prove replacement of XLNet features or the RL policy/business objective. No training, policy optimization, fine-tuning or production execution was performed.',
        '- Choice probabilities are only relative scores over supplied options, not business-outcome probabilities.','',
        '**Verdict:** Direct deterministic structured-data-to-Laya inference is a working local technical path when the recorded calls succeed. '
        'Qwen is optional for these structured events and has no demonstrated incremental business value in the current evidence. '
        'Input budgets, ordering robustness and domain/action/outcome contracts remain necessary before claiming XLNet/RL replacement.','']
    artifacts={'architecture.md':architecture,'compression_explanation.md':compression,'qwen_comparison.md':comparison,
        'context_utilization.md':utilization,'outcome_action_evidence.md':evidence,'senior_demo.md':senior,'poc_verdict.md':verdict}
    for name,lines in artifacts.items():
        text='\n'.join(lines); assert_private_ids_absent(text,private_ids)
        (output/name).write_text(text,encoding='utf-8')
