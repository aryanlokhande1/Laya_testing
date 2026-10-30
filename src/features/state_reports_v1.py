"""Aggregate-only documentation for the compact state; no customer samples printed."""
import json
from pathlib import Path

from data.combine_parquets import ROOT
from features.state_projection_v1 import CHANNELS,CHANNEL_METRICS,SCALARS,TABLE_COLUMNS,selected_lineage


def generate_reports(run_dir: Path, result: dict):
    repository=result.get('repository_audit',{})
    total=repository.get('stage_audit',{}).get('rows',0)
    population='# Original field population\n\nAll values are aggregate counts over the unchanged original feature repository.\n\n| Column | SQL type | Non-NULL rows | NULL rows | Population % | NULL % |\n|---|---|---:|---:|---:|---:|\n'
    for field in repository.get('field_population',[]):
        missing=field['null_rows'];present=total-missing
        population+=f"| {field['column']} | {field['sql_type']} | {present} | {missing} | {100*present/total if total else 0:.6f} | {100*missing/total if total else 0:.6f} |\n"
    (run_dir/'field_population.md').write_text(population,encoding='utf-8')
    summary='# Compact Laya state v1 execution\n\n'
    summary+='This build creates only features.laya_state_v1. Existing raw events, 104-column features, dictionary, source Parquets, consolidated Parquet and inference artifacts are protected. Credentials are read locally from .env.postgres and never included here.\n\n'
    summary+='```json\n'+json.dumps(result,indent=2,default=str)+'\n```\n\n'
    summary+='Read-only full-table fingerprints use SHA256 over ordered per-row SHA256 digests of fixed-format PostgreSQL record text, plus columns, constraints, indexes, owners, privileges and comments. They are not just counts or approximate database statistics. Files use streaming SHA256. Before/after proof is preserved alongside execution.json.\n\n'
    summary+='The SQL build locks the three protected tables against concurrent writes during its transaction, then publishes the new table only after all SQL checks pass. Target-exists checks refuse replacement. No model requests, choices, training or campaign execution occur. A local temporary automatic-idle-sleep request is released on completion; no power plan or global PostgreSQL configuration is changed.\n'
    (run_dir/'execution_summary.md').write_text(summary,encoding='utf-8')
    selection='# Feature selection\n\n'
    selection+=f'The table has {len(TABLE_COLUMNS)} columns: {len(SCALARS)} behavioral scalars, three internal scalars (context_id, feature_as_of, state_schema_version), and seven JSONB columns. Only state_json is model-facing.\n\n'
    selection+='For each confirmed channel SMS, WA and GRCS, reuse six verified measurements: observed send/open-read/click row counts, hours since observed send/engagement, and sends in seven days. These are raw event-row observations, not delivered or uniquely attributed messages. NULL recency remains NULL. Channel NULL and blank counts remain separately visible in quality_flags; event objects preserve the original blank/NULL value.\n\n'
    selection+='Other retained scalars: total observed rows, observed first-to-last event span (not lifetime or complete collection), missing-channel rows and missing-channel sends in seven days, overall known-channel send pressure, distinct observed applications/products, last recorded event label/recency, latest non-campaign label/usable source stage/substage/recency, and days with recorded events in seven days. Observed event days include automated campaign activity and are not verified user activity.\n\n'
    selection+='| Excluded representation | Reason |\n|---|---|\n'
    for name,why in [
      ('Absolute last-send/engagement timestamps','Equivalent per-channel hours-since values retained; source timestamps remain available for audit.'),
      ('Unmatched open-event-row ratios','Diagnostics only, never verified matched rates; remain in original table.'),
      ('Matched response rates/delays/affinity','No approved message linkage, complete response windows or affinity parameters.'),
      ('Previous/current application and raw last application/product IDs','Current application selection unverified; only distinct observed counts retained.'),
      ('Context, raw application/session/partner identifiers','Internal lookup and event_audit only; forbidden in model state.'),
      ('Full campaign, partner, product and application dictionaries','Large and identifier-heavy; preserved unchanged in original repository.'),
      ('Combined recent_events and campaign-valued latest journey fields','Replaced by independent communication/non-campaign sequences and raw non-campaign selection.'),
      ('User-initiated activity recency','Literal initiated_by mapping is unapproved; no fabricated zero.'),
      ('Visitnum-derived visit interpretation, progression scores and tariffs','Business meanings or objectives unverified; none introduced.'),
      ('Reporting disclaimers, experimental labels and catalogs','Quality/contract information lives outside model state; no action catalog is defined.')]:
        selection+=f'| {name} | {why} |\n'
    audit=result.get('repository_audit',{})
    selection+='\nActual original field population, including all 104 NULL counts, is recorded in repository_audit.json. Exclusion does not delete source facts: original tables remain unchanged and retrievable. AIP counts and recency survive independently of sequence limits.\n'
    (run_dir/'feature_selection.md').write_text(selection,encoding='utf-8')
    contract='''# Model-facing state contract: laya_state_v1

Only state_json is intended for eventual Laya input. context_id is an internal source grouping key, not a verified customer ID. It is NOT present in this JSON. state_schema_version, quality_flags and event_audit are also outside it.

Snapshot: 2026-05-01 00:00:00 exclusive, using unresolved timezone-naive source convention. This is a customer-history snapshot, not a training state for decisions earlier in March or April. All event recencies are hours before that fixed snapshot; no current system time is used. Numeric recencies are not silently rounded. Missing recency/stage evidence is JSON null; unavailable advanced metrics are absent, not zero.

| Root key | Exact structure / interpretation |
|---|---|
| history | as_of, event_rows, observed_span_hours, missing_channel_event_rows, communication_coverage and non_campaign_coverage. Each coverage has available_rows and retained_rows. Span is last minus first recorded event, not complete lifetime or collection-window coverage. |
| channels | Exactly SMS, WA, GRCS; each contains send_rows, open_read_rows, click_rows, hours_since_last_send, hours_since_last_engagement, send_rows_7d. |
| contact_pressure_7d | known_channel_send_rows and missing_channel_send_rows; never deliveries or unique-message counts. |
| applications / products | Each has distinct_observed, with no raw identifier or active/current classification. |
| activity | observed_event_days_7d; includes system and campaign observations, not verified user activity. |
| latest_event | event_name and hours_before_snapshot; any source event category. |
| latest_non_campaign_journey | event_name, stage, substage, hours_before_snapshot. These fields describe the SAME latest non-campaign event. No older-stage carry-forward or hierarchy inference. |
| aip_history | Exact observed source labels as keys. Each has event_rows and hours_since_last. AIP, aip ok, aip double-space ok and aip not ok are separate. An absent observed label is absent from the map, never inferred as a business outcome. |
| recent_communication_events | Array of event_name, channel, hours_before_snapshot. |
| recent_non_campaign_journey_events | Array of event_name, channel, hours_before_snapshot, stage, substage. |

The exact case-sensitive communication classification is campaign_sent / campaign_open_read / campaign_clicked. Other nonblank labels form the operational non-campaign category; this does not certify their business journey semantics. NULL/blank event labels are unclassified and retained in audit counts, not silently classified as a journey event. Blank or campaign-token-valued journey stage/substage becomes NULL with a rejection/availability flag outside the model input. Other nonblank source stage/substage strings are retained exactly, without asserting approval, completion or authoritative current status.

The initial sequence limits are four communication events and four non-campaign events. CLI options configure them independently. They are provisional representation choices, not contact policy or business rules. Each array is chronological (hours_before_snapshot descending). Underlying timestamp, source filename and physical row number break ties reproducibly; equal timestamps do not establish causal order. Source references and original application/stage fields remain in event_audit. Exact source-payload duplicates remain separate observed rows in counts and sequences.

Strict allowlists are implemented in state_projection_v1.py. SQL validation also checks all actual context/application/session/partner identifiers against the model's string values; no raw identifier is intentionally projected. No raw ratios, matched rates, affinity, current application, user-initiated classification, experimental labels, action options, costs or business disclaimers are inserted into state_json.

No decision-accuracy, confidence, token-count or outcome-improvement claim is made. There are no model requests. Payload sizes, when reported, are serialized UTF-8 bytes only.
'''
    (run_dir/'state_contract.md').write_text(contract,encoding='utf-8')
    quality='# Observed findings and unresolved contracts\n\n'
    quality+='Observed audit results (aggregate only):\n\n```json\n'+json.dumps({'repository_audit':audit,'duplicate_audit':result.get('duplicate_audit'),'table_summary':result.get('table_summary')},indent=2)+'\n```\n\n'
    quality+='The former latest-journey logic accepted any recorded journey fields, including campaign event tokens. This is corrected only in the new table; the original calculation is deliberately untouched. Recent campaign dominance motivates two independent sequences rather than reducing communication evidence or inventing a journey hierarchy.\n\n'
    quality+='Duplicate exposure is computed from exact equality across all 24 original payload fields, excluding provenance. A bounded candidate-hash pass is followed by exact comparison, so hash collisions cannot create false duplicate groups. Per-context groups, excess rows, rows in groups and communication/non-campaign excess are audit metadata. They indicate potential count inflation; identical payloads do not prove illegitimate business duplicates. No deduplication occurs.\n\n'
    quality+='Partner fields with separators are an observed lexical property, not a verified component mapping. No composite identifier is parsed into an invented partner meaning or included in the model state. All partner/campaign dictionaries remain in the old tables.\n\n'
    quality+='Application lookup is read-only and scoped to the historical snapshot. The utility returns no matching context, unique context, or multiple contexts with all candidate IDs returned privately to its caller. CLI output prints only status/count. No most-recent-context fallback is allowed. A unique observed mapping does not prove authoritative customer identity or identify the current application.\n\n'
    quality+='Still required: verified context-to-customer mapping; current-application selection; application/product relationship semantics; partner composite encoding; initiated_by classification; source timezone and event-availability timestamps; unique message identity/response attribution; complete telemetry and response windows; duplicate business-event rules; approved affinity parameters; labelled decision/outcome evaluation. These gaps do not block independent event-row counts or source-label recency.\n'
    (run_dir/'quality_findings.md').write_text(quality,encoding='utf-8')
    lineage='# Column lineage\n\n'
    lineage+='Counts and channel recencies reuse the immutable verified feature table. New journey/sequence logic operates directly on pre-snapshot raw events. Formatting is explicit Python-generated SQL, not per-customer Python inference. No original formula or original feature SQL is edited.\n\n'
    for item in selected_lineage():
        lineage+=f"## `{item['column']}`\n\nSource/expression: {item['source_expression']}.\n\nImplementation: `{item['implementation']}`.\n\n"
    lineage+='Exact reused source formulas and limitations are preserved in original_feature_definitions.json, copied read-only from the original dictionary. New types and their DBeaver order are recorded in execution.json.\n'
    (run_dir/'column_lineage.md').write_text(lineage,encoding='utf-8')
    queries='''-- Read-only queries, local database laya_testing.
SELECT count(*) rows FROM raw.gold_events;
SELECT count(*) rows FROM features.customer_features;
SELECT count(*) rows FROM features.laya_state_v1;
SELECT table_name,count(*) columns FROM information_schema.columns
WHERE table_schema='features' AND table_name IN('customer_features','laya_state_v1') GROUP BY 1;

SELECT * FROM features.customer_features ORDER BY context_id LIMIT 10;
SELECT * FROM features.laya_state_v1 ORDER BY context_id LIMIT 10;
SELECT state_json FROM features.laya_state_v1 ORDER BY context_id LIMIT 1;

-- Same local context, old and corrected journey representations.
SELECT s.context_id,f.latest_observed_journey_stage AS original_latest_stage,
 s.latest_observed_event_name,s.latest_non_campaign_event_name,
 s.latest_non_campaign_journey_stage,s.latest_non_campaign_journey_substage,
 s.hours_since_last_non_campaign_event,s.quality_flags
FROM features.laya_state_v1 s JOIN features.customer_features f USING(context_id)
ORDER BY s.context_id LIMIT 20;

SELECT context_id,recent_communication_events,recent_non_campaign_journey_events,
 aip_label_counts,aip_label_recency_hours,event_audit
FROM features.laya_state_v1 ORDER BY context_id LIMIT 10;

-- Exact local lookup: replace this placeholder only in your own DBeaver session.
-- SELECT DISTINCT context_id FROM raw.gold_events
-- WHERE application_id='<application-id>' AND event_datetime<timestamp '2026-05-01 00:00:00'
-- AND nullif(btrim(context_id),'') IS NOT NULL ORDER BY context_id;
-- More than one result means ambiguous; do not choose the latest context.

SELECT count(*) duplicate_exposed_contexts,
 sum((quality_flags->'exact_payload_duplicate_exposure'->>'excess_rows')::bigint) excess_rows
FROM features.laya_state_v1
WHERE (quality_flags->'exact_payload_duplicate_exposure'->>'excess_rows')::bigint>0;

SELECT context_id,jsonb_pretty(state_json) FROM features.laya_state_v1 ORDER BY context_id LIMIT 1;
'''
    (run_dir/'dbeaver_queries.sql').write_text(queries,encoding='utf-8')
