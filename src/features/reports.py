"""Generate aggregate-only documentation beside each reproducible feature build."""
import json
from pathlib import Path

from data.combine_parquets import ROOT

FAMILIES = [
('Send counts','Exact campaign_sent event-row counts, separately for SMS, WA and GRCS. These describe recorded contact, not delivery or unique messages.'),
('Open/read and click counts','Exact campaign_open_read and campaign_clicked rows per approved channel. Zero observed opens, especially for SMS, is not evidence that a message was unread.'),
('Communication recency','Snapshot minus latest qualifying send/engagement timestamp in hours. No qualifying observation gives NULL, never zero recency.'),
('Contact pressure','Trailing [snapshot-days,snapshot) send-row counts. Scalars cover 1, 7 and 30 days; contact_pressure_windows supports explicitly configured windows, each with channel and missing-channel breakdowns.'),
('Exposure-adjusted engagement','Matched rates, matched delay and affinity are NULL pending approved unique-message linkage and complete response windows. Explicit unmatched_open_event_row_ratio is only an event-count diagnostic, not a response probability.'),
('Latest observed stage/substage','The latest event with either recorded journey field supplies both fields together. Timestamp ties use source filename/row number for reproducibility; a conflict flag records ambiguity. This is not authoritative current stage.'),
('Application history','Distinct nonblank application IDs and per-application counts, products, partners and first/last observations. Previous-application count stays NULL without a current-application rule; application_id never replaces context_id.'),
('AIP-labelled history','AIP, aip ok, aip  ok and aip not ok are exact, separate keys. They are lexical source observations, never approved/success/failed classifications.'),
('AIP recency','Hours since each exact AIP label, plus most recent of the four labels. No observed label means no corresponding JSON key and no invented value.'),
('Journey inactivity and observed transitions','Journey-field inactivity is observable. Stage changes are counted only within applications associated with one nonmissing context and with no conflicting same-time states. Consecutive equal-time identical state observations collapse only for transitions. NULL means no reliable consecutive pair, not no progression.'),
('Customer activity','initiated_by labels remain literal counts. Customer-initiated recency stays NULL until classification is approved. Last observed event recency is computed separately.'),
('Sessions and visits','Count distinct recorded nonblank session IDs, missing-session rows and available visitnum rows/maximum. These are neither fabricated sessions nor an authoritative visit count.'),
('Recent sequence','Default latest eight events, chronologically presented, preserving exact timestamps, labels, channel, application/product/partner, journey fields and physical provenance. Length is configurable. Older rows remain in aggregates, not silently deleted.'),
('Active days and observed-event trend','Distinct calendar dates with events in 7/30-day windows, and recent minus prior counts over equal configurable windows. This measures observed-event activity, not verified user activity; left-edge coverage is flagged.'),
('Latest observed product/application','Independent latest nonblank identifiers and their timestamps. The last application is not assumed active, and independently last-known IDs need not describe the same event; recent_events retains row associations.'),
('Product history','Distinct product IDs, product-associated rows and per-product counts/timestamps. No inferred product meaning or current eligibility.'),
('Campaign exposure','All campaign_sent rows, distinct nonblank campaign names on sends, exact per-campaign counts and missing-campaign send rows. No assertion of delivery.'),
('Partner context','Distinct partners, associated/missing rows, per-partner observations and last known partner/time. Missing partner information is preserved rather than filled.')]

RELEVANCE=[
 'distinguish recorded channel exposure volumes',
 'provide separately observed open/read and click evidence without treating missing telemetry as disinterest',
 'describe how recently communication or recorded engagement occurred',
 'describe recent contact concentration for later policy evaluation',
 'eventually compare engagement under verified exposure; unavailable metrics prevent misleading affinity',
 'locate the latest recorded journey observation while retaining stage uncertainty',
 'distinguish observed application history and associations without guessing the active application',
 'retain AIP-labelled evidence until its business meaning is approved',
 'place AIP-labelled observations in historical time',
 'describe recorded inactivity and reliably ordered state changes without a progression score',
 'separate general event recency from unverified customer-initiated activity',
 'describe recorded session coverage and visit-field availability',
 'preserve the recent application-associated sequence that aggregate counts alone omit',
 'describe temporal concentration and changes in recorded event frequency',
 'supply last-known application/product references with explicit observation times',
 'describe the breadth and frequency of recorded product associations',
 'distinguish recorded campaign exposures while keeping delivery unknown',
 'describe observed partner associations and missingness']


def generate(run_dir: Path, execution: dict):
    run_dir.mkdir(parents=True,exist_ok=True)
    definitions=json.loads((ROOT/'sql/features/feature_definitions.json').read_text(encoding='utf-8'))
    if execution.get('consolidation_report'):
        audit=json.loads(Path(execution['consolidation_report']).read_text(encoding='utf-8'))
        quality=audit['quality']
        text='# Data quality and source audit\n\n'
        for source in audit['sources']:
            text+=f"- `{source['filename']}`: {source['rows']:,} rows; SHA256 before/after equal: {source['sha256_before']==source['sha256_after']}.\n"
        text+=f"\nCombined: {audit['combined_rows']:,} rows, exact source/provenance multiset and compatible types verified. Path: `{audit['consolidated_path']}`.\n\n"
        text+='Duplicate audit (all original 24 columns, NULL-safe exact grouping; provenance excluded from equality):\n\n```json\n'+json.dumps(audit['duplicate_audit'],indent=2)+'\n```\n\n'
        text+='Duplicates are retained. They can inflate event-row features compared with unique business/physical events. No cross-file duplicates were found in this build. No business deduplication rule is invented.\n\n'
        for label in ['channel_distribution','initiated_by_distribution','identifiers','application_relationships','timestamp_ties','message_matching_audit','timestamps']:
            text+='## '+label+'\n\n```json\n'+json.dumps(quality[label],indent=2)+'\n```\n\n'
        text+='## Schema and missingness\n\n| Source field | DuckDB type | NULL rows | Blank string rows |\n|---|---|---:|---:|\n'
        for column in audit['schema']:
            name=column['name']
            text+=f"| {name} | {column['duckdb_type']} | {quality['nulls'][name]} | {quality['blanks'].get(name,'not text')} |\n"
        text+='\nNo NULL channel values were observed; blank channel rows are missing information. Only SMS, WA, GRCS are output channel codes. initiated_by user/system are not authoritatively classified. AIP labels are preserved exactly; their outcomes are not reinterpreted. The source has no verified customer_id mapping, event-arrival timestamp or message-level response contract.\n\n'
        if audit.get('recovered_from'):
            text+='Audit recovery: a completed duplicate/value audit encountered a final profiling alias syntax error. The failed report is preserved at `'+audit['recovered_from']+'`. A fresh whole-file exact comparison hit the 2 GB DuckDB memory bound, so successful recovery used exact 100,000-row provenance partitions under unchanged source hashes. No rows were dropped or modified.\n\n'
        text+='Event/journey distributions and full reproducible audit: `'+str(execution['consolidation_report'])+'`. Read it locally; aggregate labels do not establish business semantics.\n'
        (run_dir/'data_quality.md').write_text(text,encoding='utf-8')
        matching='# Matched-response and affinity availability\n\n'+quality['message_matching_status']+'\n\n'
        matching+='Candidate (context_id, channel, channel_id) key audit:\n\n```json\n'+json.dumps(quality['message_matching_audit'],indent=2)+'\n```\n\n'
        matching+='Multiple sends per candidate key rule out assuming a unique send/open join. Even a key with one observed send is not an approved message identifier. No nearest-earlier-send heuristic is used. A complete approved response window, telemetry availability and deduplication/unique-response rules are also missing. All per-channel matched rates, median matched delay and computed affinity therefore remain SQL NULL. The reusable formulas are tested using explicitly synthetic matched examples only.\n\n'
        matching+='Needed before enabling affinity: authoritative exposure/message ID and open-attribution contract; eligibility/censoring windows; channel-specific open telemetry; duplicate/repeated-open rules; approved opening weight (>0.5), decay half-life, prior score and prior strength. SMS open absence must not be interpreted as unread messages. Channel cost is excluded and remains an external decision tie-breaker.\n'
        (run_dir/'matching_contract.md').write_text(matching,encoding='utf-8')
    text='# The 18 historical feature families\n\nGrouping key: raw context_id, not a verified customer_id. Snapshot is exclusive and timezone-naive. No inference, training, actions, tariffs or artificial scores are included.\n\n'
    for i,(name,description) in enumerate(FAMILIES,1):
        names=[x['column_name'] for x in definitions if x['feature_family']==i]
        text+=f'## {i}. {name}\n\n{description}\n\nColumns: '+', '.join('`'+n+'`' for n in names)+'.\n\nWhy it may help Laya: '+RELEVANCE[i-1]+'. Relevance requires outcome evaluation and does not establish decision quality or business lift.\n\n'
    (run_dir/'feature_explanations.md').write_text(text,encoding='utf-8')
    formulas='''# Formula explanations

No affinity weights, decay half-life or smoothing priors have been approved. The build does not run these formulas on unmatched event rows and publishes SQL NULL for matched metrics/affinity. All numbers below are explicitly illustrative arithmetic, not customer observations or production defaults.

## Historical SQL arithmetic

- Event count: sum one for each matching historical row, including source duplicates. Example: three SMS campaign_sent rows give 3, not three verified deliveries. Missing channel rows count separately.
- Recency: (exclusive snapshot - last qualifying timestamp) / 3600 seconds. Example: midnight May 1 minus noon April 30 is 12 hours. No timestamp gives NULL. Historical filtering prevents negative recency.
- Trailing-window count: include t when snapshot-d days <= t < snapshot. Example: two send rows in a seven-day window give 2; events exactly at snapshot are excluded. Coverage flags do not prove complete telemetry.
- Unmatched event-row ratio: open/read row count / send row count. Example: 6 opens / 3 sends = 2. This can exceed one and is deliberately not a matched unique-message rate. Zero denominator gives NULL.
- Active days: count distinct dates with recorded events within the window. Three rows on one date and one on another give 2 dates, not four active user days.
- Equal-window trend: recent observed rows minus prior observed rows. Example: 12 - 8 = +4 over two equal seven-day windows. No progression, interest or business score is inferred; incomplete coverage limits comparisons.
- Observed stage changes: count adjacent, distinct stage/substage pairs only within reliably associated/ordered applications. A,A,B gives 1. Conflicting timestamp ties exclude that application; no reliable pair gives NULL.

## Verified message-level Python formulas

Implementation: src/features/formulas.py. Every gate and parameter is explicit.

1. Matched time to open: hours(open timestamp - send timestamp), only for a previously verified message match. 13:00 - 11:00 = 2 hours. Missing endpoints or unverified linkage give None; negative delay raises an error. No nearest-earlier-send matching occurs.
2. Channel median delay: median of valid matched delays. [1,3,8] hours gives 3 hours. Empty observations give None; negative/nonfinite values are rejected. This describes speed only among observed verified openers.
3. Matched-open rate r: unique opened messages / eligible unique sends in a complete approved observation window. 2 / 4 = 0.5. Incomplete linkage/window or zero sends gives None. Opened count must not exceed sends. Complete no-opens is a valid zero only after telemetry is confirmed.
4. Delay decay d(t): 2 ** (-t / half_life_hours). For an illustrative 24-hour half-life, delays 0,24,48 give 1,0.5,0.25. Longer delays contribute less. Half-life must be supplied and positive; it is not inferred from channels.
5. Opening-weighted raw affinity a: r * [w + (1-w) * mean(d(t))]. w must be >0.5 and <=1, so opening is primary and timeliness secondary. Exactly one verified delay per unique opened message is required. No opens gives a=0 before smoothing. No eligible sends or missing contracts gives None. No costs enter the score.
6. Sparse-observation smoothing s: (n*a + k*p)/(n+k), where n is eligible-send count, p an explicitly approved prior score, and k its explicit nonnegative observation strength. With increasing n the observed value contributes more. Zero observations produces None, not an invented prior-only customer measurement.

## Worked derivation (illustrative inputs, never used to populate this build)

Four eligible verified sends; two distinct opens at delays 0 and 24 hours. Choose illustrative w=0.75, half-life=24 hours, prior p=0.2 and strength k=2 solely to demonstrate arithmetic:

- Opening r = 2/4 = 0.5.
- Decays = 1 and 0.5; mean = 0.75.
- Opening contribution = 0.5*0.75 = 0.375.
- Secondary timeliness contribution = 0.5*0.25*0.75 = 0.09375.
- Raw affinity = 0.46875.
- Smoothed affinity = (4*0.46875 + 2*0.2)/(4+2) = 0.3791666667.

Keeping all other inputs fixed and increasing opening delay reduces the timeliness contribution. Sparse smoothing can move the score toward its approved prior and is not additional observed behavior. A channel-level score might eventually summarize historical engagement for Laya, but none of these formulas proves business effectiveness, incrementality, causal lift or calibrated outcome probability. An outcome-labelled evaluation and approved contracts remain necessary.
'''
    (run_dir/'formula_explanations.md').write_text(formulas,encoding='utf-8')
    dictionary='# Feature dictionary\n\nThe PostgreSQL dictionary additionally records actual SQL types from information_schema. Each generated column is documented below.\n\n'
    for item in definitions:
        dictionary+=f"## `{item['column_name']}`\n\n{item['meaning']}.\n\nSource: {', '.join(item['source_fields']) or 'explicit availability metadata'}.\n\nAggregation: `{item['formula']}`.\n\nImplementation: `{item['implementation']}`. Status: {item['observability']}.\n\nLimitation: {item['limitations']}\n\nLaya relevance: {item['relevance_to_laya']}\n\n"
    (run_dir/'feature_dictionary.md').write_text(dictionary,encoding='utf-8')
    summary='# Execution results\n\n'+json.dumps(execution,indent=2,default=str)+'\n\n'
    summary+='Local client endpoint only: 127.0.0.1:5432. The initial setup audit in reports/feature_build/20261029_build/local_network_audit.json found server listen_addresses=* with HBA rules allowing only loopback TCP addresses. No server configuration or administrator password was changed. Later server configuration changes are not assumed absent on future reruns. Explicit application-role grants are limited to laya_testing; existing cluster PUBLIC privileges were not changed. Credentials reside only in gitignored local configuration.\n\n'
    summary+='Transport validation: the initial CSV ingestion was rejected after exact verification detected normalized CR/LF in utm_campaign. Original Parquet files were unchanged. The replacement ingestion uses transient binary files with client-side binary COPY, tested live for CR/LF, Unicode, NULLs, empty strings, integers, floats and timestamps. Verification base64-encodes text during CSV export to protect it from Windows text-mode translation, then compares decoded exact values. Failed/interrupted attempts remain separate reports. A 120-second count-query timeout was recorded as failure; later large verification queries use a bounded 900-second client deadline. No cause is inferred for that timeout. Raw reuse always requires complete independent value verification.\n\n'
    summary+='Unresolved contracts: context-to-customer identity; source timezone; event availability/arrival timestamps; deduplication/business event identity; send/open unique-message linkage; complete response windows and telemetry; current application; initiated_by classification; visitnum meaning; affinity weights, half-life and smoothing prior. These limitations do not invalidate independently observed event-row features.\n\n'
    summary+='Reruns preserve prior managed feature tables only with explicit --archive-existing. Consolidated inputs and raw tables are verified before explicit reuse. No unrelated tables are replaced. See reproduction.md and dbeaver_queries.sql.\n'
    (run_dir/'execution_summary.md').write_text(summary,encoding='utf-8')
    consolidation=execution.get('consolidation_report','<verified-consolidation-report>')
    reproduction=f'''# Reproduction in PowerShell

```powershell
Set-Location C:\\Aryan\\Attributics\\Laya_POC
$env:LAYA_TEST_POSTGRES = '1'
.\\.venv\\Scripts\\python.exe -m unittest discover -s tests -v
# Safe repeat: revalidate the existing audited source/raw table and preserve old features.
.\\.venv\\Scripts\\python.exe .\\src\\run_feature_pipeline.py --consolidation-report "{consolidation}" --reuse-raw --archive-existing
```

On a fresh workspace without a consolidated output or pipeline tables:

```powershell
.\\.venv\\Scripts\\python.exe .\\src\\run_feature_pipeline.py --snapshot "2026-05-01 00:00:00" --sequence-length 8 --trend-days 7 --pressure-windows 1 7 30
```

No activation or new package install is required. The installed PostgreSQL psql client is discovered from PATH or Program Files. The app-specific .env.postgres.app takes precedence over .env.postgres; process environment PG variables take precedence over either. Configure existing app credentials locally; never commit them. The app role needs CONNECT/CREATE/TEMPORARY on laya_testing and ownership of its raw/features schemas. Administrative setup is deliberately not automated into reruns.

Granular commands: src/data/combine_parquets.py --report-dir <fresh-report-directory>; src/db/load_gold_events.py --consolidation-report <audit.json> --report-dir <report-directory> [--reuse]; src/features/build_customer_features.py --report-dir <report-directory> [--archive-existing]. Existing files/tables are never silently overwritten. Existing partial files must be reviewed before retrying.
'''
    (run_dir/'reproduction.md').write_text(reproduction,encoding='utf-8')
    (run_dir/'dbeaver_queries.sql').write_text((ROOT/'sql/inspect_features.sql').read_text(encoding='utf-8'),encoding='utf-8')
