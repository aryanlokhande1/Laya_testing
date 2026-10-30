"""Fixed historical cohort and four representations. No changes to existing runners."""
from collections import Counter
import copy
import hashlib
import json

from real_data import CUTOFF, CAMPAIGN_EVENTS, build_real_state, rows

STRATEGIES = ('A', 'B', 'C', 'D')
QUESTION = (
    'For a DEMO ONLY historical ranking, select one supplied channel/theme using the observable historical '
    'evidence supplied in state. The synthetic catalog is not an authoritative action_space; eligibility '
    'and business suitability are unknown. Email was not observed in the source files. '
    'Do not choose SEND/WAIT, timing or budget, or infer PTP or payment outcomes.')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()


def select_cohort(connection, count=10):
    """Stratified candidate pools, then deterministic greedy coverage, historical data only."""
    connection.execute('''CREATE OR REPLACE TEMP VIEW bench_all AS
        SELECT *, 'march' AS source_month FROM march
        UNION ALL SELECT *, 'april' AS source_month FROM april''')
    candidates = []
    pool_counts = {}
    for scope in ('linked', 'april'):
        # April-only checks deliberately use only April; no unvalidated customer linkage.
        scope_filter = 'TRUE' if scope == 'linked' else "source_month='april'"
        linked_filter = '''AND month_count=2 AND context_id IN (SELECT context_id FROM shared)''' if scope == 'linked' else ''
        sql = f'''WITH h AS (SELECT * FROM bench_all WHERE event_datetime < CAST(? AS TIMESTAMP_NS) AND {scope_filter}),
            qa AS (SELECT application_id,count(DISTINCT context_id) AS contexts,
                count(DISTINCT nullif(product_id,'')) AS products FROM h
                WHERE nullif(application_id,'') IS NOT NULL GROUP BY application_id),
            bad AS (SELECT DISTINCT context_id FROM h JOIN qa USING(application_id) WHERE contexts<>1 OR products<>1),
            shared AS (SELECT context_id,application_id FROM h WHERE nullif(application_id,'') IS NOT NULL
                GROUP BY 1,2 HAVING count(DISTINCT source_month)=2),
            stats AS (SELECT context_id,count(*) AS event_rows,
                count(DISTINCT nullif(application_id,'')) AS applications,
                count(DISTINCT source_month) AS month_count,
                string_agg(DISTINCT nullif(channel,''),',' ORDER BY nullif(channel,'')) AS channels,
                count(*) FILTER (WHERE event_name IN ('campaign_clicked','campaign_open_read')) AS engagement,
                count(*) FILTER (WHERE event_name IN ('AIP','aip  ok','aip ok','aip not ok')) AS aip,
                count(DISTINCT event_name) AS labels FROM h
                WHERE context_id IS NOT NULL AND context_id<>'' GROUP BY context_id),
            eligible AS (SELECT *,CASE WHEN event_rows<16 THEN '8-15' WHEN event_rows<32 THEN '16-31'
                WHEN event_rows<64 THEN '32-63' ELSE '64-160' END AS row_band FROM stats
                WHERE event_rows BETWEEN 8 AND 160 AND labels>=2
                AND context_id NOT IN (SELECT context_id FROM bad) {linked_filter})
            SELECT * FROM eligible QUALIFY row_number() OVER (
                PARTITION BY row_band,channels,applications>1,applications=0,engagement>0,aip>0
                ORDER BY md5(context_id)) <= 4 ORDER BY md5(context_id)'''
        pool = rows(connection, sql, [CUTOFF])
        pool_counts[scope] = len(pool)
        for candidate in pool:
            candidate['scope'] = scope
            candidates.append(candidate)
    chosen, used_ids, covered = [], set(), set()
    while len(chosen) < count:
        best = None
        for item in candidates:
            if item['context_id'] in used_ids:
                continue
            features = {f"rows:{item['row_band']}", f"scope:{item['scope']}",
                        'applications:multiple' if item['applications']>1 else 'applications:single' if item['applications']==1 else 'applications:none',
                        f"engagement:{item['engagement']>0}", f"aip:{item['aip']>0}"}
            features.update('channel:'+channel for channel in (item['channels'] or '').split(',') if channel)
            gain = sum(3 if feature in {'scope:linked','applications:multiple'} else 1 for feature in features-covered)
            score = (-gain, 0 if item['scope']=='linked' else 1, digest(item['context_id']))
            if best is None or score < best[0]:
                best = (score, item, features)
        if best is None:
            raise ValueError('Fewer than ten distinct contexts satisfy documented identity/selection checks')
        _, item, features = best
        chosen.append(item)
        used_ids.add(item['context_id'])
        covered.update(features)
    selected = []
    for index, item in enumerate(chosen, 1):
        scope_filter = '' if item['scope']=='linked' else "AND source_month='april'"
        schema = rows(connection, 'DESCRIBE bench_all')
        projection = ','.join('cast(event_datetime AS VARCHAR) AS event_datetime' if col['column_name']=='event_datetime'
                              else f'"{col["column_name"]}"' for col in schema)
        history = rows(connection, f'''SELECT {projection} FROM bench_all WHERE context_id=?
            AND event_datetime < CAST(? AS TIMESTAMP_NS) {scope_filter}
            ORDER BY event_datetime,event_name,journey_stage,journey_substage,channel,partner_id,
            product_id,application_id,session_id,source_month''', [item['context_id'], CUTOFF])
        if len(history) != item['event_rows']:
            raise ValueError('Selection count differs from retrieved historical rows')
        state, aliases = build_real_state(history, link_months=item['scope']=='linked')
        selected.append({'alias': f'CTX{index:02d}', 'candidate': item,
                         'history': history, 'legacy_state': state, 'aliases': aliases})
    return selected, {'stratified_pool_sizes': pool_counts, 'covered_features': sorted(covered),
                      'rule': '8..160 historical rows, >=2 event labels, known apps unique context and one known product within scope; linked additionally shared app and both months. Four per feature stratum then deterministic greedy feature coverage; distinct contexts only.'}


def recent_records(history, aliases, limit):
    inverse = {kind: {raw: alias for alias,raw in mapping.items()} for kind,mapping in aliases.items()}
    return [{'at': row['event_datetime'], 'event': row['event_name'], 'stage': row['journey_stage'],
             'substage': row['journey_substage'], 'channel': row.get('channel'),
             'application': inverse['applications'].get(row.get('application_id')),
             'product': inverse['products'].get(row.get('product_id')),
             'partner': inverse['partners'].get(row.get('partner_id'))} for row in history[-limit:]]


def event_statement(row):
    # Match existing Qwen event_sentences; partner remains available in structured features only.
    return (f"{row['at']} | event={row['event']} | stage={row['stage']} | substage={row['substage']}"
            f" | channel={row['channel']} | application={row['application']} | product={row['product']}")


def seconds_to_cutoff(connection, at):
    return connection.execute('SELECT epoch_ns(CAST(? AS TIMESTAMP_NS))-epoch_ns(CAST(? AS TIMESTAMP_NS))',
                              [CUTOFF, at]).fetchone()[0] / 1_000_000_000


def structured_features(item, connection):
    history = item['history']
    legacy = item['legacy_state']
    application_features = {}
    for application, sequence in legacy['application_sequences'].items():
        last_at = sequence[-1][0]
        application_features[application] = {
            'rows': len(sequence), 'event_counts': dict(sorted(Counter(row[1] for row in sequence).items())),
            'latest_observed_journey': [row[2:4] for row in sequence if row[0]==last_at],
            'last_event_age_seconds': seconds_to_cutoff(connection,last_at),
            'products': sorted({row[4] for row in sequence if row[4]}),
            'partners': sorted({row[5] for row in sequence if row[5]})}
    channels = {}
    for channel, values in legacy['channel_observations'].items():
        subset = [row for row in history if (row.get('channel') or '(missing)')==channel]
        channels[channel] = {**values, 'last_event_age_seconds': seconds_to_cutoff(connection,max(row['event_datetime'] for row in subset))}
        for label,short in [('campaign_sent','send'),('campaign_open_read','open_read'),('campaign_clicked','click')]:
            times = [row['event_datetime'] for row in subset if row['event_name']==label]
            channels[channel][f'last_{short}_age_seconds'] = seconds_to_cutoff(connection,max(times)) if times else None
    return {'cutoff_exclusive': CUTOFF, 'timestamp_convention': 'file naive; seconds before cutoff',
            'scope': item['candidate']['scope'], 'identity': 'simulation customer-key assumption; source contract unverified',
            'observed_rows': len(history), 'event_counts': copy.deepcopy(legacy['event_counts']),
            'first_event_age_seconds': seconds_to_cutoff(connection,history[0]['event_datetime']),
            'last_event_age_seconds': seconds_to_cutoff(connection,history[-1]['event_datetime']),
            'channel_observations': channels, 'application_features': application_features,
            'application_unassigned_rows': legacy['application_unassigned_rows'],
            'partner_history': copy.deepcopy(legacy['partner_history']),
            'demo_only': True, 'action_space_authoritative': False,
            'limitations': 'Raw event/channel labels; no inferred preference, DPD, PTP, balance or conversion. Email unobserved. No execution.'}


def strategy_state(common, recent20, strategy, summary=None):
    if strategy not in STRATEGIES:
        raise ValueError('Unknown strategy')
    state = {'structured': copy.deepcopy(common)}
    if strategy in ('B','D'):
        limit = 8 if strategy=='B' else 20
        selected = recent20[-limit:]
        state['chronological_events'] = [event_statement(row) for row in selected]
        state['sequence_coverage'] = {'limit': limit, 'included': len(selected),
                                     'older_rows_not_in_sequence': common['observed_rows']-len(selected),
                                     'equal_time_order': 'deterministic, not causal'}
    elif strategy=='C':
        if summary is None:
            raise ValueError('Validated Qwen summary required; no structured-only fallback')
        from real_data import validate_event_summary
        sentences = [event_statement(row) for row in recent20[-8:]]
        summary = validate_event_summary(json.dumps(summary),sentences)
        state['grounded_event_summary'] = copy.deepcopy(summary)
        state['summary_coverage'] = {'source_recent_events':len(sentences), 'selected_events':len(summary['highlights']),
                                     'older_rows_outside_qwen_input':common['observed_rows']-len(sentences)}
    return state


def laya_request(state, reverse=False):
    from laya_client import ACTION_CATALOG
    items = list(ACTION_CATALOG.items())
    if reverse:
        items.reverse()
    return {'model':'laya','state':copy.deepcopy(state),'questions':{'action':{
        'type':'choice','instructions':QUESTION,
        'criteria':{key:f"{value['channel']}: {value['theme']}" for key,value in items}}}}


def check_request_size(payload, maximum_bytes=65536):
    size = len(json.dumps(payload,ensure_ascii=False).encode('utf-8'))
    if size>maximum_bytes:
        raise ValueError('Endpoint request byte limit exceeded; no truncation')
    return size


def token_flags(tokens, budget, context_window):
    if type(tokens) is not int or tokens<1:
        raise ValueError('Actual input token count missing or invalid')
    if context_window and tokens>context_window:
        raise ValueError('Actual input tokens exceed configured model context')
    return tokens>budget
