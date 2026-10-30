"""Read-only Parquet profiling and historical selection without pandas."""
from collections import Counter
import hashlib
import json
from pathlib import Path

import duckdb

FILES = {'march': 'events_gold_1_bucket_000_20260331.parquet',
         'april': 'events_gold_2_bucket_000_20260430.parquet'}
CUTOFF = '2026-04-15 00:00:00'
CAMPAIGN_EVENTS = {'campaign_sent', 'campaign_open_read', 'campaign_clicked'}
RECENT_LIMIT = 8


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def rows(connection, sql, parameters=None):
    cursor = connection.execute(sql, parameters or [])
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def connect_inputs(input_dir):
    connection = duckdb.connect()
    connection.execute("SET memory_limit='1GB'")
    connection.execute('SET threads=4')
    for month, name in FILES.items():
        connection.read_parquet(str(Path(input_dir) / name)).create_view(month)
        schema = rows(connection, f'DESCRIBE {month}')
        types = {column['column_name']: column['column_type'] for column in schema}
        if types.get('event_datetime') != 'TIMESTAMP_NS':
            connection.close()
            raise ValueError('Expected timezone-naive TIMESTAMP_NS; review timestamp convention before running')
    return connection


def profile(connection):
    report = {'timestamp_convention': 'TIMESTAMP_NS without timezone; no timezone conversion applied',
              'cutoff': CUTOFF, 'context_identity_verified': False, 'months': {}}
    for month in FILES:
        schema = rows(connection, f'DESCRIBE {month}')
        names = [column['column_name'] for column in schema]
        nulls = ','.join(f'count(*) FILTER (WHERE "{name}" IS NULL) AS "{name}"' for name in names)
        blanks = ','.join(f'count(*) FILTER (WHERE trim("{column["column_name"]}") = \'\') AS "{column["column_name"]}"'
                          for column in schema if column['column_type'] == 'VARCHAR')
        basic = rows(connection, f'''SELECT count(*) AS rows, count(DISTINCT context_id) AS contexts,
            cast(min(event_datetime) AS VARCHAR) AS earliest, cast(max(event_datetime) AS VARCHAR) AS latest,
            count(*) FILTER (WHERE event_datetime < CAST(? AS TIMESTAMP_NS)) AS before_cutoff,
            count(*) FILTER (WHERE event_datetime >= CAST(? AS TIMESTAMP_NS)) AS at_or_after_cutoff
            FROM {month}''', [CUTOFF, CUTOFF])[0]
        distributions = {}
        for column in ('event_name', 'journey_stage', 'journey_substage', 'channel', 'initiated_by'):
            distributions[column] = rows(connection, f'''SELECT "{column}" AS value, count(*) AS count
                FROM {month} GROUP BY 1 ORDER BY count DESC, value''')
        relationship = rows(connection, f'''WITH per_context AS (
            SELECT context_id, count(DISTINCT nullif(application_id,'')) AS applications,
            count(DISTINCT nullif(session_id,'')) AS sessions FROM {month}
            WHERE context_id IS NOT NULL AND context_id <> '' GROUP BY context_id)
            SELECT count(*) AS contexts, count(*) FILTER (WHERE applications=0) AS no_application,
            count(*) FILTER (WHERE applications=1) AS one_application,
            count(*) FILTER (WHERE applications>1) AS multiple_applications,
            count(*) FILTER (WHERE sessions>1) AS multiple_sessions FROM per_context''')[0]
        inverse = rows(connection, f'''WITH per_application AS (
            SELECT application_id, count(DISTINCT nullif(context_id,'')) AS contexts FROM {month}
            WHERE application_id IS NOT NULL AND application_id <> '' GROUP BY application_id)
            SELECT count(*) AS applications, count(*) FILTER (WHERE contexts>1) AS multiple_contexts
            FROM per_application''')[0]
        event_labels = rows(connection, f'''SELECT event_name, journey_stage, journey_substage, count(*) AS count
            FROM {month} WHERE event_name IN ('campaign_sent','campaign_open_read','campaign_clicked',
            'AIP','aip  ok','aip ok','aip not ok','Disbursed','Rejected','bank approved',
            'bank approval rejected','bank approval pending','Sent to Bank','payment started','journeycomplete')
            GROUP BY 1,2,3 ORDER BY 1,4 DESC''')
        report['months'][month] = {**basic, 'schema': schema, 'nulls': rows(connection, f'SELECT {nulls} FROM {month}')[0],
                                  'blank_strings': rows(connection, f'SELECT {blanks} FROM {month}')[0],
                                  'distributions': distributions, 'context_application_relationship': relationship,
                                  'application_context_relationship': inverse, 'event_label_evidence': event_labels}
    report['overlap'] = rows(connection, '''WITH m AS (SELECT DISTINCT context_id FROM march
        WHERE context_id IS NOT NULL AND context_id <> ''), a AS (SELECT DISTINCT context_id FROM april
        WHERE context_id IS NOT NULL AND context_id <> '')
        SELECT (SELECT count(*) FROM m JOIN a USING(context_id)) AS shared_context_values,
        (SELECT count(*) FROM m ANTI JOIN a USING(context_id)) AS march_only,
        (SELECT count(*) FROM a ANTI JOIN m USING(context_id)) AS april_only''')[0]
    report['overlap']['shared_context_same_nonblank_application'] = connection.execute('''SELECT count(DISTINCT m.context_id)
        FROM (SELECT DISTINCT context_id,application_id FROM march WHERE application_id IS NOT NULL AND application_id <> '') m
        JOIN (SELECT DISTINCT context_id,application_id FROM april WHERE application_id IS NOT NULL AND application_id <> '') a
        ON m.context_id=a.context_id AND m.application_id=a.application_id''').fetchone()[0]
    report['semantics'] = {
        'campaign_sent': 'Observed send-labelled event, not proof of delivery',
        'campaign_open_read': 'Observed open/read-labelled engagement, exact channel semantics unconfirmed',
        'campaign_clicked': 'Observed click-labelled engagement, attribution/deduplication unconfirmed',
        'AIP': 'Literal AIP label; acronym, scope and status semantics require data dictionary',
        'aip  ok / aip ok / aip not ok': 'Different raw labels; reported separately; observed AIP Approved/Declined substages only',
        'Disbursed': 'Disbursement-labelled event; candidate conversion, not an approved evaluation target',
        'bank approved / Rejected / Sent to Bank / payment started': 'Literal outcome/progress labels; not inferred PTP or repayment',
        'other_events': 'No unapproved business outcome mapping',
        'context_id': 'Equality overlap and application/session cardinalities do not establish customer identity or stable semantics',
        'channels': 'Raw values retained; GRCS and WA meanings and eligibility require confirmation',
    }
    report['relationship_validation'] = relationship_profile(connection)
    report['identity_assumption'] = 'context_id is candidate persistent customer key under established simulation design; application_id is application key; source contract not independently verified'
    return report


def relationship_profile(connection):
    result = {}
    for label, where, parameters in (
            ('full_months', 'TRUE', []),
            ('strictly_before_cutoff', 'event_datetime < CAST(? AS TIMESTAMP_NS)', [CUTOFF])):
        result[label] = rows(connection, f'''WITH h AS (
            SELECT context_id,application_id,product_id,'march' AS source_month,event_datetime FROM march
            UNION ALL SELECT context_id,application_id,product_id,'april',event_datetime FROM april),
            qa AS (SELECT application_id, count(DISTINCT context_id) AS contexts,
                count(DISTINCT nullif(product_id,'')) AS products,
                count(DISTINCT source_month) AS month_count,
                count(DISTINCT nullif(product_id,'')) FILTER (WHERE source_month='march') AS march_products,
                count(DISTINCT nullif(product_id,'')) FILTER (WHERE source_month='april') AS april_products FROM h
                WHERE {where} AND nullif(application_id,'') IS NOT NULL GROUP BY application_id)
            SELECT count(*) AS applications,
                count(*) FILTER (WHERE contexts>1) AS multiple_contexts,
                count(*) FILTER (WHERE products>1) AS multiple_products,
                count(*) FILTER (WHERE month_count=2) AS shared_applications,
                count(*) FILTER (WHERE month_count=2 AND contexts=1 AND products=1) AS shared_unique_context_product,
                count(*) FILTER (WHERE month_count=2 AND contexts>1) AS shared_multiple_contexts,
                count(*) FILTER (WHERE month_count=2 AND products>1) AS shared_multiple_products,
                count(*) FILTER (WHERE month_count=2 AND march_products=1 AND april_products=1 AND products>1) AS shared_single_product_per_month_but_changed
            FROM qa''', parameters)[0]
    result['interpretation'] = 'Multiple applications per context are permitted. Multiple products may be legitimate; meaning is unconfirmed, so strict selected linkage uses only single-known-product apps. Selected agreement is not a universal identity guarantee. Blank/NULL application/product values are unknown.'
    return result


def select_history(connection, cutoff=CUTOFF, link_months=False):
    """Selection depends only on pre-cutoff observations; default is April alone."""
    source = '(SELECT *, \'april\' AS source_month FROM april)'
    if link_months:
        source = "(SELECT *, 'march' AS source_month FROM march UNION ALL SELECT *, 'april' AS source_month FROM april)"
    connection.execute(f'CREATE OR REPLACE TEMP VIEW selection_source AS SELECT * FROM {source}')
    linkage_filter = ''
    linkage_ctes = ''
    if link_months:
        linkage_ctes = ''', app_quality AS (
            SELECT application_id, count(DISTINCT context_id) AS contexts,
            count(DISTINCT nullif(product_id,'')) AS products FROM h
            WHERE nullif(application_id,'') IS NOT NULL GROUP BY application_id),
            shared AS (SELECT context_id,application_id FROM h WHERE nullif(application_id,'') IS NOT NULL
                GROUP BY 1,2 HAVING count(DISTINCT source_month)=2),
            bad_context AS (SELECT DISTINCT context_id FROM h JOIN app_quality USING(application_id)
                WHERE contexts<>1 OR products<>1)'''
        linkage_filter = '''AND count(DISTINCT source_month)=2
            AND context_id IN (SELECT context_id FROM shared)
            AND context_id NOT IN (SELECT context_id FROM bad_context)'''
    candidate = connection.execute(f'''WITH h AS (SELECT * FROM selection_source
        WHERE event_datetime < CAST(? AS TIMESTAMP_NS)) {linkage_ctes}
        SELECT context_id FROM h
        WHERE context_id IS NOT NULL AND context_id <> ''
        GROUP BY context_id HAVING count(*) BETWEEN 12 AND 40 AND count(DISTINCT event_name)>=5
        AND count(DISTINCT nullif(channel,''))>=2
        AND count(*) FILTER (WHERE event_name='campaign_sent')>0
        AND count(*) FILTER (WHERE event_name IN ('campaign_open_read','campaign_clicked'))>0
        AND count(*) FILTER (WHERE event_name NOT IN ('campaign_sent','campaign_open_read','campaign_clicked'))>0
        {linkage_filter}
        ORDER BY count(DISTINCT nullif(application_id,''))>1 DESC, md5(context_id) LIMIT 1''', [cutoff]).fetchone()
    if candidate is None:
        raise ValueError('No context meets the explicit historical selection rule; no automatic relaxation')
    context_id = candidate[0]
    schema = rows(connection, 'DESCRIBE selection_source')
    projection = ','.join('cast(event_datetime AS VARCHAR) AS event_datetime' if col['column_name'] == 'event_datetime'
                          else f'"{col["column_name"]}"' for col in schema)
    history = rows(connection, f'''SELECT {projection} FROM selection_source
        WHERE context_id=? AND event_datetime < CAST(? AS TIMESTAMP_NS)
        ORDER BY event_datetime, event_name, journey_stage, journey_substage, channel, partner_id,
        product_id, application_id, session_id, source_month''', [context_id, cutoff])
    return context_id, history


def build_real_state(history, cutoff=CUTOFF, link_months=False):
    if not history:
        raise ValueError('Historical state requires events')
    # Compare with DuckDB to retain nanosecond precision and the files' naive convention.
    with duckdb.connect() as check:
        for row in history:
            valid = check.execute('SELECT CAST(? AS TIMESTAMP_NS) < CAST(? AS TIMESTAMP_NS)',
                                  [row['event_datetime'], cutoff]).fetchone()[0]
            if not valid:
                raise ValueError('At/after-cutoff or null event rejected')
    if len({row['context_id'] for row in history}) != 1:
        raise ValueError('Mixed contexts in selected history')
    if not link_months and any(row['source_month'] != 'april' for row in history):
        raise ValueError('Cross-month linkage has not been authorized')
    history = sorted(history, key=lambda r: (r['event_datetime'], r.get('event_name') or '',
                                           r.get('journey_stage') or '', r.get('journey_substage') or '',
                                           r.get('channel') or '', r.get('partner_id') or '',
                                           r.get('product_id') or '', r.get('application_id') or '',
                                           r.get('session_id') or '', r['source_month']))
    partners = {}
    applications = {}
    products = {}
    application_sequences = {}
    for row in history:
        application = row.get('application_id')
        product = row.get('product_id')
        if application:
            applications.setdefault(application, f'A{len(applications)+1}')
        if product:
            products.setdefault(product, f'PR{len(products)+1}')
        partner = row.get('partner_id')
        if partner:
            partners.setdefault(partner, f'P{len(partners)+1}')
    for row in history:
        application = applications.get(row.get('application_id'))
        if application:
            application_sequences.setdefault(application, []).append([
                row['event_datetime'], row['event_name'], row.get('journey_stage'), row.get('journey_substage'),
                products.get(row.get('product_id')), partners.get(row.get('partner_id'))])
    partner_history = {alias: {'first_at': min(row['event_datetime'] for row in history if row.get('partner_id') == raw),
                               'last_at': max(row['event_datetime'] for row in history if row.get('partner_id') == raw),
                               'rows': sum(row.get('partner_id') == raw for row in history)} for raw,alias in partners.items()}
    per_channel = {}
    for channel in sorted({row.get('channel') or '(missing)' for row in history}):
        selected = [row for row in history if (row.get('channel') or '(missing)') == channel]
        per_channel[channel] = {'events': len(selected), **{name: sum(row['event_name'] == name for row in selected)
                                                          for name in sorted(CAMPAIGN_EVENTS)}}
    recent = [{'at': row['event_datetime'], 'event': row['event_name'], 'stage': row.get('journey_stage'),
               'substage': row.get('journey_substage'), 'channel': row.get('channel'),
               'partner': partners.get(row.get('partner_id')), 'application': applications.get(row.get('application_id')),
               'product': products.get(row.get('product_id'))} for row in history[-RECENT_LIMIT:]]
    journey = [row for row in history if row['event_name'] not in CAMPAIGN_EVENTS]
    latest = max((row['event_datetime'] for row in journey), default=None)
    latest_journey = [dict(at=row['event_datetime'], stage=row.get('journey_stage'), substage=row.get('journey_substage'),
                           application=applications.get(row.get('application_id')))
                      for row in journey if row['event_datetime'] == latest]
    state = {
        'entity_scope': 'context_id: candidate customer key under simulation assumption; source contract unverified', 'synthetic': False,
        'cutoff_exclusive': cutoff, 'timestamp_convention': 'timezone-naive file timestamps',
        'source_months': sorted({row['source_month'] for row in history}),
        'cross_month_linkage': 'selected shared application/context/product consistent before cutoff; not independent identity proof' if link_months else 'April only; cross-month linkage not used',
        'history_rows': len(history), 'first_at': history[0]['event_datetime'], 'last_at': history[-1]['event_datetime'],
        'latest_observed_noncampaign_journey': latest_journey,
        'event_counts': dict(sorted(Counter(row['event_name'] for row in history).items(), key=lambda item: item[0] or '')),
        'channel_observations': per_channel, 'partner_history': partner_history,
        'application_event_columns': ['at','event','stage','substage','product','partner'],
        'application_sequences': application_sequences,
        'application_unassigned_rows': sum(not row.get('application_id') for row in history),
        'recent_events': recent,
        'sequence_coverage': {'recent_limit': RECENT_LIMIT, 'included': len(recent), 'older_rows_in_counts_only': max(0,len(history)-RECENT_LIMIT),
                              'all_rows_saved_locally': True, 'equal_time_order': 'deterministic; relative order unknown'},
        'limitations': 'Event labels only; no invented DPD/PTP/balance/note. Channel meanings, send eligibility and outcomes unverified. Demo catalog only.',
    }
    return state, {'partners': {alias: raw for raw, alias in partners.items()},
                   'applications': {alias: raw for raw, alias in applications.items()},
                   'products': {alias: raw for raw, alias in products.items()}}


def event_sentences(state):
    return [f"{row['at']} | event={row['event']} | stage={row['stage']} | substage={row['substage']} | channel={row['channel']} | application={row['application']} | product={row['product']}"
            for row in state['recent_events']]


def validate_event_summary(content, sentences):
    result = json.loads(content)
    if not isinstance(result, dict) or set(result) != {'highlights'}:
        raise ValueError('Expected exactly highlights')
    highlights = result['highlights']
    if not isinstance(highlights, list) or not 1 <= len(highlights) <= 3:
        raise ValueError('Expected one to three extractive highlights')
    indices = []
    for fact in highlights:
        if not isinstance(fact, dict) or set(fact) != {'event_index', 'fact'}:
            raise ValueError('Invalid highlight schema')
        index = fact['event_index']
        if type(index) is not int or not 0 <= index < len(sentences) or fact['fact'] != sentences[index]:
            raise ValueError('Highlight is not exact supplied event evidence')
        indices.append(index)
    if indices != sorted(set(indices)):
        raise ValueError('Highlights must be unique and chronological')
    return result
