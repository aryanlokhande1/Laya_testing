"""Explicit v1 field selection and allowlisted model-facing JSON projection.

Limits of four communication and four non-campaign events are provisional
representation parameters. This module contains no business action or model call.
"""
import math
import re

CHANNELS=('SMS','WA','GRCS')
CAMPAIGN_EVENTS=('campaign_sent','campaign_open_read','campaign_clicked')
AIP_LABELS=('AIP','aip ok','aip  ok','aip not ok')
CHANNEL_METRICS=('send_rows','open_read_rows','click_rows','hours_since_last_send',
                 'hours_since_last_engagement','send_rows_7d')
CHANNEL_SCALARS=tuple(ch.lower()+'_'+metric for ch in CHANNELS for metric in CHANNEL_METRICS)
CORE_SCALARS=('history_row_count','observed_history_hours','missing_channel_rows',
 'missing_channel_send_rows_7d','send_rows_7d','distinct_application_count','distinct_product_count',
 'hours_since_last_observed_event','latest_observed_event_name','latest_non_campaign_event_name',
 'latest_non_campaign_journey_stage','latest_non_campaign_journey_substage',
 'hours_since_last_non_campaign_event','observed_event_active_days_7d')
SCALARS=CHANNEL_SCALARS+CORE_SCALARS
JSON_COLUMNS=('aip_label_counts','aip_label_recency_hours','recent_communication_events',
              'recent_non_campaign_journey_events','event_audit')
TABLE_COLUMNS=('context_id','feature_as_of','state_schema_version','state_json','quality_flags')+SCALARS+JSON_COLUMNS
ROOT_KEYS={'history','channels','contact_pressure_7d','applications','products','activity','latest_event',
           'latest_non_campaign_journey','aip_history','recent_communication_events','recent_non_campaign_journey_events'}
UUID=re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')


def model_sql(alias='b') -> str:
    """Render JSON formatting only; all aggregation remains in the SQL file."""
    if not re.fullmatch('[a-z][a-z0-9_]*',alias):raise ValueError('Invalid SQL alias')
    a=alias+'.'
    channels=','.join("'"+channel+"',jsonb_build_object("+','.join("'"+metric+"',"+a+channel.lower()+'_'+metric for metric in CHANNEL_METRICS)+')' for channel in CHANNELS)
    return f"""jsonb_build_object(
 'history',jsonb_build_object('as_of',{a}feature_as_of,'event_rows',{a}history_row_count,
   'observed_span_hours',{a}observed_history_hours,'missing_channel_event_rows',{a}missing_channel_rows,
   'communication_coverage',jsonb_build_object('available_rows',{a}_communication_rows,'retained_rows',jsonb_array_length({a}recent_communication_events)),
   'non_campaign_coverage',jsonb_build_object('available_rows',{a}_non_campaign_rows,'retained_rows',jsonb_array_length({a}recent_non_campaign_journey_events))),
 'channels',jsonb_build_object({channels}),
 'contact_pressure_7d',jsonb_build_object('known_channel_send_rows',{a}send_rows_7d,'missing_channel_send_rows',{a}missing_channel_send_rows_7d),
 'applications',jsonb_build_object('distinct_observed',{a}distinct_application_count),
 'products',jsonb_build_object('distinct_observed',{a}distinct_product_count),
 'activity',jsonb_build_object('observed_event_days_7d',{a}observed_event_active_days_7d),
 'latest_event',jsonb_build_object('event_name',{a}latest_observed_event_name,'hours_before_snapshot',{a}hours_since_last_observed_event),
 'latest_non_campaign_journey',jsonb_build_object('event_name',{a}latest_non_campaign_event_name,'stage',{a}latest_non_campaign_journey_stage,
   'substage',{a}latest_non_campaign_journey_substage,'hours_before_snapshot',{a}hours_since_last_non_campaign_event),
 'aip_history',coalesce((SELECT jsonb_object_agg(a.key,jsonb_build_object('event_rows',a.value,'hours_since_last',{a}aip_label_recency_hours->a.key)) FROM jsonb_each({a}aip_label_counts) a),'{{}}'::jsonb),
 'recent_communication_events',{a}recent_communication_events,
 'recent_non_campaign_journey_events',{a}recent_non_campaign_journey_events)"""


def validate_state(payload: dict, communication_limit=4, journey_limit=4, *, forbidden_identifiers=()):
    """Strict recursive key contract and privacy check, without printing a state."""
    if not isinstance(payload,dict) or set(payload)!=ROOT_KEYS:raise ValueError('Invalid state root contract')
    forbidden=set(forbidden_identifiers)
    def shape(value,keys):
        if not isinstance(value,dict) or set(value)!=set(keys):raise ValueError('Unexpected model-facing fields')
    def number(value,nullable=False,integer=False):
        if nullable and value is None:return
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or value<0:
            raise ValueError('Invalid nonnegative observation')
        if integer and not isinstance(value,int):raise ValueError('Count must be an integer')
    def text(value,nullable=True):
        if value is None and nullable:return
        if not isinstance(value,str):raise ValueError('Observed label must be text or NULL')
    shape(payload['history'],['as_of','event_rows','observed_span_hours','missing_channel_event_rows','communication_coverage','non_campaign_coverage'])
    from datetime import datetime
    if datetime.fromisoformat(payload['history']['as_of']).tzinfo is not None:raise ValueError('Source snapshot is timezone-naive')
    for key in ['event_rows','missing_channel_event_rows']:number(payload['history'][key],integer=True)
    number(payload['history']['observed_span_hours'])
    for key,limit in [('communication_coverage',communication_limit),('non_campaign_coverage',journey_limit)]:
        value=payload['history'][key];shape(value,['available_rows','retained_rows'])
        for n in value.values():number(n,integer=True)
        if value['retained_rows']!=min(value['available_rows'],limit):raise ValueError('Sequence coverage mismatch')
    shape(payload['channels'],CHANNELS)
    for channel in CHANNELS:
        shape(payload['channels'][channel],CHANNEL_METRICS)
        for metric,value in payload['channels'][channel].items():number(value,nullable=metric.startswith('hours_'),integer=not metric.startswith('hours_'))
    shape(payload['contact_pressure_7d'],['known_channel_send_rows','missing_channel_send_rows'])
    for value in payload['contact_pressure_7d'].values():number(value,integer=True)
    for key in ['applications','products']:
        shape(payload[key],['distinct_observed']);number(payload[key]['distinct_observed'],integer=True)
    shape(payload['activity'],['observed_event_days_7d']);number(payload['activity']['observed_event_days_7d'],integer=True)
    shape(payload['latest_event'],['event_name','hours_before_snapshot'])
    text(payload['latest_event']['event_name'])
    number(payload['latest_event']['hours_before_snapshot'],nullable=True)
    shape(payload['latest_non_campaign_journey'],['event_name','stage','substage','hours_before_snapshot'])
    nc=payload['latest_non_campaign_journey']
    for key in ['event_name','stage','substage']:text(nc[key])
    if nc['event_name'] in CAMPAIGN_EVENTS or any(isinstance(nc[k],str) and nc[k].strip().lower() in CAMPAIGN_EVENTS for k in ['stage','substage']):raise ValueError('Campaign field is not a journey stage')
    number(nc['hours_before_snapshot'],nullable=True)
    if not isinstance(payload['aip_history'],dict) or not set(payload['aip_history'])<=set(AIP_LABELS):raise ValueError('Unreviewed AIP source label')
    for observation in payload['aip_history'].values():
        shape(observation,['event_rows','hours_since_last']);number(observation['event_rows'],integer=True);number(observation['hours_since_last'],nullable=True)
    for key,limit,campaign in [('recent_communication_events',communication_limit,True),('recent_non_campaign_journey_events',journey_limit,False)]:
        events=payload[key]
        if not isinstance(events,list) or len(events)>limit:raise ValueError('Sequence bound violated')
        hours=[]
        for event in events:
            shape(event,['event_name','channel','hours_before_snapshot']+([] if campaign else ['stage','substage']))
            if not isinstance(event['event_name'],str) or not event['event_name'].strip():raise ValueError('Missing event label is not classified')
            if (event['event_name'] in CAMPAIGN_EVENTS)!=campaign:raise ValueError('Mixed sequence categories')
            if event['channel'] not in (*CHANNELS,'',None):raise ValueError('Unapproved channel code')
            number(event['hours_before_snapshot']);hours.append(event['hours_before_snapshot'])
            if not campaign and any(isinstance(event[k],str) and event[k].strip().lower() in CAMPAIGN_EVENTS for k in ['stage','substage']):raise ValueError('Invalid journey stage')
            if not campaign:
                for k in ['stage','substage']:text(event[k])
        if hours!=sorted(hours,reverse=True):raise ValueError('Sequence must be chronological')
    def privacy(value):
        if isinstance(value,str) and (value in forbidden or UUID.search(value)):raise ValueError('Opaque identifier in model input')
        if isinstance(value,dict):
            for child in value.values():privacy(child)
        elif isinstance(value,list):
            for child in value:privacy(child)
    privacy(payload)
    return payload


def selected_lineage() -> list[dict]:
    """Column-level expressions and exclusions are explicit, not inferred by LLM."""
    source={name:'features.customer_features.'+name for name in CHANNEL_SCALARS}
    for name in ['history_row_count','missing_channel_rows','send_rows_7d','distinct_application_count','distinct_product_count','hours_since_last_observed_event','aip_label_counts','aip_label_recency_hours']:
        source[name]='features.customer_features.'+name
    source.update(observed_history_hours='(last_observed_event_at-first_observed_event_at)/3600 seconds from existing features',
     observed_event_active_days_7d='features.customer_features.active_days_7d; includes all recorded event origins',
     missing_channel_send_rows_7d='count historical campaign_sent rows with NULL/blank channel and timestamp >= snapshot-7 days',
     latest_observed_event_name='latest raw event by timestamp, source filename, source row number',
     latest_non_campaign_event_name='latest labelled event outside the three exact campaign types',
     latest_non_campaign_journey_stage='same latest non-campaign event journey_stage; NULL if blank or campaign-token-valued',
     latest_non_campaign_journey_substage='same latest non-campaign event journey_substage; NULL if blank or campaign-token-valued',
     hours_since_last_non_campaign_event='(snapshot-latest labelled non-campaign event_datetime)/3600 seconds',
     recent_communication_events='latest configured campaign events ordered chronologically; label/channel/relative time only',
     recent_non_campaign_journey_events='latest configured labelled non-campaign events, with usable source stage/substage',
     context_id='source grouping key from existing feature table; internal PK only',feature_as_of='existing feature snapshot; must equal configured exclusive cutoff',
     state_schema_version="literal laya_state_v1; internal, excluded from state_json",state_json='allowlisted formatting defined in state_projection_v1.model_sql',
     quality_flags='raw missingness/category coverage, exact duplicate exposure, rejected stage fields, missing business contracts; excluded from state_json',
     event_audit='selected event timestamps, raw application IDs, source row/file references and original stage/substage; excluded from state_json')
    return [{'column':name,'source_expression':source[name],
             'implementation':'src/features/state_projection_v1.py (JSON formatting)' if name=='state_json' else 'sql/features/build_laya_state_v1.sql'} for name in TABLE_COLUMNS]
