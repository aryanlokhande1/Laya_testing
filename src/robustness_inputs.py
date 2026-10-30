"""Loss-aware table encoding of frozen B inputs; no changes to existing experiments."""
import copy
import itertools
import json

from benchmark_inputs import recent_records,event_statement,digest,CUTOFF
from real_data import build_real_state

ALIASES = ('CTX01','CTX02','CTX03','CTX08','CTX09')
CHANNEL_COLUMNS = ('events','campaign_sent','campaign_open_read','campaign_clicked',
    'last_event_age_seconds','last_send_age_seconds','last_open_read_age_seconds','last_click_age_seconds')
APP_COLUMNS = ('rows','event_counts','latest_observed_journey','last_event_age_seconds','products','partners')
AIP_LABELS = {'AIP','aip  ok','aip ok','aip not ok'}


def compact_state(original,history,aliases):
    common = original['structured']
    linked = common['scope']=='linked'
    # Revalidate cutoff and identity grouping rather than trusting copied timestamps.
    legacy,_ = build_real_state(history,link_months=linked)
    records = recent_records(history,aliases,len(history))
    last8 = list(range(max(0,len(records)-8),len(records)))
    if [event_statement(records[i]) for i in last8] != original['chronological_events']:
        raise ValueError('Frozen B sequence does not match saved history')
    important = {}
    for index,row in enumerate(records):
        if row['event'] in AIP_LABELS:
            important[row['event']] = index
    journey = [i for i,row in enumerate(records) if row['event'] not in {'campaign_sent','campaign_clicked','campaign_open_read'}]
    latest_journey = [i for i in journey if records[i]['at']==records[journey[-1]]['at']] if journey else []
    included = sorted(set(last8)|set(important.values())|set(latest_journey))
    types,table,type_lookup = {},[],{}
    for i in included:
        row = records[i]
        identity = (row['event'],row['stage'],row['substage'])
        if identity not in type_lookup:
            label = f'T{len(types)}'
            type_lookup[identity] = label
            types[label] = list(identity)
        table.append([row['at'],type_lookup[identity],row['channel'],row['application'],row['product']])
    positions = {source:index for index,source in enumerate(included)}
    state = {
        'cutoff_exclusive':common['cutoff_exclusive'],'timestamp_convention':common['timestamp_convention'],
        'scope':common['scope'],'demo_only':True,'action_space_authoritative':False,
        'observed_rows':common['observed_rows'],'event_counts':copy.deepcopy(common['event_counts']),
        'first_last_event_age_seconds':[common['first_event_age_seconds'],common['last_event_age_seconds']],
        'channel_columns':list(CHANNEL_COLUMNS),
        'channels':{key:[row[column] for column in CHANNEL_COLUMNS] for key,row in common['channel_observations'].items()},
        'application_columns':list(APP_COLUMNS),
        'applications':{key:[copy.deepcopy(row[column]) for column in APP_COLUMNS] for key,row in common['application_features'].items()},
        'application_unassigned_rows':common['application_unassigned_rows'],
        'partner_columns':['first_at','last_at','rows'],
        'partners':{key:[row['first_at'],row['last_at'],row['rows']] for key,row in common['partner_history'].items()},
        'event_type_columns':['event','stage','substage'],'event_types':types,
        'event_columns':['at','type','channel','application','product'],'event_rows':table,
        'recent_event_indices':[positions[i] for i in last8],
        'latest_observed_noncampaign_journey_indices':[positions[i] for i in latest_journey],
        'latest_observed_aip_indices':{name:positions[i] for name,i in important.items()},
        'sequence_coverage':copy.deepcopy(original['sequence_coverage']),
        'missingness':{'null':'unobserved or missing, never zero','(missing)':'original blank/NULL channel group',
                      'no_noncampaign_journey_observed':not bool(latest_journey)},
    }
    ledger = {
        'preserved':'All original global event counts, recencies, per-channel fields including zero/null, per-app fields, partner fields, all eight event occurrences and sequence coverage.',
        'removed_presentation_fields':['structured.identity','structured.limitations'],
        'representation_changes':'Column-labelled arrays replace repeated dictionary keys. Event type table replaces repeated event/stage/substage strings. Shared event rows referenced once; repeated source occurrences are NOT deduplicated.',
        'grounded_additions':{'latest_noncampaign_journey_rows_outside_recent8':sum(i not in last8 for i in latest_journey),
                              'latest_aip_rows_outside_recent8':sum(i not in last8 for i in set(important.values())),
                              'explicit_missingness_legend':True},
        'comparison_limit':'Where older grounded journey/AIP rows are added, this compares a documented information-preserving supplement plus compression, not a pure encoding ablation.',
        'full_application_transitions':'Already absent from baseline B; not newly dropped. Full saved history retained privately.',
        'source_rows':len(history),'event_rows_in_compact_table':len(table),'baseline_state_sha256':digest(original),
    }
    assert reconstruct_baseline(state,common['identity'],common['limitations'])==original
    return state,ledger


def reconstruct_baseline(state,identity,limitations):
    common = {key:copy.deepcopy(state[key]) for key in ('cutoff_exclusive','timestamp_convention','scope',
        'demo_only','action_space_authoritative','observed_rows','event_counts','application_unassigned_rows')}
    common.update(identity=identity,limitations=limitations,
                  first_event_age_seconds=state['first_last_event_age_seconds'][0],
                  last_event_age_seconds=state['first_last_event_age_seconds'][1],
                  channel_observations={key:dict(zip(state['channel_columns'],value)) for key,value in state['channels'].items()},
                  application_features={key:dict(zip(state['application_columns'],value)) for key,value in state['applications'].items()},
                  partner_history={key:dict(zip(state['partner_columns'],value)) for key,value in state['partners'].items()})
    statements=[]
    for index in state['recent_event_indices']:
        at,type_id,channel,app,product=state['event_rows'][index]
        event,stage,substage=state['event_types'][type_id]
        statements.append(event_statement(dict(at=at,event=event,stage=stage,substage=substage,channel=channel,application=app,product=product)))
    return {'structured':common,'chronological_events':statements,'sequence_coverage':copy.deepcopy(state['sequence_coverage'])}


def reordered_request(request,order,state=None):
    result=copy.deepcopy(request)
    criteria=request['questions']['action']['criteria']
    if len(order)!=3 or set(order)!=set(criteria):
        raise ValueError('Permutation must contain the same three action IDs exactly once')
    result['questions']['action']['criteria']={key:criteria[key] for key in order}
    if state is not None:
        result['state']=copy.deepcopy(state)
    return result


def schedule():
    from laya_client import ACTION_CATALOG
    permutations=list(itertools.permutations(ACTION_CATALOG))
    planned=[]
    # Interleave repeats and aliases; avoid running all identical repeats consecutively.
    for repeat in range(1,4):
        for offset in range(6):
            for index,alias in enumerate(ALIASES):
                order_index=(offset+repeat-1+index)%6
                planned.append((alias,'original',order_index,repeat,permutations[order_index]))
                if order_index==0:
                    planned.append((alias,'compact',0,repeat,permutations[0]))
    return planned
