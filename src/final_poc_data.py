"""Read-only real selection and explicitly separate deterministic synthetic fixtures."""
from collections import Counter
from datetime import datetime,timedelta
import copy
import json

from benchmark_inputs import CUTOFF,recent_records,event_statement,structured_features,strategy_state
from real_data import rows,build_real_state
from robustness_inputs import compact_state


def real_examples(connection,root):
    identities=json.loads((root/'outputs/benchmark/private/identity_map.json').read_text())
    connection.execute("CREATE OR REPLACE TEMP VIEW final_all AS SELECT *,'march' AS source_month FROM march UNION ALL SELECT *,'april' AS source_month FROM april")
    large=connection.execute('''WITH h AS (SELECT * FROM april WHERE event_datetime<CAST(? AS TIMESTAMP_NS)),
        qa AS (SELECT application_id,count(DISTINCT context_id) AS contexts,count(DISTINCT nullif(product_id,'')) AS products
            FROM h WHERE nullif(application_id,'') IS NOT NULL GROUP BY 1),
        bad AS (SELECT DISTINCT context_id FROM h JOIN qa USING(application_id) WHERE contexts<>1 OR products<>1)
        SELECT context_id,count(*) AS n FROM h WHERE context_id IS NOT NULL AND context_id<>''
            AND context_id NOT IN (SELECT context_id FROM bad) GROUP BY 1 ORDER BY n DESC,md5(context_id) LIMIT 1''',[CUTOFF]).fetchone()
    if large is None: raise ValueError('No valid April context for large-history demonstration')
    specs=[('CTX03','small','april',identities['CTX03']['context_id']),
           ('CTX01','medium','linked',identities['CTX01']['context_id']),
           ('CTX06','larger','linked',identities['CTX06']['context_id']),
           ('APRIL_MAX_VALID','largest_valid_april','april',large[0])]
    totals=rows(connection,'''SELECT count(*) AS dataset_rows,
        count(*) FILTER(WHERE event_datetime<CAST(? AS TIMESTAMP_NS)) AS dataset_rows_before_cutoff
        FROM final_all''',[CUTOFF])[0]
    examples=[]
    schema=rows(connection,'DESCRIBE final_all')
    projection=','.join('cast(event_datetime AS VARCHAR) AS event_datetime' if c['column_name']=='event_datetime' else f'"{c["column_name"]}"' for c in schema)
    for alias,band,scope,context in specs:
        scoped='' if scope=='linked' else "AND source_month='april'"
        counts=rows(connection,f'''SELECT count(*) AS context_scope_rows_all_times,
            count(*) FILTER(WHERE event_datetime<CAST(? AS TIMESTAMP_NS)) AS historical_rows,
            count(*) FILTER(WHERE event_datetime>=CAST(? AS TIMESTAMP_NS)) AS excluded_at_or_after_cutoff
            FROM final_all WHERE context_id=? {scoped}''',[CUTOFF,CUTOFF,context])[0]
        history=rows(connection,f'''SELECT {projection} FROM final_all WHERE context_id=?
            AND event_datetime<CAST(? AS TIMESTAMP_NS) {scoped}
            ORDER BY event_datetime,event_name,journey_stage,journey_substage,channel,partner_id,
                product_id,application_id,session_id,source_month''',[context,CUTOFF])
        if len(history)!=counts['historical_rows']: raise ValueError('Historical count mismatch')
        legacy,aliases=build_real_state(history,link_months=scope=='linked')
        item={'candidate':{'scope':scope},'history':history,'legacy_state':legacy,'aliases':aliases}
        common=structured_features(item,connection)
        baseline=strategy_state(common,recent_records(history,aliases,20),'B')
        compact,ledger=compact_state(baseline,history,aliases)
        compact['source_coverage']={'synthetic':False,'months':legacy['source_months'],
            'window':'Available files only, strictly before cutoff; not lifetime customer history',
            'all_selected_history_rows_counted':True}
        examples.append({'alias':alias,'category':'real','size_band':band,'scope':scope,'history':history,'aliases':aliases,
            'state':compact,'ledger':ledger,**counts,**totals,'event_types':len(common['event_counts']),
            'applications':len(common['application_features']),'partners':len(common['partner_history']),
            'recent_individual_events':len(compact['recent_event_indices']),
            'retained_individual_events':len(compact['event_rows']),
            'aggregate_only_rows':len(history)-len(compact['event_rows'])})
    return examples,totals


def synthetic_stress(n,vocabulary=3,applications=1,partners=1):
    """Stream generated events into counters; never materialize a million-row Python list."""
    if n<8 or n>1_000_000 or not 3<=vocabulary<=100 or min(applications,partners)<1:
        raise ValueError('Explicit synthetic stress bounds exceeded')
    start=datetime(2026,3,1); cutoff=datetime.fromisoformat(CUTOFF)
    def at(i): return (start+timedelta(seconds=i)).isoformat(sep=' ')
    def age(i): return (cutoff-start).total_seconds()-i
    labels=['campaign_sent','campaign_clicked','AIP']+[f'fixture_event_{i:02d}' for i in range(3,vocabulary)]
    counts=Counter(); channels={}; apps={}; partner_stats={}; recent=[]
    latest_aip=None; latest_journey=None
    for i in range(n):
        event=labels[i%vocabulary]; channel=('SMS','WA',None)[i%3]
        app=f'A{i%applications+1}'; partner=f'P{i%partners+1}'
        counts[event]+=1
        c=channels.setdefault(channel or '(missing)',{'events':0,'campaign_sent':0,'campaign_open_read':0,'campaign_clicked':0,
            'last_event_age_seconds':None,'last_send_age_seconds':None,'last_open_read_age_seconds':None,'last_click_age_seconds':None})
        c['events']+=1; c['last_event_age_seconds']=age(i)
        if event in ('campaign_sent','campaign_clicked'):
            c[event]+=1; c['last_send_age_seconds' if event=='campaign_sent' else 'last_click_age_seconds']=age(i)
        a=apps.setdefault(app,{'rows':0,'event_counts':Counter(),'latest_observed_journey':[['fixture_stage','fixture_substage']],
            'last_event_age_seconds':None,'products':['PR1'],'partners':set()})
        a['rows']+=1; a['event_counts'][event]+=1; a['last_event_age_seconds']=age(i); a['partners'].add(partner)
        p=partner_stats.setdefault(partner,{'first_index':i,'last_index':i,'rows':0}); p['last_index']=i; p['rows']+=1
        if event=='AIP': latest_aip=i
        if event not in ('campaign_sent','campaign_clicked'): latest_journey=i
        if i>=n-8:
            recent.append({'at':at(i),'event':event,'stage':'fixture_stage','substage':'fixture_substage',
                           'channel':channel,'application':app,'product':'PR1'})
    for a in apps.values(): a['event_counts']=dict(a['event_counts']); a['partners']=sorted(a['partners'])
    state={'synthetic':True,'cutoff_exclusive':CUTOFF,'timestamp_convention':'synthetic naive seconds',
        'source_coverage':{'generated_rows':n,'parquet_customer_rows':0,'all_rows_before_cutoff':True},
        'observed_rows':n,'event_counts':dict(counts),'first_last_event_age_seconds':[age(0),age(n-1)],
        'channel_observations':channels,'application_features':apps,
        'partner_history':{k:{'first_at':at(v['first_index']),'last_at':at(v['last_index']),'rows':v['rows']} for k,v in partner_stats.items()},
        'chronological_events':[event_statement(r) for r in recent],
        'latest_observed_noncampaign_journey':{'at':at(latest_journey),'stage':'fixture_stage','substage':'fixture_substage'} if latest_journey is not None else None,
        'latest_aip_observation':{'at':at(latest_aip),'event':'AIP','stage':'fixture_stage','substage':'fixture_substage'} if latest_aip is not None else None,
        'application_unassigned_rows':0,'missingness':{'null':'explicit missing or unobserved, not zero',
            'missing_channel_rows':channels.get('(missing)',{}).get('events',0)},
        'sequence_coverage':{'included':8,'older_rows_in_aggregates':n-8,'source_order':'generated chronological order'},
        'demo_only':True,'action_space_authoritative':False}
    assert sum(counts.values())==n and sum(c['events'] for c in channels.values())==n
    return state


def retrieval_fixture(filler_count,position,color):
    if position not in ('beginning','middle','end') or color not in ('RED','BLUE','GREEN'):
        raise ValueError('Unknown diagnostic condition')
    filler='Background entry: an ordinary item was logged.'
    signal=f'The badge color is {color}.'
    records=[filler]*filler_count
    index={'beginning':0,'middle':filler_count//2,'end':filler_count}[position]
    records.insert(index,signal)
    payload={'model':'laya','state':{'synthetic':True,'records':records},'questions':{'badge_color':{
        'type':'choice','instructions':'What badge color is explicitly stated in the records? Select that color; background entries contain no color facts.',
        'criteria':{c:f'The explicitly stated badge color is {c}.' for c in ('RED','BLUE','GREEN')}}}}
    return payload,{'synthetic':True,'correct_answer':color,'signal_index':index,'filler_count':filler_count,'position':position}


def validate_retrieval(raw,expected):
    import math
    answer=raw['answers']['badge_color']; probs=answer['probabilities']
    if set(probs)!={'RED','BLUE','GREEN'} or answer['choice'] not in probs: raise ValueError('Invalid factual choices')
    if any(type(v) not in (int,float) or not math.isfinite(v) or not 0<=v<=1 for v in probs.values()): raise ValueError('Invalid probabilities')
    if not math.isclose(sum(probs.values()),1,abs_tol=.002) or probs[answer['choice']]<max(probs.values()): raise ValueError('Invalid factual distribution')
    return {'answer':answer['choice'],'probabilities':probs,'retrieval_correct':answer['choice']==expected}
