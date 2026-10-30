"""Synthetic-only state tests; native PostgreSQL fixtures always roll back."""
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from data.combine_parquets import ROOT,digest
from db.postgres import configuration,execute,DatabaseUnavailable
from features.state_projection_v1 import (CHANNELS,CHANNEL_METRICS,SCALARS,TABLE_COLUMNS,AIP_LABELS,
    model_sql,validate_state,selected_lineage)
from features.state_audit_v1 import duplicate_exposure,resolve_application_context
from features.build_laya_state_v1 import build,render_build_sql,render_verification_sql
from test_feature_pipeline import FIELDS


def sample():
    return {'history':{'as_of':'2026-05-01T00:00:00','event_rows':20,'observed_span_hours':100,
        'missing_channel_event_rows':1,'communication_coverage':{'available_rows':8,'retained_rows':1},
        'non_campaign_coverage':{'available_rows':6,'retained_rows':1}},
      'channels':{ch:{m:(None if m.startswith('hours_') else 0) for m in CHANNEL_METRICS} for ch in CHANNELS},
      'contact_pressure_7d':{'known_channel_send_rows':0,'missing_channel_send_rows':0},
      'applications':{'distinct_observed':2},'products':{'distinct_observed':1},'activity':{'observed_event_days_7d':1},
      'latest_event':{'event_name':'campaign_sent','hours_before_snapshot':1},
      'latest_non_campaign_journey':{'event_name':'AIP','stage':'AIP','substage':None,'hours_before_snapshot':2},
      'aip_history':{label:{'event_rows':1,'hours_since_last':50} for label in AIP_LABELS},
      'recent_communication_events':[{'event_name':'campaign_sent','channel':'SMS','hours_before_snapshot':1}],
      'recent_non_campaign_journey_events':[{'event_name':'AIP','channel':'','hours_before_snapshot':2,'stage':'AIP','substage':None}]}


class StateProjectionTests(unittest.TestCase):
    def test_existing_target_is_never_replaced_or_reports_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            destination=Path(tmp)/'fresh_run'
            with patch('features.build_laya_state_v1.configuration',return_value={'local':'test'}),patch('features.build_laya_state_v1.query',return_value=[{'present':True}]),patch('features.build_laya_state_v1.execute') as mutation:
                with self.assertRaises(FileExistsError):build(destination)
                mutation.assert_not_called()
                self.assertFalse(destination.exists())

    def test_explicit_small_schema_and_complete_lineage(self):
        self.assertEqual(len(SCALARS),32)
        self.assertEqual(len(TABLE_COLUMNS),42)
        self.assertEqual([x['column'] for x in selected_lineage()],list(TABLE_COLUMNS))
        self.assertNotIn('b.context_id',model_sql())
        self.assertNotIn('b.event_audit',model_sql())
        with self.assertRaises(ValueError):model_sql('b; DROP')

    def test_valid_observations_preserve_null_and_exact_aip_spelling(self):
        value=sample()
        validate_state(value,1,1)
        self.assertIsNone(value['channels']['SMS']['hours_since_last_engagement'])
        self.assertEqual(len(value['aip_history']),4)
        self.assertIn('aip  ok',value['aip_history'])
        self.assertEqual(value['recent_non_campaign_journey_events'][0]['channel'],'')

    def test_identifiers_and_reporting_fields_are_rejected(self):
        for key in ['context_id','application_id','quality_flags','choices','matched_open_rate','affinity']:
            value=sample();value[key]='FORBIDDEN'
            with self.assertRaises(ValueError):validate_state(value,1,1)
        value=sample();value['latest_non_campaign_journey']['stage']='OPAQUE-PARTNER'
        with self.assertRaises(ValueError):validate_state(value,1,1,forbidden_identifiers=['OPAQUE-PARTNER'])
        value=sample();value['latest_event']['event_name']='00000000-0000-4000-8000-000000000001'
        with self.assertRaises(ValueError):validate_state(value,1,1)

    def test_campaign_tokens_cannot_be_non_campaign_stage_or_event(self):
        for field in ['event_name','stage','substage']:
            value=sample();value['latest_non_campaign_journey'][field]='campaign_sent'
            with self.assertRaises(ValueError):validate_state(value,1,1)
        value=sample();value['recent_non_campaign_journey_events'][0]['stage']=' campaign_clicked '
        with self.assertRaises(ValueError):validate_state(value,1,1)

    def test_independent_bounds_chronology_and_missing_channels(self):
        value=sample();value['history']['communication_coverage']={'available_rows':2,'retained_rows':2}
        value['recent_communication_events']=[{'event_name':'campaign_sent','channel':None,'hours_before_snapshot':3},
          {'event_name':'campaign_clicked','channel':'WA','hours_before_snapshot':1}]
        validate_state(value,2,1)
        with self.assertRaises(ValueError):validate_state(value,1,1)
        value['recent_communication_events'].reverse()
        with self.assertRaises(ValueError):validate_state(value,2,1)

    def test_no_advanced_zero_fallback_or_unapproved_channels(self):
        for bad in ['Email','whatsapp','RCS','MISSING']:
            value=sample();value['recent_communication_events'][0]['channel']=bad
            with self.assertRaises(ValueError):validate_state(value,1,1)
        value=sample();value['channels']['SMS']['matched_open_rate']=0
        with self.assertRaises(ValueError):validate_state(value,1,1)
        for bad in [-1,float('nan'),True]:
            value=sample();value['channels']['SMS']['send_rows']=bad
            with self.assertRaises(ValueError):validate_state(value,1,1)

    def test_lookup_no_match_unique_and_ambiguous_without_latest_fallback(self):
        for matches,status in [([], 'no_matching_context'),([{'context_id':'fixture-a'}],'unique_context'),
                               ([{'context_id':'fixture-a'},{'context_id':'fixture-b'}],'multiple_contexts')]:
            with patch('features.state_audit_v1.query',return_value=matches) as call:
                result=resolve_application_context("fixture'app",env={'local':'test'})
                self.assertEqual(result['status'],status)
                self.assertEqual(result['context_ids'],[x['context_id'] for x in matches])
                sql=call.call_args[0][0]
                self.assertIn("fixture''app",sql)
                self.assertNotIn('LIMIT 1',sql)
                self.assertNotIn('ORDER BY event_datetime',sql)
        with patch('features.state_audit_v1.query') as call:
            self.assertEqual(resolve_application_context('  ')['context_count'],0)
            call.assert_not_called()

    def test_exact_duplicate_candidates_are_verified_without_source_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'source.parquet';binary=root/'exposure.bin'
            c=duckdb.connect();c.execute('CREATE TABLE x('+','.join(n+' '+t for n,t in FIELDS)+',source_filename VARCHAR,source_month VARCHAR,source_row_number BIGINT)')
            c.execute("INSERT INTO x(context_id,event_datetime,event_name,channel,source_filename,source_month,source_row_number) VALUES ('fixture','2026-03-01','campaign_sent','SMS','m','2026-03',1),('fixture','2026-03-01','campaign_sent','SMS','a','2026-04',1),('fixture','2026-05-01','future',NULL,'a','2026-04',2)")
            c.execute('COPY x TO ? (FORMAT PARQUET)',[str(source)]);c.close()
            before=digest(source)
            result=duplicate_exposure(source,'2026-05-01 00:00:00',binary,root/'reports')
            self.assertEqual(result['exact_duplicate_groups'],1)
            self.assertEqual(result['excess_rows'],1)
            self.assertEqual(result['cross_file_groups'],1)
            self.assertEqual(result['affected_contexts'],1)
            self.assertFalse(result['deduplicated'])
            self.assertEqual(digest(source),before)


@unittest.skipUnless(os.environ.get('LAYA_TEST_POSTGRES')=='1','Enable local rollback-only PostgreSQL fixtures with LAYA_TEST_POSTGRES=1')
class NativeStateTests(unittest.TestCase):
    def test_complete_sql_split_stage_correction_aip_coverage_and_cutoff(self):
        types={'VARCHAR':'text','TIMESTAMP_NS':'timestamp','DOUBLE':'double precision','BIGINT':'bigint'}
        schema=','.join(n+' '+types[t] for n,t in FIELDS)+',source_filename text,source_month text,source_row_number bigint'
        sql='BEGIN; CREATE TEMP TABLE fixture_events('+schema+');\n'
        rows=[
          ('fixture-X','2026-03-01','campaign_sent','SMS','fixture-app','campaign_sent','campaign_sent','m',1),
          ('fixture-X','2026-03-01','campaign_sent','SMS','fixture-app','campaign_sent','campaign_sent','a',1),
          ('fixture-X','2026-03-02','AIP','GRCS','fixture-app','AIP','Observed','m',2),
          ('fixture-X','2026-03-03','aip ok',None,'fixture-app','AIP','Observed','m',3),
          ('fixture-X','2026-03-04','aip  ok','','fixture-app','AIP','Observed','m',4),
          ('fixture-X','2026-03-05','aip not ok',None,'fixture-app','AIP','Observed','m',5),
          *[('fixture-X',f'2026-04-{d:02d}','recorded_event',None,'fixture-app','Recorded','Observed','a',d) for d in range(20,25)],
          ('fixture-X','2026-04-29','journey_latest','','fixture-app','campaign_sent',' ','a',29),
          ('fixture-X','2026-04-30 21:00','campaign_sent','WA','fixture-app','campaign_sent','campaign_sent','a',30),
          ('fixture-X','2026-04-30 22:00','campaign_open_read','WA','fixture-app','campaign_open_read','campaign_open_read','a',31),
          ('fixture-X','2026-05-01','future_event','WA','fixture-app','Future','Future','a',32),
          ('fixture-Y','2026-04-28','campaign_sent','GRCS','','campaign_sent','campaign_sent','a',33),
          ('fixture-Z','2026-04-29','recorded_event',None,None,'Recorded',None,'a',34),
          ('fixture-Z','2026-04-30',None,None,None,None,None,'a',35),
          ('fixture-T','2026-04-01','recorded_event',None,'fixture-app','Recorded',None,'a',36),
          (' ','2026-04-01','ignored_key',None,None,None,None,'a',37),
          ('fixture-undated',None,'undated',None,None,None,None,'a',38)]
        from data.combine_parquets import literal
        for context,at,name,ch,app,stage,substage,file,row in rows:
            values=[context,at,name,ch,app,stage,substage,file,'2026-03' if file=='m' else '2026-04',row]
            sql+='INSERT INTO fixture_events(context_id,event_datetime,event_name,channel,application_id,journey_stage,journey_substage,source_filename,source_month,source_row_number) VALUES('+','.join('NULL' if v is None else str(v) if isinstance(v,int) else literal(v) for v in values)+');\n'
        original=(ROOT/'sql/features/customer_aggregates.sql').read_text(encoding='utf-8').replace('raw.gold_events','pg_temp.fixture_events').replace('CREATE TABLE features.:"feature_stage" AS','CREATE TEMP TABLE fixture_features AS')
        sql+=original
        sql+='CREATE TEMP TABLE _state_duplicates(context_id text,duplicate_groups bigint,excess_rows bigint,rows_in_groups bigint,communication_excess_rows bigint,non_campaign_excess_rows bigint,cross_file_groups bigint);\n'
        sql+="INSERT INTO _state_duplicates VALUES('fixture-X',1,1,2,1,0,1);\n"
        sql+=render_build_sql().replace('raw.gold_events','pg_temp.fixture_events').replace('features.customer_features','pg_temp.fixture_features').replace('features.laya_state_v1','pg_temp.fixture_state')
        sql+=render_verification_sql().replace('raw.gold_events','pg_temp.fixture_events').replace('features.customer_features','pg_temp.fixture_features').replace('features.laya_state_v1','pg_temp.fixture_state')
        sql+="SELECT json_build_object('states',json_agg(state_json ORDER BY context_id),'quality',json_agg(quality_flags ORDER BY context_id)) FROM fixture_state; ROLLBACK;"
        output=execute(sql,env=configuration(prefer_application=False),variables={'snapshot':'2026-05-01 00:00:00','communication_limit':4,'journey_limit':4,'sequence_length':8,'pressure_windows':'1,7,30','trend_days':7,'expected_rows':4,'duplicate_groups':1,'duplicate_excess_rows':1})
        result=next(json.loads(line) for line in output.splitlines() if line.startswith('{') and '"states"' in line)
        self.assertEqual(len(result['states']),4)
        for state in result['states']:validate_state(state,4,4,forbidden_identifiers=['fixture-X','fixture-Y','fixture-Z','fixture-T','fixture-app'])
        x=result['states'][1]
        self.assertEqual(x['latest_event']['event_name'],'campaign_open_read')
        self.assertEqual(x['latest_non_campaign_journey']['event_name'],'journey_latest')
        self.assertIsNone(x['latest_non_campaign_journey']['stage'])
        self.assertEqual(set(x['aip_history']),set(AIP_LABELS))
        self.assertTrue(all(e['event_name'] not in AIP_LABELS for e in x['recent_non_campaign_journey_events']))
        self.assertEqual(x['channels']['SMS']['send_rows'],2)
        y=result['states'][2]
        self.assertEqual(y['recent_non_campaign_journey_events'],[])
        self.assertIsNone(y['latest_non_campaign_journey']['event_name'])
        self.assertEqual(result['quality'][3]['unclassified_event_rows'],1)


if __name__=='__main__':unittest.main()
