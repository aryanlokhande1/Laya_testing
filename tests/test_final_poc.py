"""Focused synthetic fixtures; no new real model calls in regression tests."""
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from final_poc_data import synthetic_stress,retrieval_fixture,validate_retrieval,real_examples
from run_final_poc import actual_call,write_csv
from benchmark_inputs import CUTOFF
from test_benchmark import history_fixture


def factual_response(answer='RED'):
    probs={key:.1 for key in ('RED','BLUE','GREEN')}; probs[answer]=.8
    return {'answers':{'badge_color':{'choice':answer,'probabilities':probs}},'usage':{'input_tokens':563,'output_tokens':0}}


class FinalPocTests(unittest.TestCase):
    def test_stress_counts_missingness_and_chronology(self):
        state=synthetic_stress(1000)
        self.assertTrue(state['synthetic'])
        self.assertEqual(state['source_coverage']['parquet_customer_rows'],0)
        self.assertEqual(sum(state['event_counts'].values()),1000)
        self.assertEqual(state['event_counts'],{'campaign_sent':334,'campaign_clicked':333,'AIP':333})
        self.assertEqual(state['missingness']['missing_channel_rows'],333)
        self.assertEqual(len(state['chronological_events']),8)
        self.assertEqual(state['chronological_events'],sorted(state['chronological_events']))
        self.assertEqual(state['sequence_coverage']['older_rows_in_aggregates'],992)
        self.assertLess(datetime.fromisoformat(state['chronological_events'][-1].split(' | ')[0]),datetime.fromisoformat(CUTOFF))

    def test_fixed_cardinality_vs_entity_growth_without_truncation(self):
        small=synthetic_stress(1000); larger=synthetic_stress(10000)
        self.assertEqual(set(small['event_counts']),set(larger['event_counts']))
        self.assertEqual(len(larger['application_features']),1)
        self.assertEqual(len(larger['partner_history']),1)
        high=synthetic_stress(1000,40,20,20)
        self.assertEqual(len(high['event_counts']),40)
        self.assertEqual(len(high['application_features']),20)
        self.assertEqual(len(high['partner_history']),20)
        self.assertEqual(sum(a['rows'] for a in high['application_features'].values()),1000)
        self.assertEqual(sum(p['rows'] for p in high['partner_history'].values()),1000)

    def test_fixture_bounds_explicit(self):
        for n in (0,7,1000001):
            with self.assertRaises(ValueError): synthetic_stress(n)

    def test_factual_positions_are_matched_without_answer_metadata_leak(self):
        states=[]
        for position in ('beginning','middle','end'):
            request,truth=retrieval_fixture(48,position,'GREEN')
            self.assertEqual(set(request['state']),{'synthetic','records'})
            self.assertNotIn('correct_answer',json.dumps(request))
            self.assertNotIn('signal_index',json.dumps(request))
            self.assertEqual(request['state']['records'][truth['signal_index']],'The badge color is GREEN.')
            self.assertEqual(len(request['state']['records']),49)
            states.append(Counter(request['state']['records']))
        self.assertEqual(states[0],states[1]); self.assertEqual(states[0],states[2])

    def test_processing_success_does_not_imply_retrieval_success(self):
        raw=factual_response('RED')
        result=validate_retrieval(raw,'BLUE')
        self.assertFalse(result['retrieval_correct'])
        self.assertEqual(result['answer'],'RED')
        payload,_=retrieval_fixture(4,'end','BLUE')
        outcome={'status':'response','response':raw,'http_latency_ms':10,'supervisor_elapsed_ms':12,
            'supervisor_started_utc':'2026-10-29T01:00:00Z','supervisor_ended_utc':'2026-10-29T01:00:01Z'}
        with tempfile.TemporaryDirectory() as directory,patch('run_final_poc.supervise',return_value=outcome):
            measured=actual_call(Path(directory),payload,'http://127.0.0.1:11434','synthetic_factual_retrieval','BLUE')
        self.assertEqual(measured['status'],'success')
        self.assertFalse(measured['retrieval_correct'])
        self.assertEqual(measured['input_tokens'],563)
        self.assertTrue(measured['over_512_tokens'])

    def test_failed_processing_never_receives_fabricated_tokens(self):
        payload,_=retrieval_fixture(4,'end','BLUE')
        late={'status':'deadline_exceeded','response':factual_response(),'supervisor_elapsed_ms':46000,
            'supervisor_started_utc':'2026-10-29T01:00:00Z','supervisor_ended_utc':'2026-10-29T01:00:46Z'}
        with tempfile.TemporaryDirectory() as directory,patch('run_final_poc.supervise',return_value=late):
            result=actual_call(Path(directory),payload,'http://127.0.0.1:11434','synthetic_factual_retrieval','BLUE')
        self.assertEqual(result['status'],'deadline_exceeded')
        self.assertNotIn('input_tokens',result)
        self.assertNotIn('retrieval_correct',result)

    def test_invalid_factual_probabilities_rejected(self):
        raw=factual_response(); raw['answers']['badge_color']['probabilities']['BLUE']=-.1
        with self.assertRaises(ValueError): validate_retrieval(raw,'RED')

    def test_real_selection_excludes_future_from_features(self):
        with tempfile.TemporaryDirectory() as directory,duckdb.connect() as connection:
            root=Path(directory); private=root/'outputs/benchmark/private'; private.mkdir(parents=True)
            mapping={alias:{'context_id':f'PRIVATE-CONTEXT-{index:02d}'} for alias,index in [('CTX03',0),('CTX01',1),('CTX06',2)]}
            (private/'identity_map.json').write_text(json.dumps(mapping))
            for month in ('march','april'):
                connection.execute(f'''CREATE TABLE {month}(context_id VARCHAR,event_datetime TIMESTAMP_NS,event_name VARCHAR,
                    journey_stage VARCHAR,journey_substage VARCHAR,channel VARCHAR,partner_id VARCHAR,
                    application_id VARCHAR,product_id VARCHAR,session_id VARCHAR)''')
            for index in range(3):
                for i,row in enumerate(history_fixture(index,12+index*4)):
                    month='march' if index>0 and i%2==0 else 'april'
                    at=row['event_datetime'].replace('2026-04','2026-03') if month=='march' else row['event_datetime']
                    connection.execute(f'INSERT INTO {month} VALUES (?,?,?,?,?,?,?,?,?,?)',[
                        row['context_id'],at,row['event_name'],row['journey_stage'],row['journey_substage'],row['channel'],
                        row['partner_id'],row['application_id'],row['product_id'],row['session_id']])
            before,totals=real_examples(connection,root)
            connection.execute("INSERT INTO april SELECT context_id,TIMESTAMP '2026-04-20',event_name,journey_stage,journey_substage,channel,partner_id,application_id,'FUTURE_PRODUCT',session_id FROM april")
            after,_=real_examples(connection,root)
            self.assertEqual([r['state'] for r in before],[r['state'] for r in after])
            self.assertTrue(all(r['state']['cutoff_exclusive']==CUTOFF for r in after))

    def test_csv_customer_identifiers_blocked(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError):
                write_csv(Path(directory)/'results.csv',[{'alias':'RAW-CUSTOMER-123'}],{'RAW-CUSTOMER-123'})


if __name__=='__main__': unittest.main()
