"""Benchmark controls tested only with synthetic histories and mocked services."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from benchmark_inputs import (CUTOFF,STRATEGIES,select_cohort,structured_features,recent_records,
    strategy_state,laya_request,token_flags,check_request_size,event_statement,digest)
from benchmark_report import compare,percentile,assert_private_ids_absent,public_csv
from laya_client import validate_answer
from real_data import build_real_state
import run_benchmark


def history_fixture(index=0, count=12):
    history = []
    for i in range(count):
        history.append({'context_id':f'PRIVATE-CONTEXT-{index:02d}',
            'event_datetime':f'2026-04-{i//24+1:02d} {i%24:02d}:00:00',
            'event_name':['campaign_sent','campaign_clicked','AIP','aip  ok','product apply'][i%5],
            'journey_stage':'Fixture stage','journey_substage':'Fixture substage',
            'channel':'SMS' if i%2 else 'WA','partner_id':f'PRIVATE-PARTNER-{index:02d}',
            'application_id':f'PRIVATE-APP-{index:02d}','product_id':f'PRIVATE-PRODUCT-{index:02d}',
            'session_id':f'PRIVATE-SESSION-{index:02d}-{i:02d}','source_month':'april'})
    return history


def item_fixture(index=0,count=12):
    history = history_fixture(index,count)
    state,aliases = build_real_state(history)
    item = {'alias':f'CTX{index+1:02d}','history':history,'legacy_state':state,'aliases':aliases,
            'candidate':{'context_id':history[0]['context_id'],'scope':'april','applications':1,
                         'channels':'SMS,WA','engagement':1,'aip':1}}
    with duckdb.connect() as connection:
        item['structured'] = structured_features(item,connection)
    item['recent20'] = recent_records(history,aliases,20)
    return item


def valid_raw():
    return {'answers':{'action':{'choice':'SMS_REMINDER','probabilities':{
        'SMS_REMINDER':.6,'WA_SUPPORT':.3,'EMAIL_DETAILS':.1}}},'usage':{'input_tokens':400,'output_tokens':0}}


class BenchmarkTests(unittest.TestCase):
    def test_identical_structure_cutoff_and_no_future_records(self):
        item = item_fixture(count=25)
        recent = item['recent20']
        summary = {'highlights':[{'event_index':0,'fact':event_statement(recent[-8])}]}
        states = [strategy_state(item['structured'],recent,s,summary) for s in STRATEGIES]
        for state in states:
            self.assertEqual(state['structured'],item['structured'])
            self.assertEqual(state['structured']['cutoff_exclusive'],CUTOFF)
            self.assertNotIn('2026-04-15',json.dumps({k:v for k,v in state.items() if k!='structured'}))
        self.assertEqual(len(states[1]['chronological_events']),8)
        self.assertEqual(len(states[3]['chronological_events']),20)
        self.assertEqual(states[1]['sequence_coverage']['older_rows_not_in_sequence'],17)
        self.assertEqual(states[3]['sequence_coverage']['older_rows_not_in_sequence'],5)
        states[1]['structured']['observed_rows'] = 0
        self.assertEqual(item['structured']['observed_rows'],25)
        future = history_fixture()
        future[-1]['event_datetime'] = CUTOFF
        with self.assertRaisesRegex(ValueError,'cutoff'):
            build_real_state(future)

    def test_recent_chronology_and_short_history(self):
        item = item_fixture(count=9)
        state = strategy_state(item['structured'],item['recent20'],'D')
        statements = state['chronological_events']
        self.assertEqual(len(statements),9)
        self.assertEqual(statements,sorted(statements))
        self.assertEqual(state['sequence_coverage']['older_rows_not_in_sequence'],0)

    def test_qwen_grounding_no_silent_fallback(self):
        item = item_fixture()
        for summary in (None,{'highlights':[{'event_index':0,'fact':'will pay'}]},
                        {'highlights':[{'event_index':99,'fact':event_statement(item['recent20'][0])}]}):
            with self.assertRaises(ValueError):
                strategy_state(item['structured'],item['recent20'],'C',summary)

    def test_question_catalog_and_reverse_only_order(self):
        item = item_fixture()
        state = strategy_state(item['structured'],item['recent20'],'B')
        original,reverse = laya_request(state),laya_request(state,True)
        self.assertEqual(original,reverse)
        self.assertEqual(digest(original['state']),digest(reverse['state']))
        first = original['questions']['action']['criteria']
        second = reverse['questions']['action']['criteria']
        self.assertEqual(list(first),list(reversed(second)))
        self.assertNotEqual(json.dumps(original),json.dumps(reverse))
        questions = [laya_request(strategy_state(item['structured'],item['recent20'],s))['questions'] for s in ('A','B','D')]
        self.assertEqual(questions[0],questions[1])
        self.assertEqual(questions[0],questions[2])

    def test_actual_token_budget_and_endpoint_size(self):
        self.assertFalse(token_flags(512,512,8192))
        self.assertTrue(token_flags(513,512,8192))
        for tokens in (None,True,0,1.5,8193):
            with self.assertRaises(ValueError):
                token_flags(tokens,512,8192)
        payload = laya_request({'huge':'x'*70000})
        with self.assertRaisesRegex(ValueError,'no truncation'):
            check_request_size(payload)
        self.assertEqual(len(payload['state']['huge']),70000)

    def test_invalid_actions_rejected(self):
        raw = valid_raw()
        raw['answers']['action']['choice'] = 'SEND'
        with self.assertRaises(ValueError):
            validate_answer(raw)

    def test_probability_comparison_and_small_sample_percentile(self):
        one = {'status':'success','action_id':'SMS_REMINDER','probabilities':valid_raw()['answers']['action']['probabilities']}
        two = {'status':'success','action_id':'WA_SUPPORT','probabilities':{'SMS_REMINDER':.3,'WA_SUPPORT':.6,'EMAIL_DETAILS':.1}}
        self.assertTrue(compare(one,two)['action_changed'])
        self.assertAlmostEqual(compare(one,two)['max_probability_delta'],.3)
        self.assertAlmostEqual(compare(one,two)['probability_l1_delta'],.6)
        self.assertIsNone(compare(one,{'status':'error'}))
        self.assertEqual(percentile([1,2,3,4,5,6,7,8,9,10],.5),5.5)
        self.assertAlmostEqual(percentile([1,2,3,4,5,6,7,8,9,10],.95),9.55)

    def test_customer_identifiers_cannot_enter_reports(self):
        with self.assertRaisesRegex(ValueError,'Customer identifier detected') as error:
            assert_private_ids_absent('alias PRIVATE-CUSTOMER-123',{'PRIVATE-CUSTOMER-123'})
        self.assertNotIn('PRIVATE-CUSTOMER-123',str(error.exception))
        assert_private_ids_absent('CTX01 A original 123ms',{'PRIVATE-CUSTOMER-123'})
        # Raw HTTP diagnostics are deliberately not CSV fields.
        output = public_csv([{'context_alias':'CTX01','strategy':'A','option_order':'original','status':'error',
                              'private_error':'PRIVATE-CUSTOMER-123','error_code':'RuntimeError'}])
        self.assertNotIn('PRIVATE-CUSTOMER-123',output)

    def test_selection_is_deterministic_and_future_independent(self):
        with duckdb.connect() as connection:
            for month in ('march','april'):
                connection.execute(f'''CREATE TABLE {month}(context_id VARCHAR,event_datetime TIMESTAMP_NS,event_name VARCHAR,
                    journey_stage VARCHAR,journey_substage VARCHAR,channel VARCHAR,partner_id VARCHAR,
                    application_id VARCHAR,product_id VARCHAR,session_id VARCHAR)''')
            for index in range(12):
                for i,row in enumerate(history_fixture(index,8+index)):
                    month = 'march' if index%2==0 and i%2==0 else 'april'
                    at = row['event_datetime'].replace('2026-04','2026-03') if month=='march' else row['event_datetime']
                    columns = ('context_id','event_name','journey_stage','journey_substage','channel','partner_id','application_id','product_id','session_id')
                    connection.execute(f'INSERT INTO {month} VALUES (?,CAST(? AS TIMESTAMP_NS),?,?,?,?,?,?,?,?)',
                                       [row['context_id'],at]+[row[key] for key in columns[1:]])
            first,_ = select_cohort(connection)
            connection.execute("INSERT INTO april SELECT context_id,TIMESTAMP '2026-04-20',event_name,journey_stage,journey_substage,channel,partner_id,application_id,'FUTURE-PRODUCT',session_id FROM april")
            second,_ = select_cohort(connection)
            self.assertEqual([(i['alias'],digest(i['history'])) for i in first],[(i['alias'],digest(i['history'])) for i in second])
            self.assertEqual(len({item['candidate']['context_id'] for item in first}),10)

    def test_full_schedule_once_per_qwen_context_same_cohort_all_arms(self):
        selected = [item_fixture(i) for i in range(10)]
        policy = {'rule':'synthetic test cohort'}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            service_count = 0
            def qwen(source,base,destination):
                summary = {'highlights':[{'event_index':0,'fact':event_statement(source['recent_events'][0])}]}
                return summary,{'input_tokens':100,'output_tokens':20,'latency_ms':5}
            def service(base,path,payload=None):
                nonlocal service_count
                if path=='/v1/systemone':
                    service_count += 1
                    return valid_raw(),70000 if service_count==1 else 10
                return {},1
            with patch.object(run_benchmark,'ROOT',root), patch.object(sys,'argv',['run_benchmark.py']), \
                    patch('run_benchmark.protected_hashes',return_value={'original':'hash'}), \
                    patch('run_benchmark.file_digest',return_value='hash'), \
                    patch('run_benchmark.input_audit',return_value='Audit of synthetic test input'), \
                    patch('run_benchmark.connect_inputs',side_effect=lambda path:duckdb.connect()), \
                    patch('run_benchmark.select_cohort',return_value=(selected,policy)), \
                    patch('run_benchmark.model_metadata',return_value={'laya_context_window':8192}), \
                    patch('run_benchmark.summarize_events',side_effect=qwen) as summaries, \
                    patch('run_benchmark.request_json',side_effect=service) as http, \
                    contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(run_benchmark.main(),0)
            self.assertEqual(summaries.call_count,10)
            calls = [call.args[2] for call in http.call_args_list if call.args[1]=='/v1/systemone']
            self.assertEqual(len(calls),60)
            csv_text = (root/'outputs/benchmark/results.csv').read_text()
            self.assertNotIn(b'\r\r\n',(root/'outputs/benchmark/results.csv').read_bytes())
            self.assertNotIn('PRIVATE-',csv_text)
            self.assertNotIn('PRIVATE-',(root/'outputs/benchmark/report.md').read_text())
            self.assertIn('Timing anomaly',(root/'outputs/benchmark/report.md').read_text())
            self.assertIn('70000.000',(root/'outputs/benchmark/report.md').read_text())
            self.assertNotIn('PRIVATE-',stdout.getvalue())
            import csv
            results = list(csv.DictReader(io.StringIO(csv_text)))
            expected = {f'CTX{i:02d}' for i in range(1,11)}
            for strategy in STRATEGIES:
                self.assertEqual({row['context_alias'] for row in results if row['strategy']==strategy},expected)
            for alias in expected:
                for strategy in ('B','C'):
                    pair = [row for row in results if row['context_alias']==alias and row['strategy']==strategy]
                    self.assertEqual(pair[0]['state_sha256'],pair[1]['state_sha256'])
                    self.assertEqual(pair[0]['history_sha256'],pair[1]['history_sha256'])


if __name__=='__main__':
    unittest.main()
