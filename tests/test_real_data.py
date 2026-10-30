"""Synthetic in-memory fixtures only; no customer records in tests."""
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from real_data import (build_real_state, event_sentences, validate_event_summary,
                       select_history, file_digest, connect_inputs, FILES, relationship_profile)
from run_real import summarize_events
import run_real


def event(at='2026-04-14 12:00:00', name='campaign_sent', channel='SMS', month='april'):
    return {'context_id': 'PRIVATE-CONTEXT-TEST', 'event_datetime': at, 'event_name': name,
            'journey_stage': 'Test stage', 'journey_substage': 'Test substage',
            'channel': channel, 'partner_id': 'PRIVATE-PARTNER-TEST',
            'application_id': 'PRIVATE-APPLICATION-TEST', 'session_id': 'PRIVATE-SESSION-TEST',
            'product_id': 'PRIVATE-PRODUCT-TEST', 'source_month': month}


class RealDataTests(unittest.TestCase):
    def test_strict_cutoff_and_nanoseconds(self):
        for at in ('2026-04-15 00:00:00', '2026-04-15 00:00:00.000000001', '2026-04-16 00:00:00'):
            with self.subTest(at=at), self.assertRaisesRegex(ValueError, 'cutoff'):
                build_real_state([event(at)])
        state, _ = build_real_state([event('2026-04-14 23:59:59.999999999')])
        self.assertEqual(state['last_at'], '2026-04-14 23:59:59.999999999')

    def test_mixed_context_and_cross_month_guards(self):
        other = event()
        other['context_id'] = 'OTHER-PRIVATE-CONTEXT'
        with self.assertRaisesRegex(ValueError, 'Mixed contexts'):
            build_real_state([event(), other])
        with self.assertRaisesRegex(ValueError, 'linkage'):
            build_real_state([event(month='march')])

    def test_deterministic_counts_coverage_and_no_ids(self):
        history = [event(f'2026-04-{day:02d} 12:00:00', 'campaign_clicked' if day % 2 else 'campaign_sent')
                   for day in range(1, 11)]
        original = copy.deepcopy(history)
        state, aliases = build_real_state(list(reversed(history)))
        self.assertEqual(state['history_rows'], 10)
        self.assertEqual(state['event_counts'], {'campaign_sent': 5, 'campaign_clicked': 5})
        self.assertEqual(state['sequence_coverage']['older_rows_in_counts_only'], 2)
        self.assertEqual(len(state['recent_events']), 8)
        self.assertEqual(aliases['partners'], {'P1': 'PRIVATE-PARTNER-TEST'})
        self.assertEqual(len(state['application_sequences']['A1']),10)
        self.assertNotIn('PRIVATE-', json.dumps(state))
        self.assertEqual(history, original)
        for key in ('days_past_due','ptp_history','balance','customer_note'):
            self.assertNotIn(key, state)

    def test_equal_time_journey_ambiguity_and_raw_labels(self):
        state, _ = build_real_state([event(name='AIP', channel='GRCS'), event(name='aip  ok', channel='WA')])
        self.assertEqual(len(state['latest_observed_noncampaign_journey']), 2)
        self.assertEqual(set(state['event_counts']), {'AIP','aip  ok'})
        self.assertEqual(set(state['channel_observations']), {'GRCS','WA'})

    def test_extract_summary_grounding(self):
        sentences = event_sentences(build_real_state([event()])[0])
        valid = {'highlights': [{'event_index': 0, 'fact': sentences[0]}]}
        self.assertEqual(validate_event_summary(json.dumps(valid), sentences), valid)
        for bad in ('broken', '{}', '{"highlights":[]}',
                    json.dumps({'highlights': [{'event_index': 0, 'fact': 'Customer will pay'}]}),
                    json.dumps({'highlights': [{'event_index': True, 'fact': sentences[0]}]}),
                    json.dumps({'highlights': valid['highlights'] * 2})):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate_event_summary(bad, sentences)

    def test_qwen_request_bounds_and_validation(self):
        state, _ = build_real_state([event()])
        sentences = event_sentences(state)
        raw = {'done': True, 'message': {'content': json.dumps({'highlights': [{'event_index': 0,'fact': sentences[0]}]})},
               'prompt_eval_count': 45, 'eval_count': 20}
        with tempfile.TemporaryDirectory() as directory, patch('run_real.request_json', return_value=(raw, 123)) as http:
            with contextlib.redirect_stdout(io.StringIO()) as printed:
                summary, metrics = summarize_events(state, 'http://127.0.0.1:11434', Path(directory))
            payload = http.call_args.args[2]
            self.assertFalse(payload['stream'])
            self.assertEqual(payload['format']['properties']['highlights']['maxItems'], 3)
            self.assertEqual(metrics['input_tokens'], 45)
            self.assertEqual(metrics['latency_ms'], 123)
            self.assertEqual(printed.getvalue(), '')
            self.assertNotIn('PRIVATE-', json.dumps(payload))
        with tempfile.TemporaryDirectory() as directory, patch('run_real.request_json', return_value=({'done': True,'done_reason': 'length'},1)):
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                summarize_events(state, 'http://127.0.0.1:11434', Path(directory))
        state['recent_events'][0]['event'] = 'x' * 7000
        with tempfile.TemporaryDirectory() as directory, patch('run_real.request_json') as http:
            with self.assertRaisesRegex(ValueError, 'nothing truncated'):
                summarize_events(state, 'http://127.0.0.1:11434', Path(directory))
            http.assert_not_called()

    def test_selection_does_not_use_future_rows(self):
        with duckdb.connect() as connection:
            connection.execute('''CREATE TABLE april(context_id VARCHAR,event_datetime TIMESTAMP_NS,event_name VARCHAR,
                journey_stage VARCHAR,journey_substage VARCHAR,channel VARCHAR,partner_id VARCHAR,
                product_id VARCHAR,application_id VARCHAR,session_id VARCHAR)''')
            for i in range(12):
                row = event(f'2026-04-{i+1:02d} 10:00:00', ['campaign_sent','campaign_clicked','AIP','aip  ok','product apply'][i%5], 'SMS' if i%2 else 'WA')
                connection.execute('INSERT INTO april VALUES (?,?,?,?,?,?,?,?,?,?)',
                    [row[key] for key in ('context_id','event_datetime','event_name','journey_stage','journey_substage','channel','partner_id','product_id','application_id','session_id')])
            selected, before = select_history(connection)
            connection.execute("INSERT INTO april SELECT context_id, TIMESTAMP '2026-04-15 00:00:00', 'Disbursed',journey_stage,journey_substage,channel,partner_id,product_id,application_id,session_id FROM april LIMIT 1")
            connection.execute("INSERT INTO april SELECT 'FUTURE-ONLY',TIMESTAMP '2026-04-20 00:00:00',event_name,journey_stage,journey_substage,channel,partner_id,product_id,application_id,session_id FROM april")
            selected_after, after = select_history(connection)
            self.assertEqual(selected, selected_after)
            self.assertEqual(before, after)
            self.assertEqual(len(after),12)

    def test_read_only_parquet_schema_guard(self):
        with tempfile.TemporaryDirectory() as directory, duckdb.connect() as connection:
            for name in FILES.values():
                path = Path(directory) / name
                connection.execute("COPY (SELECT TIMESTAMP '2026-04-01' AS event_datetime) TO ? (FORMAT PARQUET)",[str(path)])
            before = {name: file_digest(Path(directory)/name) for name in FILES.values()}
            with self.assertRaisesRegex(ValueError, 'TIMESTAMP_NS'):
                connect_inputs(directory)
            self.assertEqual(before, {name: file_digest(Path(directory)/name) for name in FILES.values()})

    def test_application_transitions_preserved(self):
        one = event('2026-04-01 00:00:00', 'application created success')
        two = event('2026-04-02 00:00:00', 'AIP')
        two['application_id'] = 'SECOND-PRIVATE-APP'
        two['product_id'] = 'SECOND-PRIVATE-PRODUCT'
        three = event('2026-04-03 00:00:00', 'aip  ok')
        state, aliases = build_real_state([three,two,one])
        self.assertEqual(set(state['application_sequences']), {'A1','A2'})
        self.assertEqual([row[1] for row in state['application_sequences']['A1']], ['application created success','aip  ok'])
        self.assertEqual([row['application'] for row in state['recent_events']], ['A1','A2','A1'])
        self.assertEqual(state['application_unassigned_rows'],0)
        self.assertNotIn('PRIVATE',json.dumps(state))

    def test_cross_month_selection_checks_use_only_history(self):
        with duckdb.connect() as connection:
            for month in ('march','april'):
                connection.execute(f'''CREATE TABLE {month}(context_id VARCHAR,event_datetime TIMESTAMP_NS,event_name VARCHAR,
                    journey_stage VARCHAR,journey_substage VARCHAR,channel VARCHAR,partner_id VARCHAR,
                    product_id VARCHAR,application_id VARCHAR,session_id VARCHAR)''')
                for i in range(6):
                    row = event(f'2026-{3 if month=="march" else 4:02d}-{i+1:02d} 10:00:00',
                                ['campaign_sent','campaign_clicked','AIP','aip  ok','product apply'][i%5], 'SMS' if i%2 else 'WA')
                    connection.execute(f'INSERT INTO {month} VALUES (?,?,?,?,?,?,?,?,?,?)',
                        [row[key] for key in ('context_id','event_datetime','event_name','journey_stage','journey_substage','channel','partner_id','product_id','application_id','session_id')])
            _, history = select_history(connection, link_months=True)
            self.assertEqual(len(history),12)
            evidence = relationship_profile(connection)
            self.assertEqual(evidence['strictly_before_cutoff']['shared_unique_context_product'],1)
            connection.execute("INSERT INTO april SELECT 'OTHER-CONTEXT',TIMESTAMP '2026-04-20',event_name,journey_stage,journey_substage,channel,partner_id,'CHANGED-PRODUCT',application_id,session_id FROM april LIMIT 1")
            self.assertEqual(select_history(connection,link_months=True)[1], history)
            self.assertEqual(relationship_profile(connection)['full_months']['shared_multiple_contexts'],1)
            connection.execute("UPDATE april SET event_datetime=TIMESTAMP '2026-04-10' WHERE context_id='OTHER-CONTEXT'")
            with self.assertRaisesRegex(ValueError,'No context meets'):
                select_history(connection,link_months=True)

    def test_failure_is_sanitized_and_synthetic_outputs_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'input').mkdir()
            (root/'outputs').mkdir()
            for name in FILES.values():
                (root/'input'/name).write_bytes(b'synthetic test placeholder')
            original = b'{"status":"synthetic result preserved"}'
            (root/'outputs/decision.json').write_bytes(original)
            with patch.object(run_real,'ROOT',root), patch.object(sys,'argv',['run_real.py']), \
                    patch('run_real.connect_inputs',side_effect=RuntimeError('PRIVATE-CUSTOMER-ERROR')), \
                    contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
                self.assertEqual(run_real.main(),1)
            self.assertNotIn('PRIVATE-CUSTOMER',stdout.getvalue()+stderr.getvalue())
            self.assertEqual((root/'outputs/decision.json').read_bytes(),original)
            self.assertEqual(json.loads((root/'outputs/real/decision.json').read_text())['status'],'error')


if __name__ == '__main__':
    unittest.main()
