"""Offline unit tests; model quality needs a separate labelled evaluation."""
import copy
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
import io

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from http_client import request_json
from laya_client import ACTION_CATALOG, build_request, validate_answer
from qwen_summary import summarize_note, validate_facts
from state_builder import build_state
import run_one


def fixture():
    # Synthetic fixture is deliberately independent of ignored customer input files.
    return {
        'synthetic': True, 'customer_id': 'SYNTH-TEST', 'as_of': '2026-10-27T09:00:00+05:30',
        'days_past_due': 12, 'outstanding_balance_band': '1001-5000',
        'interactions': [
            {'timestamp': '2026-10-24T11:00:00+05:30', 'channel': 'whatsapp', 'status': 'replied'},
            {'timestamp': '2026-10-20T10:00:00+05:30', 'channel': 'sms', 'status': 'delivered'}],
        'ptp_history': [{'timestamp': '2026-10-24T11:00:00+05:30', 'promised_date': '2026-10-29', 'status': 'pending'}],
        'partner_aip_history': [], 'events': [],
        'customer_note': {'timestamp': '2026-10-24T11:00:00+05:30', 'text': 'I prefer WhatsApp.'}}


class PipelineTests(unittest.TestCase):
    def test_state_counts_order_and_no_mutation(self):
        record = fixture()
        original = copy.deepcopy(record)
        state = build_state(record)
        self.assertEqual(state['channel_counts']['sms'], {'delivered': 1})
        self.assertEqual(state['channel_counts']['email'], {})
        self.assertEqual(state['ptp_counts'], {'pending': 1})
        self.assertEqual(state['interactions'][0]['timestamp'], record['interactions'][1]['timestamp'])
        self.assertEqual(record, original)
        self.assertNotIn('customer_note', state)

    def test_future_observation_rejected(self):
        for field in ('interactions', 'ptp_history', 'partner_aip_history', 'events'):
            record = fixture()
            record[field].append({'timestamp': '2026-10-28T09:00:00+05:30'})
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'future'):
                build_state(record)

    def test_future_note_and_ptp_outcome_rejected(self):
        record = fixture()
        record['customer_note']['timestamp'] = '2026-10-28T09:00:00+05:30'
        with self.assertRaises(ValueError):
            build_state(record)
        record = fixture()
        record['ptp_history'][0]['status'] = 'kept'
        with self.assertRaisesRegex(ValueError, 'Future PTP'):
            build_state(record)

    def test_timezone_and_synthetic_guards(self):
        for key, value in [('as_of', '2026-10-27T09:00:00'), ('synthetic', False)]:
            record = fixture()
            record[key] = value
            with self.assertRaises(ValueError):
                build_state(record)

    def test_malformed_or_unsupported_note_json(self):
        for content in ('not json', '[]', '{"facts":{},"count":1}',
                        '{"facts":[{"type":"payment_outcome","quote":"paid"}]}',
                        '{"facts":[{"type":"channel_preference","quote":"Email"}]}'):
            with self.subTest(content=content), self.assertRaises(ValueError):
                validate_facts(content, 'I prefer WhatsApp.')

    def test_grounded_note(self):
        self.assertEqual(validate_facts('{"facts":[{"type":"channel_preference","quote":"I prefer WhatsApp."}]}',
                                        'I prefer WhatsApp.')['facts'][0]['quote'], 'I prefer WhatsApp.')

    def test_qwen_incomplete_response(self):
        with patch('qwen_summary.request_json', return_value=({'done': True, 'done_reason': 'length'}, 1)):
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                summarize_note('I prefer WhatsApp.', 'http://127.0.0.1:11434')

    def test_choice_validation(self):
        raw = {'answers': {'action': {'choice': 'WA_SUPPORT', 'probabilities': {
            'SMS_REMINDER': 0.1, 'WA_SUPPORT': 0.7, 'EMAIL_DETAILS': 0.2}}}}
        self.assertEqual(validate_answer(raw)['channel'], 'whatsapp')
        for invalid in ('SEND', 'WAIT', 'unknown'):
            altered = copy.deepcopy(raw)
            altered['answers']['action']['choice'] = invalid
            with self.assertRaises(ValueError):
                validate_answer(altered)
        for probabilities in ({}, {'SMS_REMINDER': -1, 'WA_SUPPORT': 1, 'EMAIL_DETAILS': 1},
                              {'SMS_REMINDER': 0.1, 'WA_SUPPORT': 0.1, 'EMAIL_DETAILS': 0.1},
                              {'SMS_REMINDER': float('nan'), 'WA_SUPPORT': 0.7, 'EMAIL_DETAILS': 0.3}):
            altered = copy.deepcopy(raw)
            altered['answers']['action']['probabilities'] = probabilities
            with self.assertRaises(ValueError):
                validate_answer(altered)

    def test_contract_and_input_budget(self):
        payload = build_request(build_state(fixture()))
        self.assertEqual(payload['model'], 'laya')
        self.assertEqual(set(payload['questions']), {'action'})
        self.assertEqual(set(payload['questions']['action']['criteria']), set(ACTION_CATALOG))
        with self.assertRaisesRegex(ValueError, 'no input was truncated'):
            build_request({'events': 'x' * 7000})

    def test_missing_service(self):
        with patch('http_client.build_opener') as opener:
            opener.return_value.open.side_effect = URLError('connection refused')
            with self.assertRaisesRegex(RuntimeError, 'Ollama unavailable'):
                request_json('http://127.0.0.1:11434', '/api/tags')

    def test_actual_closed_local_port(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
            with self.assertRaisesRegex(RuntimeError, 'Ollama unavailable'):
                request_json(f'http://127.0.0.1:{port}', '/api/tags', timeout=1)

    def test_http_and_malformed_service_response(self):
        with patch('http_client.build_opener') as opener:
            opener.return_value.open.side_effect = HTTPError('local', 404, 'missing', {}, io.BytesIO(b'endpoint absent'))
            with self.assertRaisesRegex(RuntimeError, '404: endpoint absent'):
                request_json('http://127.0.0.1:11434', '/v1/systemone', {})
        with patch('http_client.build_opener') as opener:
            opener.return_value.open.return_value.__enter__.return_value.read.return_value = b'broken json'
            with self.assertRaisesRegex(ValueError, 'malformed JSON'):
                request_json('http://127.0.0.1:11434', '/api/tags')

    def test_cloud_url_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Only local'):
            request_json('https://example.com', '/api/tags')

    def test_failure_artifact_replaces_previous_success(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            (out / 'decision.json').write_text('{"status":"success"}')
            with patch.object(sys, 'argv', ['run_one.py', '--output-dir', directory]), \
                    patch('run_one.request_json', side_effect=RuntimeError('service missing')), \
                    patch('sys.stderr', new_callable=io.StringIO):
                self.assertEqual(run_one.main(), 1)
            self.assertEqual(json.loads((out / 'decision.json').read_text())['status'], 'error')


if __name__ == '__main__':
    unittest.main()
