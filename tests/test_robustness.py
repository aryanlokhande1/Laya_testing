"""Synthetic-only robustness controls, timeout classification and local slow-server test."""
import copy
from http.server import BaseHTTPRequestHandler,HTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from benchmark_inputs import strategy_state
from test_benchmark import item_fixture,valid_raw
from robustness_inputs import compact_state,reconstruct_baseline,reordered_request,schedule,ALIASES
from robustness_runtime import supervise,timed_out_exception
from run_robustness import result_metrics,summarize_groups
from benchmark_inputs import laya_request
from robustness_analysis import option_effects,standby_intervals


class RobustnessTests(unittest.TestCase):
    def test_full_schedule_repeats_all_six_permutations(self):
        planned=schedule()
        self.assertEqual(len(planned),105)
        for alias in ALIASES:
            rows=[row for row in planned if row[0]==alias and row[1]=='original']
            self.assertEqual(len(rows),18)
            self.assertEqual(len({row[4] for row in rows}),6)
            for order in range(6):
                self.assertEqual({row[3] for row in rows if row[2]==order},{1,2,3})

    def test_permutations_preserve_everything_except_order(self):
        original=laya_request({'observed':'fixture'})
        for _,rep,_,_,order in schedule():
            modified=reordered_request(original,order)
            self.assertEqual(modified,original)
            self.assertEqual(list(modified['questions']['action']['criteria']),list(order))
        with self.assertRaises(ValueError): reordered_request(original,['SMS_REMINDER']*3)

    def test_compaction_roundtrip_nulls_counts_events_and_no_mutation(self):
        item=item_fixture(count=25)
        state=strategy_state(item['structured'],item['recent20'],'B')
        before=copy.deepcopy(state)
        compact,ledger=compact_state(state,item['history'],item['aliases'])
        reconstructed=reconstruct_baseline(compact,state['structured']['identity'],state['structured']['limitations'])
        self.assertEqual(reconstructed,state)
        self.assertEqual(state,before)
        self.assertEqual(len(compact['recent_event_indices']),8)
        self.assertEqual(compact['sequence_coverage']['older_rows_not_in_sequence'],17)
        self.assertEqual(compact['event_counts'],item['structured']['event_counts'])
        self.assertEqual(compact['channels']['SMS'][2],0)
        self.assertIsNone(compact['channels']['SMS'][6])
        self.assertNotIn('PRIVATE-',json.dumps(compact))
        self.assertIn('grounded_additions',ledger)

    def test_no_event_occurrences_dropped_or_future_rows_accepted(self):
        item=item_fixture(count=12)
        state=strategy_state(item['structured'],item['recent20'],'B')
        history=copy.deepcopy(item['history'])
        history[-1]['event_datetime']='2026-04-15 00:00:00'
        with self.assertRaises(ValueError): compact_state(state,history,item['aliases'])
        compact,_=compact_state(state,item['history'],item['aliases'])
        self.assertEqual(len(set(compact['recent_event_indices'])),8)

    def test_top_two_margin_and_invalid_action(self):
        metrics=result_metrics(valid_raw())
        self.assertAlmostEqual(metrics['top_two_margin'],.3)
        raw=valid_raw(); raw['answers']['action']['choice']='WAIT'
        with self.assertRaises(ValueError): result_metrics(raw)

    def test_identical_variation_separated_from_order_variation(self):
        results=[]
        for order in range(6):
            for repeat in range(3):
                raw=valid_raw()
                if order:
                    raw['answers']['action']['choice']='WA_SUPPORT'
                    raw['answers']['action']['probabilities']={'SMS_REMINDER':.3,'WA_SUPPORT':.6,'EMAIL_DETAILS':.1}
                results.append({'context_alias':'CTX01','representation':'original','order_index':order,
                    'status':'success',**result_metrics(raw)})
        groups=[g for g in summarize_groups(results) if g['alias']=='CTX01' and g['representation']=='original']
        self.assertTrue(all(g['probability_spread']==0 for g in groups))
        self.assertNotEqual(groups[0]['actions'],groups[1]['actions'])
        effect=option_effects(results)[0]
        self.assertEqual(effect['max_identical_request_spread'],0)
        self.assertAlmostEqual(effect['max_between_order_mean_spread'],.3)

    def test_power_events_require_actual_utc_window(self):
        events=[{'time_created_utc':'2026-10-28T13:00:00Z','message_first_line':'The system is entering Modern Standby'},
                {'time_created_utc':'2026-10-28T15:00:00Z','message_first_line':'The system is exiting Modern Standby'}]
        spans=standby_intervals(events,'2026-10-28T12:00:00Z','2026-10-28T16:00:00Z')
        self.assertEqual(spans[0]['duration_seconds'],7200)
        with self.assertRaises(ValueError): standby_intervals(events,'2026-10-28T14:00:00Z','2026-10-28T16:00:00Z')

    def test_late_valid_response_is_never_success(self):
        with tempfile.TemporaryDirectory() as directory:
            result=Path(directory)/'result.json'; result.write_text(json.dumps({'status':'response','response':valid_raw()}))
            with patch('robustness_runtime.subprocess.Popen') as popen,patch('robustness_runtime.time.perf_counter',side_effect=[0,46]):
                popen.return_value.pid=999
                outcome=supervise(Path(directory)/'request.json',result,'http://127.0.0.1:11434',30,45)
            self.assertEqual(outcome['status'],'deadline_exceeded')
            self.assertTrue(outcome['late_response_retained'])
            self.assertNotIn('response',outcome)

    def test_bounded_timeout_actual_local_slow_server(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                time.sleep(1)
                try:
                    self.send_response(200); self.end_headers(); self.wfile.write(b'{}')
                except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError): pass
            def log_message(self,*args): pass
        server=HTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                request=Path(directory)/'request.json'; request.write_text('{}')
                outcome=supervise(request,Path(directory)/'response.json',f'http://127.0.0.1:{server.server_port}',.1,5)
                self.assertEqual(outcome['status'],'http_timeout')
                self.assertLess(outcome['supervisor_elapsed_ms'],5000)
        finally: server.shutdown(); server.server_close(); thread.join(timeout=2)

    def test_timeout_chain_and_deadline_arguments(self):
        try:
            try: raise TimeoutError('synthetic timeout')
            except TimeoutError as exc: raise RuntimeError('wrapped') from exc
        except RuntimeError as exc: self.assertTrue(timed_out_exception(exc))
        with self.assertRaises(ValueError): supervise('a','b','http://127.0.0.1:11434',30,30)


if __name__=='__main__': unittest.main()
