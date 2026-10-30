"""Run from any working directory: python src/run_one.py."""
import argparse
import json
import os
from pathlib import Path
import sys
import time
from http_client import request_json
from laya_client import build_request, decide
from qwen_summary import summarize_note
from state_builder import build_state

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding='utf-8')
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=ROOT / 'input/sample_customer.json')
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'outputs')
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Clear only the three generated artifacts so a failed rerun cannot look successful.
    for name in ('customer_state.json', 'laya_request.json', 'decision.json'):
        (args.output_dir / name).unlink(missing_ok=True)
    start = time.perf_counter()
    try:
        base_url = os.environ.get('OLLAMA_BASE_URL', 'http://127.0.0.1:11434')
        tags, _ = request_json(base_url, '/api/tags', timeout=10)
        names = {model['name'] for model in tags['models']}
        if not {'qwen2.5:1.5b', 'laya:latest'} <= names:
            raise RuntimeError(f'Required models missing; installed: {sorted(names)}. No downloads attempted.')
        customer = json.loads(args.input.read_text(encoding='utf-8-sig'))
        state = build_state(customer)
        facts, qwen_metrics = summarize_note(customer['customer_note']['text'], base_url)
        state['note_facts'] = facts['facts']
        save(args.output_dir / 'customer_state.json', state)
        payload = build_request(state)
        save(args.output_dir / 'laya_request.json', payload)
        decision = decide(payload, base_url)
        decision.update(status='success', customer_id=state['customer_id'], as_of=state['as_of'],
                        synthetic=True, probability_meaning='Model choice probabilities, not PTP probabilities',
                        qwen_metrics=qwen_metrics, total_latency_ms=round((time.perf_counter()-start)*1000, 3))
        save(args.output_dir / 'decision.json', decision)
        print(json.dumps(decision, indent=2))
        return 0
    except (ValueError, RuntimeError, KeyError, TypeError, OSError) as exc:
        failure = {'status': 'error', 'error': f'{type(exc).__name__}: {exc}'}
        save(args.output_dir / 'decision.json', failure)
        print(json.dumps(failure), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
