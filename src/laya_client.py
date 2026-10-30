"""Named System One choice request and strict local action validation."""
import json
import math
from http_client import request_json

ACTION_CATALOG = {
    'SMS_REMINDER': {'channel': 'sms', 'theme': 'gentle_reminder'},
    'WA_SUPPORT': {'channel': 'whatsapp', 'theme': 'payment_support'},
    'EMAIL_DETAILS': {'channel': 'email', 'theme': 'balance_explanation'},
}


def build_request(state):
    payload = {'model': 'laya', 'state': state, 'questions': {'action': {
        'type': 'choice',
        'instructions': 'Choose the most suitable synthetic channel/theme using observed engagement and explicit note preferences. Select only a catalog action; do not choose SEND/WAIT, timing or budget.',
        'criteria': {key: f"{value['channel']}: {value['theme']}" for key, value in ACTION_CATALOG.items()},
    }}}
    # UTF-8 bytes are a conservative budget for this small, predominantly ASCII input.
    # Reject oversize requests instead of relying on server truncation.
    if len(json.dumps(payload, ensure_ascii=False).encode('utf-8')) > 6000:
        raise ValueError('Laya request exceeds conservative 6000-byte budget; no input was truncated')
    return payload


def validate_answer(raw):
    answer = raw['answers']['action']
    selected = answer['choice']
    if not isinstance(selected, str) or selected not in ACTION_CATALOG:
        raise ValueError(f'Laya returned invalid action: {selected!r}')
    probabilities = answer['probabilities']
    if not isinstance(probabilities, dict) or set(probabilities) != set(ACTION_CATALOG):
        raise ValueError('Laya probabilities must cover exactly the action catalog')
    if any(type(p) not in (int, float) or not math.isfinite(p) or not 0 <= p <= 1
           for p in probabilities.values()):
        raise ValueError('Invalid choice probability')
    if not math.isclose(sum(probabilities.values()), 1, abs_tol=0.002):
        raise ValueError('Choice probabilities do not sum to one')
    if probabilities[selected] < max(probabilities.values()):
        raise ValueError('Selected choice disagrees with probabilities')
    return {'action_id': selected, **ACTION_CATALOG[selected], 'model_probabilities': probabilities}


def decide(payload, base_url):
    raw, latency = request_json(base_url, '/v1/systemone', payload)
    result = validate_answer(raw)
    result['metrics'] = {'model': 'laya', 'input_tokens': raw.get('usage', {}).get('input_tokens'),
                         'latency_ms': latency}
    result['raw_laya_response'] = raw
    return result
