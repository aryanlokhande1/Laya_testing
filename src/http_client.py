"""Small local-only JSON HTTP client; never downloads models."""
import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, build_opener, ProxyHandler


def request_json(base_url, path, payload=None, timeout=180):
    url = urlparse(base_url)
    if url.scheme != 'http' or url.hostname not in {'127.0.0.1', 'localhost', '::1'}:
        raise ValueError('Only local HTTP Ollama URLs are allowed')
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode('utf-8')
    request = Request(base_url.rstrip('/') + path, data=data,
                      headers={'Content-Type': 'application/json'})
    start = time.perf_counter()
    try:
        # Bypass corporate proxies for local inference.
        with build_opener(ProxyHandler({})).open(request, timeout=timeout) as response:
            result = json.loads(response.read())
    except HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='replace')
        raise RuntimeError(f'Ollama {path}: HTTP {exc.code}: {detail}') from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f'Ollama unavailable at {base_url}{path}: {exc}') from exc
    except (ValueError, UnicodeError) as exc:
        raise ValueError(f'Ollama {path} returned malformed JSON') from exc
    if not isinstance(result, dict):
        raise ValueError(f'Ollama {path} must return a JSON object')
    return result, round((time.perf_counter() - start) * 1000, 3)
