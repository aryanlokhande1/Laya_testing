"""Contract-gated numerical formulas. No business parameter has a default.

These functions consume verified message-level observations, not unmatched event
rows. The present source has no approved matching contract, so the build publishes
SQL NULL for matched measurements and affinity; these functions remain reusable.
"""
from datetime import datetime
import math
from statistics import median
from typing import Iterable


def _finite(value: float, name: str) -> float:
    if isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f'{name} must be finite')
    return value


def _count(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer')
    return value


def matched_time_to_open_hours(sent_at: datetime | None, opened_at: datetime | None,
                               *, linkage_verified: bool) -> float | None:
    """Elapsed hours for an independently matched send/open; never match by time."""
    if not linkage_verified or sent_at is None or opened_at is None:
        return None
    delay = (opened_at - sent_at).total_seconds() / 3600
    if delay < 0:
        raise ValueError('An open cannot precede its matched send')
    return delay


def channel_median_time_to_open_hours(delays: Iterable[float | None], *,
                                      linkage_verified: bool) -> float | None:
    """Median of observed, verified per-message delays; missing delays are absent."""
    if not linkage_verified:
        return None
    values = [float(_finite(d, 'delay')) for d in delays if d is not None]
    if any(d < 0 for d in values):
        raise ValueError('Delays must be nonnegative')
    return median(values) if values else None


def matched_open_rate(opened_messages: int, eligible_sends: int, *,
                      linkage_verified: bool, observation_window_complete: bool) -> float | None:
    """Unique opened / eligible sent messages, only with verified complete exposure."""
    _count(opened_messages, 'opened_messages')
    _count(eligible_sends, 'eligible_sends')
    if opened_messages > eligible_sends:
        raise ValueError('Unique opened messages cannot exceed eligible sends')
    if not linkage_verified or not observation_window_complete or eligible_sends == 0:
        return None
    return opened_messages / eligible_sends


def delay_decay(delay_hours: float, *, half_life_hours: float) -> float:
    """2**(-delay / half_life): increasing delay decreases timeliness."""
    _finite(delay_hours, 'delay_hours')
    _finite(half_life_hours, 'half_life_hours')
    if delay_hours < 0 or half_life_hours <= 0:
        raise ValueError('Delay must be nonnegative and half-life positive')
    return 2 ** (-delay_hours / half_life_hours)


def sparse_observation_smoothing(observed_score: float | None, observations: int, *,
                                 prior_score: float, prior_strength: float) -> float | None:
    """(n*score + strength*prior)/(n+strength); no value without observations."""
    _count(observations, 'observations')
    _finite(prior_score, 'prior_score')
    _finite(prior_strength, 'prior_strength')
    if not 0 <= prior_score <= 1 or prior_strength < 0:
        raise ValueError('Prior must be in [0,1], strength nonnegative')
    if observed_score is None or observations == 0:
        return None
    _finite(observed_score, 'observed_score')
    if not 0 <= observed_score <= 1:
        raise ValueError('Score must be in [0,1]')
    return (observations * observed_score + prior_strength * prior_score) / (observations + prior_strength)


def opening_weighted_channel_affinity(opened_messages: int, eligible_sends: int,
                                      delays_hours: Iterable[float], *, opening_weight: float,
                                      half_life_hours: float, prior_score: float,
                                      prior_strength: float, linkage_verified: bool,
                                      observation_window_complete: bool) -> float | None:
    """Smooth r*(w + (1-w)*mean(decay(delay))) using eligible-send count.

    w must exceed 0.5: opening is primary, timeliness secondary. Supply exactly
    one delay per unique matched opened message. No channel cost enters this score.
    Weights, half-life and prior are caller-approved inputs, never inferred here.
    """
    _finite(opening_weight, 'opening_weight')
    if not 0.5 < opening_weight <= 1:
        raise ValueError('Opening weight must be >0.5 and <=1')
    delay_decay(0, half_life_hours=half_life_hours)
    sparse_observation_smoothing(None, 0, prior_score=prior_score, prior_strength=prior_strength)
    rate = matched_open_rate(opened_messages, eligible_sends, linkage_verified=linkage_verified,
                             observation_window_complete=observation_window_complete)
    if rate is None:
        return None
    delays = list(delays_hours)
    if len(delays) != opened_messages:
        raise ValueError('One verified delay is required per unique opened message')
    timeliness = sum(delay_decay(d, half_life_hours=half_life_hours) for d in delays) / len(delays) if delays else 0
    score = rate * (opening_weight + (1 - opening_weight) * timeliness)
    return sparse_observation_smoothing(score, eligible_sends, prior_score=prior_score, prior_strength=prior_strength)
