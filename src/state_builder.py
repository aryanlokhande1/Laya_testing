"""Deterministic as-of features; future observations are rejected, never truncated."""
from collections import Counter
from datetime import datetime


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timestamps must include a timezone')
    return result


def build_state(customer):
    if customer.get('synthetic') is not True:
        raise ValueError('This POC accepts synthetic records only')
    as_of = timestamp(customer['as_of'])
    if type(customer['days_past_due']) is not int or customer['days_past_due'] < 0:
        raise ValueError('days_past_due must be a nonnegative integer')
    if customer['outstanding_balance_band'] not in {'0-1000', '1001-5000', '5001-10000', '10000+'}:
        raise ValueError('Unknown synthetic balance band')

    def history(name):
        rows = customer[name]
        for row in rows:
            if timestamp(row['timestamp']) > as_of:
                raise ValueError(f'{name}: future observation is not allowed')
        return sorted(rows, key=lambda row: timestamp(row['timestamp']))

    interactions = history('interactions')
    for row in interactions:
        if row['channel'] not in {'sms', 'whatsapp', 'email'}:
            raise ValueError('Unknown channel')
        if set(row) != {'timestamp', 'channel', 'status'}:
            raise ValueError('Unexpected interaction fields')
    ptp = history('ptp_history')
    for row in ptp:
        if set(row) != {'timestamp', 'promised_date', 'status'}:
            raise ValueError('Unexpected PTP fields')
        if row['status'] not in {'kept', 'broken', 'pending'}:
            raise ValueError('Unknown PTP status')
        if row['status'] != 'pending' and datetime.fromisoformat(row['promised_date']).date() > as_of.date():
            raise ValueError('Future PTP outcome is not allowed')
    partner = history('partner_aip_history')
    for row in partner:
        if set(row) != {'timestamp', 'partner', 'aip'}:
            raise ValueError('Unexpected partner/AIP fields')
    events = history('events')
    for row in events:
        if set(row) != {'timestamp', 'event'}:
            raise ValueError('Unexpected event fields')
    note = customer['customer_note']
    if set(note) != {'timestamp', 'text'} or timestamp(note['timestamp']) > as_of:
        raise ValueError('Invalid or future note')
    if not isinstance(note['text'], str) or not note['text'].strip():
        raise ValueError('Customer note must be nonempty text')
    return {
        'customer_id': customer['customer_id'], 'synthetic': True,
        'as_of': customer['as_of'], 'days_past_due': customer['days_past_due'],
        'outstanding_balance_band': customer['outstanding_balance_band'],
        'channel_counts': {channel: dict(Counter(r['status'] for r in interactions if r['channel'] == channel))
                           for channel in ('sms', 'whatsapp', 'email')},
        'interactions': interactions, 'ptp_history': ptp,
        'ptp_counts': dict(Counter(row['status'] for row in ptp)),
        'partner_aip_history': partner, 'events': events,
        'note_timestamp': note['timestamp'],
    }
