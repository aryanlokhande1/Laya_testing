"""Lossless PostgreSQL binary COPY framing, including embedded CR/LF in text."""
from datetime import datetime
import struct
import re

HEADER=b'PGCOPY\n\xff\r\n\0'+struct.pack('!ii',0,0)
TRAILER=struct.pack('!h',-1)


def binary_record(values, schema) -> bytes:
    if len(values)!=len(schema):
        raise ValueError('COPY column/value count mismatch')
    result=[struct.pack('!h',len(values))]
    for value,(_,kind) in zip(values,schema):
        if value is None:
            result.append(struct.pack('!i',-1))
            continue
        if kind=='VARCHAR':
            data=value.encode('utf-8')
        elif kind=='BIGINT':
            data=struct.pack('!q',value)
        elif kind=='DOUBLE':
            data=struct.pack('!d',value)
        elif kind=='TIMESTAMP_NS':
            if isinstance(value,str) and (fractional:=re.search(r'\.(\d+)',value)) and len(fractional[1])>6:
                raise ValueError('No sub-microsecond timestamp truncation is permitted')
            value=datetime.fromisoformat(value) if isinstance(value,str) else value
            if value.tzinfo is not None:
                raise ValueError('Source timestamp must remain timezone-naive')
            delta=value-datetime(2000,1,1)
            microseconds=delta.days*86400000000+delta.seconds*1000000+delta.microseconds
            data=struct.pack('!q',microseconds)
        else:
            raise ValueError('Unsupported PostgreSQL binary conversion')
        result.extend([struct.pack('!i',len(data)),data])
    return b''.join(result)
