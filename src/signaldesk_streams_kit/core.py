"""Fenced, transport-only Redis Streams helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
from enum import Enum
from typing import Any, Mapping
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from redis.exceptions import ResponseError
from signaldesk_contracts import Event, parse_event_json

_MAX_NAME_LENGTH = 128
_NAME_PATTERN = r"^[A-Za-z0-9._:-]+$"
_SAFE_FRAGMENT = re.compile(r"[^A-Za-z0-9._-]+")
DEFAULT_DLQ_MAXLEN = 1000


class StreamConsumerConfig(BaseModel):
    """Validated Redis stream topology and consumer identity."""

    model_config = ConfigDict(extra="forbid", strict=True)

    stream: str = Field(min_length=1, max_length=_MAX_NAME_LENGTH, pattern=_NAME_PATTERN)
    group: str = Field(min_length=1, max_length=_MAX_NAME_LENGTH, pattern=_NAME_PATTERN)
    dead_letter_stream: str = Field(min_length=1, max_length=_MAX_NAME_LENGTH, pattern=_NAME_PATTERN)
    consumer: str = Field(min_length=1, max_length=_MAX_NAME_LENGTH, pattern=_NAME_PATTERN)

    @model_validator(mode="after")
    def distinct_dead_letter_stream(self) -> "StreamConsumerConfig":
        if self.stream == self.dead_letter_stream:
            raise ValueError("dead_letter_stream must differ from stream")
        return self


class _RawStreamEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event: str
    event_id: str = Field(
        min_length=36,
        max_length=36,
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    )

    @field_validator("event_id")
    @classmethod
    def event_id_is_uuid(cls, value: str) -> str:
        try:
            UUID(value)
        except ValueError as error:
            raise ValueError("event_id must be a UUID") from error
        return value


class DeadLetterReason(str, Enum):
    MALFORMED_EVENT = "malformed_event"
    TENANT_MISMATCH = "tenant_mismatch"
    IMPOSSIBLE_STATE = "impossible_state"
    UNSUPPORTED_EVENT = "unsupported_event"


class ReclaimedEntry(BaseModel):
    """A Redis pending entry claimed by the current consumer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    fields: dict[str, str]


class ReclaimResult(BaseModel):
    """Cursor and entries returned by one `XAUTOCLAIM` call."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    next_start_id: str
    entries: tuple[ReclaimedEntry, ...]


def _text(value: str | bytes | bytearray) -> str:
    if isinstance(value, str):
        return value
    return bytes(value).decode("utf-8")


def process_consumer_name(service: str, instance_id: str | None = None) -> str:
    """Return a Redis-safe, bounded identity unique to this process incarnation."""

    if not isinstance(service, str) or not service.strip():
        raise ValueError("service must be nonempty")
    if instance_id is not None and (not isinstance(instance_id, str) or not instance_id.strip()):
        raise ValueError("instance_id must be nonempty when supplied")
    source = service if instance_id is None else f"{service}:{instance_id}"
    cleaned = _SAFE_FRAGMENT.sub("-", source.strip()).strip(".-:") or "consumer"
    suffix = f":{os.getpid()}:{uuid4().hex}"
    digest = hashlib.blake2s(source.encode("utf-8"), digest_size=6).hexdigest()
    prefix_limit = _MAX_NAME_LENGTH - len(suffix) - len(digest) - 1
    return f"{cleaned[:prefix_limit]}-{digest}{suffix}"


def ensure_consumer_group(redis: Any, stream: str, group: str) -> bool:
    """Create a group from the stream beginning, returning false if it exists."""

    try:
        redis.xgroup_create(stream, group, id="0-0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error).upper():
            raise
        return False
    return True


def canonical_event_json(event: Event) -> str:
    """Serialize a contract event deterministically after strict contract parsing."""

    parsed = parse_event_json(event.model_dump_json())
    return json.dumps(
        parsed.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def parse_stream_event(raw: Mapping[str | bytes, str | bytes | bytearray]) -> Event:
    """Parse the exact two-field stream envelope through the strict event contracts."""

    normalized = {_text(key): _text(value) for key, value in raw.items()}
    payload = _RawStreamEvent.model_validate(normalized)
    event = parse_event_json(payload.event)
    if UUID(payload.event_id) != event.event_id:
        raise ValueError("stream event_id does not match contract event_id")
    return event


def reclaim_stale_pending(
    redis: Any,
    stream: str,
    group: str,
    consumer: str,
    *,
    min_idle_ms: int,
    start_id: str = "0-0",
    count: int = 100,
) -> ReclaimResult:
    """Claim entries idle long enough according to Redis server-side semantics."""

    if min_idle_ms < 1 or count < 1:
        raise ValueError("min_idle_ms and count must be positive")
    reply = redis.xautoclaim(
        stream,
        group,
        consumer,
        min_idle_time=min_idle_ms,
        start_id=start_id,
        count=count,
    )
    next_id, records = reply[0], reply[1]
    entries = tuple(
        ReclaimedEntry(
            id=_text(entry_id),
            fields={_text(key): _text(value) for key, value in fields.items()},
        )
        for entry_id, fields in records
    )
    return ReclaimResult(next_start_id=_text(next_id), entries=entries)


_ACK_SCRIPT = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[3] then return {0, 'ownership_lost'} end
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged ~= 1 then return {0, 'ownership_lost'} end
return {1, 'acknowledged'}
"""

_DLQ_SCRIPT = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[3] then return {0, 'ownership_lost'} end
local fields = {}
for index = 5, #ARGV, 2 do
  table.insert(fields, ARGV[index])
  table.insert(fields, ARGV[index + 1])
end
local dlq_id = redis.call('XADD', KEYS[2], 'MAXLEN', '=', ARGV[4], '*', unpack(fields))
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged ~= 1 then
  redis.call('XDEL', KEYS[2], dlq_id)
  return {0, 'ownership_lost'}
end
return {1, 'dead_lettered'}
"""


def _script_succeeded(reply: Any) -> bool:
    return isinstance(reply, (list, tuple)) and bool(reply) and int(reply[0]) == 1


def ack_if_owned(redis: Any, stream: str, group: str, message_id: str, consumer: str) -> bool:
    """ACK only if `consumer` still owns this exact pending message."""

    return _script_succeeded(redis.eval(_ACK_SCRIPT, 1, stream, group, message_id, consumer))


def _safe_dlq_fields(
    stream: str,
    message_id: str,
    reason: DeadLetterReason,
    raw: Mapping[str | bytes, str | bytes | bytearray],
) -> dict[str, str]:
    fields = {
        "reason_code": reason.value,
        "original_stream": stream,
        "original_message_id": message_id,
    }
    try:
        event = parse_stream_event(raw)
    except (ValidationError, UnicodeDecodeError, ValueError, TypeError):
        return fields
    fields.update(
        {
            "event_type": event.event_type,
            "event_id": str(event.event_id),
            "organization_id": str(event.organization_id),
            "correlation_id": str(event.correlation_id),
        }
    )
    return fields


def _validate_dlq_maxlen(dlq_maxlen: int) -> int:
    if isinstance(dlq_maxlen, bool) or not isinstance(dlq_maxlen, int) or dlq_maxlen < 1:
        raise ValueError("dlq_maxlen must be a positive integer")
    return dlq_maxlen


def dead_letter_if_owned(
    redis: Any,
    stream: str,
    group: str,
    message_id: str,
    consumer: str,
    dead_letter_stream: str,
    reason: DeadLetterReason,
    raw: Mapping[str | bytes, str | bytes | bytearray],
    *,
    dlq_maxlen: int = DEFAULT_DLQ_MAXLEN,
) -> bool:
    """Atomically write a sanitized, exactly capped DLQ record and ACK if still owned.

    Redis ``XADD MAXLEN =`` retains only the newest ``dlq_maxlen`` DLQ records.
    If ownership is lost before the source ACK, the just-added DLQ record is deleted.
    """

    dlq_maxlen = _validate_dlq_maxlen(dlq_maxlen)
    fields = _safe_dlq_fields(stream, message_id, reason, raw)
    arguments: list[str | int] = [group, message_id, consumer, dlq_maxlen]
    for key, value in fields.items():
        arguments.extend((key, value))
    return _script_succeeded(redis.eval(_DLQ_SCRIPT, 2, stream, dead_letter_stream, *arguments))
