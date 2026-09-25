from __future__ import annotations

import shutil
import subprocess
import time
from collections.abc import Iterator

import pytest
from pydantic import ValidationError
import redis
from redis.exceptions import ResponseError

from signaldesk_streams_kit import (
    DeadLetterReason,
    ack_if_owned,
    canonical_event_json,
    dead_letter_if_owned,
    ensure_consumer_group,
    parse_stream_event,
    reclaim_stale_pending,
)
from test_streams import event


@pytest.fixture(scope="module")
def real_redis() -> Iterator[redis.Redis[str]]:
    if shutil.which("docker") is None:
        pytest.skip("Docker is unavailable")
    container: str | None = None
    try:
        container = subprocess.run(
            ["docker", "run", "--rm", "-d", "-P", "redis:7-alpine"],
            check=True,
            capture_output=True,
            text=True,
            timeout=45,
        ).stdout.strip()
        port = subprocess.run(
            ["docker", "port", container, "6379/tcp"],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        ).stdout.strip().rsplit(":", 1)[1]
    except (OSError, subprocess.SubprocessError):
        if container:
            subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True, timeout=30)
        pytest.skip("Docker daemon or redis:7-alpine is unavailable")
    client = redis.Redis(host="127.0.0.1", port=int(port), decode_responses=True)
    try:
        for _ in range(40):
            try:
                client.ping()
                break
            except redis.ConnectionError:
                time.sleep(0.1)
        else:
            pytest.skip("temporary Redis did not become ready")
        yield client
    finally:
        try:
            client.close()
        finally:
            if container:
                subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True, timeout=30)


def _pending_entry(client: redis.Redis[str], stream: str, group: str, consumer: str) -> tuple[str, dict[str, str]]:
    contract_event = event()
    entry_id = client.xadd(
        stream,
        {"event": canonical_event_json(contract_event), "event_id": str(contract_event.event_id)},
    )
    result = client.xreadgroup(group, consumer, {stream: ">"}, count=1)
    return entry_id, result[0][1][0][1]


def _stored_fields(client: redis.Redis[str], stream: str, fields: dict[str, str]) -> dict[str, str]:
    entry_id = client.xadd(stream, fields)
    return client.xrange(stream, min=entry_id, max=entry_id)[0][1]


def test_real_redis_stream_envelope_requires_exact_matching_uuid_fields(real_redis: redis.Redis[str]) -> None:
    stream = "events:envelope"
    encoded = canonical_event_json(event())
    valid = {"event": encoded, "event_id": str(event().event_id)}
    assert parse_stream_event(_stored_fields(real_redis, stream, valid)) == event()

    invalid_envelopes = [
        ({"event": encoded, "event_id": "00000000-0000-4000-8000-000000000099"}, ValueError),
        ({"event": encoded, "event_id": "not-a-uuid"}, ValidationError),
        ({"event": encoded, "event_id": "0" * 37}, ValidationError),
        ({"event": encoded}, ValidationError),
        ({**valid, "credential": "must-not-pass"}, ValidationError),
    ]
    for fields, error in invalid_envelopes:
        with pytest.raises(error):
            parse_stream_event(_stored_fields(real_redis, stream, fields))


def test_real_redis_group_creation_and_stale_reclaim_are_fenced(real_redis: redis.Redis[str]) -> None:
    stream, group = "events:reclaim", "workers"
    assert ensure_consumer_group(real_redis, stream, group)
    assert not ensure_consumer_group(real_redis, stream, group)
    entry_id, fields = _pending_entry(real_redis, stream, group, "first")
    reclaimed = reclaim_stale_pending(real_redis, stream, group, "second", min_idle_ms=1)
    if not reclaimed.entries:
        time.sleep(0.01)
        reclaimed = reclaim_stale_pending(real_redis, stream, group, "second", min_idle_ms=1)
    assert [(entry.id, entry.fields) for entry in reclaimed.entries] == [(entry_id, fields)]
    assert parse_stream_event(fields) == event()
    assert not ack_if_owned(real_redis, stream, group, entry_id, "first")
    assert ack_if_owned(real_redis, stream, group, entry_id, "second")
    assert not ack_if_owned(real_redis, stream, group, entry_id, "second")


def test_real_redis_owned_dlq_is_atomic_sanitized_and_stale_owner_is_refused(real_redis: redis.Redis[str]) -> None:
    stream, group, dlq = "events:dlq", "workers", "events:dlq:dead"
    ensure_consumer_group(real_redis, stream, group)
    entry_id, fields = _pending_entry(real_redis, stream, group, "owner")
    assert dead_letter_if_owned(real_redis, stream, group, entry_id, "owner", dlq, DeadLetterReason.IMPOSSIBLE_STATE, fields)
    assert real_redis.xpending_range(stream, group, "-", "+", 10) == []
    dlq_fields = real_redis.xrange(dlq)[0][1]
    assert set(dlq_fields) == {
        "reason_code", "original_stream", "original_message_id", "event_type", "event_id", "organization_id", "correlation_id",
    }
    assert dlq_fields["reason_code"] == "impossible_state"
    assert "event" not in dlq_fields and "token" not in dlq_fields and "secret" not in dlq_fields

    stale_id, stale_fields = _pending_entry(real_redis, stream, group, "old-owner")
    real_redis.xautoclaim(stream, group, "new-owner", min_idle_time=0, start_id="0-0", count=10)
    assert not dead_letter_if_owned(real_redis, stream, group, stale_id, "old-owner", dlq, DeadLetterReason.MALFORMED_EVENT, stale_fields)
    assert len(real_redis.xpending_range(stream, group, "-", "+", 10)) == 1
    assert len(real_redis.xrange(dlq)) == 1


def test_real_redis_dlq_failure_before_completion_leaves_entry_pending(real_redis: redis.Redis[str]) -> None:
    stream, group, dlq = "events:failure", "workers", "events:failure:dead"
    ensure_consumer_group(real_redis, stream, group)
    entry_id, fields = _pending_entry(real_redis, stream, group, "owner")
    real_redis.set(dlq, "wrong-type")
    with pytest.raises(ResponseError):
        dead_letter_if_owned(real_redis, stream, group, entry_id, "owner", dlq, DeadLetterReason.MALFORMED_EVENT, fields)
    pending = real_redis.xpending_range(stream, group, "-", "+", 10)
    assert [item["message_id"] for item in pending] == [entry_id]


def test_real_redis_malformed_event_dlq_never_copies_payload_or_secret(real_redis: redis.Redis[str]) -> None:
    stream, group, dlq = "events:malformed", "workers", "events:malformed:dead"
    ensure_consumer_group(real_redis, stream, group)
    entry_id = real_redis.xadd(
        stream,
        {"event": '{"event_type":"unknown.v1"}', "event_id": "not-a-uuid", "token": "super-secret"},
    )
    fields = real_redis.xreadgroup(group, "owner", {stream: ">"}, count=1)[0][1][0][1]
    assert dead_letter_if_owned(real_redis, stream, group, entry_id, "owner", dlq, DeadLetterReason.MALFORMED_EVENT, fields)
    dlq_fields = real_redis.xrange(dlq)[0][1]
    assert dlq_fields == {
        "reason_code": "malformed_event",
        "original_stream": stream,
        "original_message_id": entry_id,
    }
    assert "event" not in dlq_fields and "event_id" not in dlq_fields


def test_real_redis_dlq_retention_is_exactly_bounded_and_sanitized(real_redis: redis.Redis[str]) -> None:
    stream, group, dlq = "events:retention", "workers", "events:retention:dead"
    ensure_consumer_group(real_redis, stream, group)
    source_ids: list[str] = []
    for _ in range(5):
        entry_id, fields = _pending_entry(real_redis, stream, group, "owner")
        source_ids.append(entry_id)
        assert dead_letter_if_owned(
            real_redis,
            stream,
            group,
            entry_id,
            "owner",
            dlq,
            DeadLetterReason.IMPOSSIBLE_STATE,
            fields,
            dlq_maxlen=3,
        )

    dlq_entries = real_redis.xrange(dlq)
    assert len(dlq_entries) == 3
    assert [fields["original_message_id"] for _, fields in dlq_entries] == source_ids[-3:]
    for _, dlq_fields in dlq_entries:
        assert set(dlq_fields) == {
            "reason_code",
            "original_stream",
            "original_message_id",
            "event_type",
            "event_id",
            "organization_id",
            "correlation_id",
        }
        assert "event" not in dlq_fields and "token" not in dlq_fields and "secret" not in dlq_fields
