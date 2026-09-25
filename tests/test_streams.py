from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError
from redis.exceptions import ResponseError
from signaldesk_contracts import DiagnosticTerminalV2

from signaldesk_streams_kit import (
    DeadLetterReason,
    StreamConsumerConfig,
    ack_if_owned,
    canonical_event_json,
    dead_letter_if_owned,
    ensure_consumer_group,
    parse_stream_event,
    process_consumer_name,
    reclaim_stale_pending,
)


def event() -> DiagnosticTerminalV2:
    return DiagnosticTerminalV2(
        schema_version=1,
        event_type="diagnostic.terminal.v2",
        event_id=UUID("00000000-0000-4000-8000-000000000001"),
        occurred_at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-4000-8000-000000000002"),
        organization_id=UUID("00000000-0000-4000-8000-000000000003"),
        diagnostic_job_id=UUID("00000000-0000-4000-8000-000000000004"),
        status="failed",
    )


def test_config_is_strict_bounded_and_prevents_dlq_loop() -> None:
    valid = StreamConsumerConfig(
        stream="signaldesk:notifications",
        group="notification-workers",
        dead_letter_stream="signaldesk:notifications:dlq",
        consumer="worker-1",
    )
    assert valid.stream == "signaldesk:notifications"
    with pytest.raises(ValidationError):
        StreamConsumerConfig(stream="", group="g", dead_letter_stream="dlq", consumer="c")
    with pytest.raises(ValidationError):
        StreamConsumerConfig(stream="same", group="g", dead_letter_stream="same", consumer="c")
    with pytest.raises(ValidationError):
        StreamConsumerConfig(stream="s", group="g bad", dead_letter_stream="dlq", consumer="c")
    with pytest.raises(ValidationError):
        StreamConsumerConfig(stream="s" * 129, group="g", dead_letter_stream="dlq", consumer="c")


def test_process_consumer_name_is_safe_bounded_and_incarnated() -> None:
    first = process_consumer_name(" alert worker / ", "instance with spaces")
    second = process_consumer_name(" alert worker / ", "instance with spaces")
    assert first != second
    assert len(first) <= 128
    assert all(char.isalnum() or char in "._:-" for char in first)
    assert ":" in first


def test_canonical_json_is_stable_and_contract_parse_rejects_unknown_data() -> None:
    encoded = canonical_event_json(event())
    assert encoded == canonical_event_json(event())
    assert json.loads(encoded)["event_type"] == "diagnostic.terminal.v2"
    assert parse_stream_event({"event": encoded, "event_id": str(event().event_id)}) == event()


def test_stream_envelope_requires_matching_canonical_event_id() -> None:
    encoded = canonical_event_json(event())

    with pytest.raises(ValidationError):
        parse_stream_event({"event": encoded})
    with pytest.raises(ValidationError):
        parse_stream_event({"event": encoded, "event_id": "not-a-uuid"})
    with pytest.raises(ValidationError):
        parse_stream_event({"event": encoded, "event_id": "0" * 37})
    with pytest.raises(ValueError, match="event_id"):
        parse_stream_event({"event": encoded, "event_id": "00000000-0000-4000-8000-000000000099"})
    with pytest.raises(ValidationError):
        parse_stream_event({"event": encoded, "event_id": str(event().event_id), "credential": "must-not-pass"})
    with pytest.raises(ValidationError):
        parse_stream_event({"event": '{"event_type":"unknown.v1"}', "event_id": str(event().event_id)})


class GroupRedis:
    def __init__(self, response: Exception | None = None) -> None:
        self.response = response
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def xgroup_create(self, *args: object, **kwargs: object) -> None:
        self.calls.append((args, kwargs))
        if self.response is not None:
            raise self.response


def test_ensure_group_only_ignores_busygroup() -> None:
    created = GroupRedis()
    assert ensure_consumer_group(created, "s", "g") is True
    assert created.calls == [(("s", "g"), {"id": "0-0", "mkstream": True})]
    assert ensure_consumer_group(GroupRedis(ResponseError("BUSYGROUP Consumer Group name already exists")), "s", "g") is False
    with pytest.raises(ResponseError, match="NOGROUP"):
        ensure_consumer_group(GroupRedis(ResponseError("NOGROUP topology failure")), "s", "g")


class ScriptRedis:
    def __init__(self, result: object = [1, "ok"], error: Exception | None = None) -> None:
        self.result, self.error = result, error
        self.calls: list[tuple[str, int, object]] = []

    def eval(self, script: str, numkeys: int, *args: object) -> object:
        self.calls.append((script, numkeys, args))
        if self.error:
            raise self.error
        return self.result


def test_ack_and_dlq_interpret_lua_ownership_results_and_propagate_redis_failure() -> None:
    assert ack_if_owned(ScriptRedis([1, "acknowledged"]), "s", "g", "1-0", "worker")
    assert not ack_if_owned(ScriptRedis([0, "ownership_lost"]), "s", "g", "1-0", "worker")
    with pytest.raises(ConnectionError):
        ack_if_owned(ScriptRedis(error=ConnectionError("down")), "s", "g", "1-0", "worker")
    dlq = ScriptRedis([1, "dead_lettered"])
    assert dead_letter_if_owned(
        dlq,
        "s",
        "g",
        "1-0",
        "worker",
        "s:dlq",
        DeadLetterReason.MALFORMED_EVENT,
        {"event": canonical_event_json(event()), "token": "do-not-copy"},
    )
    script, keys, args = dlq.calls[0]
    assert keys == 2 and "XPENDING" in script and "XACK" in script and "MAXLEN" in script
    assert args[2:6] == ("g", "1-0", "worker", 1000)
    assert "do-not-copy" not in args


@pytest.mark.parametrize("dlq_maxlen", [0, -1, 1.0, True, "1000"])
def test_dead_letter_rejects_invalid_dlq_maxlen_before_calling_redis(dlq_maxlen: object) -> None:
    redis = ScriptRedis()
    with pytest.raises(ValueError, match="dlq_maxlen"):
        dead_letter_if_owned(
            redis,
            "s",
            "g",
            "1-0",
            "worker",
            "s:dlq",
            DeadLetterReason.MALFORMED_EVENT,
            {"event": canonical_event_json(event())},
            dlq_maxlen=dlq_maxlen,  # type: ignore[arg-type]
        )
    assert redis.calls == []


def test_reclaim_normalizes_typed_entries() -> None:
    class Redis:
        def xautoclaim(self, *args: object, **kwargs: object) -> tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]], list[bytes]]:
            assert args == ("s", "g", "worker")
            assert kwargs["min_idle_time"] == 500
            return b"2-0", [(b"1-0", {b"event": b"{}"})], []

    result = reclaim_stale_pending(Redis(), "s", "g", "worker", min_idle_ms=500)
    assert result.next_start_id == "2-0"
    assert result.entries[0].id == "1-0"
    assert result.entries[0].fields == {"event": "{}"}
