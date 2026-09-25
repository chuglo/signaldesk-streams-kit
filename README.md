# signaldesk-streams-kit

**Not for production use.**

Small, strict Redis Streams primitives for SignalDesk Python 3.11 consumers.

`StreamConsumerConfig` validates Redis-safe topology names. Events are serialized
and parsed through `signaldesk-contracts`; stream records are exactly one `event`
field. `ack_if_owned` and `dead_letter_if_owned` fence finalization with an
in-script `XPENDING` ownership check. DLQ records contain only a finite reason,
the original stream/message identifiers, and envelope identifiers safely parsed
from a valid contract event.

`dead_letter_if_owned` retains a maximum of 1,000 DLQ entries by default. It
uses Redis `XADD MAXLEN =`, so the cap is exact: when full, the oldest DLQ
record is dropped as the newest terminal diagnostic is written. This retention
policy applies only to the DLQ; source-stream ACK and stale-reclaim semantics
are unchanged.

Use `reclaim_stale_pending` with a Redis server idle threshold and retain its
returned cursor for subsequent calls.

## License

MIT. See [LICENSE](LICENSE).
