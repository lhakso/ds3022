from quixstreams import Application
from collections import Counter
import json
import logging
import os
import time
import redis

"""
Consumes Bluesky events from the `bluesky-events` topic and keeps a running count of each
commit `operation` (create/update/delete) in Redis.

Design:
- Counts live in ONE Redis hash (`bluesky:operations`), so this project's data never mixes
  with other Redis keys and the summary is a single HGETALL.
- Counts are accumulated locally and flushed to Redis in one pipeline every FLUSH_EVERY
  messages or FLUSH_SECONDS, whichever comes first.
- Kafka offsets are committed only AFTER a successful Redis flush (at-least-once): a crash
  re-reads unflushed messages, so counts are never lost (a few may be double counted).
- Totals persist across runs: the consumer group resumes from its last committed offset.
"""

TOPIC = "bluesky-events"
REDIS_KEY = "bluesky:operations"   # Hash: field = operation, value = running count
FLUSH_EVERY = 1000                 # Flush after this many messages...
FLUSH_SECONDS = 1.0                # ...or after this many seconds, whichever comes first

# Log to a local file alongside the script
logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), "bluesky-consumer.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("bluesky-consumer")

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def flush(consumer, pending: Counter, uncommitted: int) -> None:
    """Write pending counts to Redis in one pipeline, then commit Kafka offsets."""
    if not uncommitted:
        return  # Nothing consumed since the last commit
    if pending:
        pipe = redis_client.pipeline()
        for operation, n in pending.items():
            pipe.hincrby(REDIS_KEY, operation, n)
        pipe.execute()
        log.info(f"Flushed {sum(pending.values())} msgs: {dict(pending)}")
        pending.clear()
    # Commit only after Redis has the counts, so a crash never skips uncounted messages
    consumer.commit(asynchronous=False)


def print_counts() -> None:
    """Print the running totals stored in Redis."""
    counts = redis_client.hgetall(REDIS_KEY)
    print(f"\nOperation counts ({REDIS_KEY}):")
    for operation, n in sorted(counts.items(), key=lambda kv: -int(kv[1])):
        print(f"  {operation:<8} {int(n):>12,}")
    print(f"  {'total':<8} {sum(map(int, counts.values())):>12,}")


def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="INFO",
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        consumer_extra_config={"broker.address.family": "v4"},
    )

    pending = Counter()           # Counts not yet written to Redis
    uncommitted = 0               # Messages consumed (incl. malformed) since last commit
    last_flush = time.time()

    # auto_commit_enable=False: we commit manually, after each Redis flush
    with app.get_consumer(auto_commit_enable=False) as consumer:
        consumer.subscribe([TOPIC])
        try:
            while True:
                msg = consumer.poll(1)

                if msg is None:
                    pass  # No new message within the poll timeout
                elif msg.error() is not None:
                    raise Exception(msg.error())
                else:
                    # Count the operation; skip (but log) malformed events
                    try:
                        value = json.loads(msg.value())
                        pending[value["commit"]["operation"]] += 1
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        log.warning(f"Bad message at offset {msg.offset()}: {e}")
                    # Mark as processed locally; it's only sent to Kafka on commit()
                    consumer.store_offsets(msg)
                    uncommitted += 1

                # Flush on size or time, whichever comes first
                if uncommitted >= FLUSH_EVERY or time.time() - last_flush >= FLUSH_SECONDS:
                    flush(consumer, pending, uncommitted)
                    uncommitted, last_flush = 0, time.time()
        finally:
            # On exit (Ctrl-C or error), write what's left before the consumer closes
            try:
                flush(consumer, pending, uncommitted)
            except Exception:
                log.exception("Final flush failed; unflushed messages will be re-read next run")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("Consumer crashed")
        raise
    finally:
        print_counts()
        redis_client.close()
