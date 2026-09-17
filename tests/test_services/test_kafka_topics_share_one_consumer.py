"""A Kafka upload with several topics loads every topic in one run (core#1408).

The builder used to create **one ``KafkaConsumer`` per topic**, all in the connection's group, each
inside its own dlt resource. dlt interleaves resources on one thread. The second consumer's join
started a rebalance, and the first consumer could only complete it by polling, which it could not
do while the thread sat in the second consumer's poll. After the idle timeout the second iterator
ended with no partition assigned, and its topic contributed nothing to a run that finished
`success`. On a real broker, a two-topic upload loaded one topic per run whenever both had unread
messages.

The fake below is a broker and a group, **not** a consumer double. Its one rule about membership
is the constraint that caused the defect: a member that joins while another member of the same
group is open **in this process** cannot complete its join inside its own poll, because nothing
else runs on the thread. Everything else is real: the builder, dlt's scheduling of resources, the
pipeline and a DuckDB destination.

The real-broker reading is QA's class probe arm ``kafka-two-topics-both-waiting`` (core#1419).
"""

import json
from collections import namedtuple

import duckdb
import pytest
from kafka import TopicPartition

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

Message = namedtuple("Message", "topic partition offset timestamp key value")

GROUP = "datanika-consumer"


def _payload(value: dict) -> bytes:
    return json.dumps(value).encode()


class FakeBroker:
    """Topics, their messages, and who else holds partitions in the group."""

    def __init__(self, messages: dict[str, list[dict]], *, held_elsewhere=(), joins=True):
        #: topic -> message values. A topic absent from this dict does not exist on the broker.
        self.messages = messages
        #: topics whose partitions a live member in ANOTHER process holds
        self.held_elsewhere = set(held_elsewhere)
        #: False models a join that does not complete within the idle timeout
        self.joins = joins
        self.open_members: list = []
        self.consumers: list = []

    def consumer_class(self):
        broker = self

        class FakeConsumer:
            def __init__(self, *topics, **config):
                self.topics = topics
                self.config = config
                self._assignment: set[TopicPartition] = set()
                self._joined = False
                self._records = None
                self.closed_with: dict | None = None
                #: messages handed out, and how many had been when the assignment was first read
                self.handed_out = 0
                self.handed_out_when_assignment_read: int | None = None
                broker.consumers.append(self)

            def _join(self):
                self._joined = True
                others_open_here = [m for m in broker.open_members if m is not self]
                broker.open_members.append(self)
                if others_open_here or not broker.joins:
                    return
                self._assignment = {
                    TopicPartition(t, 0)
                    for t in self.topics
                    if t in broker.messages and t not in broker.held_elsewhere
                }

            def assignment(self):
                if self.handed_out_when_assignment_read is None:
                    self.handed_out_when_assignment_read = self.handed_out
                return set(self._assignment)

            def __iter__(self):
                return self

            def __next__(self):
                if self.closed_with is not None:
                    raise StopIteration
                if not self._joined:
                    self._join()
                if self._records is None:
                    self._records = iter(
                        [
                            Message(tp.topic, 0, offset, 1_700_000_000_000, None, _payload(v))
                            for tp in sorted(self._assignment)
                            for offset, v in enumerate(broker.messages[tp.topic])
                        ]
                    )
                record = next(self._records)
                self.handed_out += 1
                return record

            def close(self, autocommit=True, timeout_ms=None):
                self.closed_with = {"autocommit": autocommit}
                if self in broker.open_members:
                    broker.open_members.remove(self)

        return FakeConsumer


def _records(prefix: str, n: int) -> list[dict]:
    return [{"id": f"{prefix}-{i}"} for i in range(n)]


def _run(tmp_path, monkeypatch, broker: FakeBroker, topics: str = "events, orders") -> dict:
    monkeypatch.setattr("kafka.KafkaConsumer", broker.consumer_class())
    return DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=1,
        source_type="kafka",
        source_config={"bootstrap_servers": "broker:9092", "topics": topics},
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config={},
        dataset_name="landed",
        run_id=1,
    )


def _landed(tmp_path) -> dict[str, set[str]]:
    path = tmp_path / "dest.duckdb"
    if not path.exists():
        return {}
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'landed'"
            ).fetchall()
            if not r[0].startswith("_dlt")
        ]
        return {
            t: {r[0] for r in con.execute(f'SELECT id FROM landed."{t}"').fetchall()}  # noqa: S608
            for t in tables
        }
    finally:
        con.close()


def _refusal(exc: BaseException) -> DltRunnerError | None:
    seen = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, DltRunnerError):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return None


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# AC1 / AC2: both topics hold messages, and both load in one run
# ─────────────────────────────────────────────────────────────────────────────────────────────────


def test_two_topics_both_holding_messages_both_load_in_one_run(tmp_path, monkeypatch):
    events, orders = _records("event", 5), _records("order", 7)
    broker = FakeBroker({"events": events, "orders": orders})

    result = _run(tmp_path, monkeypatch, broker)

    assert _landed(tmp_path) == {
        "events": {r["id"] for r in events},
        "orders": {r["id"] for r in orders},
    }
    assert result["rows_loaded"] == 12


def test_one_consumer_is_subscribed_to_every_topic(tmp_path, monkeypatch):
    broker = FakeBroker({"events": _records("event", 1), "orders": _records("order", 1)})

    _run(tmp_path, monkeypatch, broker)

    assert [c.topics for c in broker.consumers] == [("events", "orders")]
    assert broker.consumers[0].config["group_id"] == GROUP


def test_control_one_topic_loads(tmp_path, monkeypatch):
    """The shape that always worked, so the fake is not what fails the two-topic case."""
    events = _records("event", 3)
    broker = FakeBroker({"events": events})

    _run(tmp_path, monkeypatch, broker, topics="events")

    assert _landed(tmp_path) == {"events": {r["id"] for r in events}}


def test_the_provenance_columns_are_on_every_topic_table(tmp_path, monkeypatch):
    """What each per-topic resource declared, still declared on each, dotted topic name included."""
    broker = FakeBroker({"events": _records("event", 2), "orders.v1": _records("order", 2)})

    _run(tmp_path, monkeypatch, broker, topics="events, orders.v1")

    con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
    try:
        for table in ("events", "orders_v1"):
            columns = {
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'landed' AND table_name = ?",
                    [table],
                ).fetchall()
            }
            assert {
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                "_kafka_timestamp",
                "_kafka_key",
            } <= columns, (table, columns)
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# AC3: a topic that was never assigned a partition does not leave the run `success`
# ─────────────────────────────────────────────────────────────────────────────────────────────────


def test_a_topic_held_by_another_member_fails_the_run_before_anything_is_consumed(
    tmp_path, monkeypatch
):
    broker = FakeBroker(
        {"events": _records("event", 4), "orders": _records("order", 4)},
        held_elsewhere={"orders"},
    )

    with pytest.raises(Exception) as failed:  # noqa: B017, PT011 - dlt wraps it; the cause is asserted
        _run(tmp_path, monkeypatch, broker)

    refusal = _refusal(failed.value)
    assert refusal is not None, f"not refused by the builder: {failed.value!r}"
    assert "'orders'" in str(refusal) and GROUP in str(refusal)
    assert "'events'" not in str(refusal), "a topic that was assigned is named as missing"
    # Checked on the first message, before any row left the consumer's generator, and nothing
    # consumed is committed: the offsets of `events` stay where they were, so the next run reads
    # those messages again instead of losing them.
    assert broker.consumers[0].handed_out_when_assignment_read == 1
    assert broker.consumers[0].closed_with == {"autocommit": False}
    assert _landed(tmp_path).get("events") in (None, set())


def test_a_join_that_never_completes_fails_the_run(tmp_path, monkeypatch):
    broker = FakeBroker({"events": _records("event", 2), "orders": []}, joins=False)

    with pytest.raises(Exception) as failed:  # noqa: B017, PT011
        _run(tmp_path, monkeypatch, broker)

    refusal = _refusal(failed.value)
    assert refusal is not None, f"not refused by the builder: {failed.value!r}"
    assert "'events'" in str(refusal) and "'orders'" in str(refusal)


def test_control_assigned_topics_with_nothing_new_succeed_with_zero_rows(tmp_path, monkeypatch):
    """No unread messages is a normal run, not a refusal. Offsets are committed as usual."""
    broker = FakeBroker({"events": [], "orders": []})

    result = _run(tmp_path, monkeypatch, broker)

    assert result["rows_loaded"] == 0
    assert broker.consumers[0].closed_with == {"autocommit": True}


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Pages: bounded by message count and by payload bytes
# ─────────────────────────────────────────────────────────────────────────────────────────────────


def test_pages_are_bounded_by_count_and_by_bytes(monkeypatch):
    import datanika.services.dlt_runner as dlt_runner

    broker = FakeBroker({"events": [{"id": i} for i in range(5)]})
    consumer = broker.consumer_class()("events")

    monkeypatch.setattr(dlt_runner, "KAFKA_PAGE_MESSAGES", 2)
    by_count = [len(page) for page in dlt_runner._kafka_pages(consumer, ["events"], GROUP)]
    assert by_count == [2, 2, 1]
    consumer.close()

    consumer = broker.consumer_class()("events")
    monkeypatch.setattr(dlt_runner, "KAFKA_PAGE_MESSAGES", 1_000)
    monkeypatch.setattr(dlt_runner, "KAFKA_PAGE_BYTES", 18)  # each value here is 9 bytes
    by_bytes = [len(page) for page in dlt_runner._kafka_pages(consumer, ["events"], GROUP)]
    assert by_bytes == [2, 2, 1]
