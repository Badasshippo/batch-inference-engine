"""Unit tests for the global fair scheduler (weighted round-robin)."""
from __future__ import annotations

from app.models import Priority, WorkItem
from app.scheduler import FairScheduler


def _items(job: str, n: int) -> list[WorkItem]:
    return [WorkItem(seq=i, id=f"{job}-{i}", prompt=f"{job}-{i}") for i in range(n)]


def test_round_robin_is_fair_between_equal_jobs():
    s = FairScheduler()
    s.add_job("A", _items("A", 100), Priority.NORMAL)
    s.add_job("B", _items("B", 100), Priority.NORMAL)

    served = {"A": 0, "B": 0}
    for _ in range(20):
        jid, _item = s.pop()
        served[jid] += 1

    # Equal weights -> balanced service over time.
    assert served["A"] == served["B"] == 10


def test_priority_gets_more_throughput_without_starving():
    s = FairScheduler()
    s.add_job("hi", _items("hi", 100), Priority.HIGH)   # weight 4
    s.add_job("lo", _items("lo", 100), Priority.LOW)    # weight 1

    served = {"hi": 0, "lo": 0}
    for _ in range(20):
        jid, _item = s.pop()
        served[jid] += 1

    # High priority served ~4x more, but low priority is NOT starved.
    assert served["hi"] > served["lo"]
    assert served["lo"] > 0
    assert served["hi"] == 16 and served["lo"] == 4


def test_small_job_finishes_alongside_huge_job():
    s = FairScheduler()
    s.add_job("huge", _items("huge", 1000), Priority.NORMAL)
    s.add_job("small", _items("small", 4), Priority.NORMAL)

    # Drain the small job and record how many pops it took.
    small_done_at = None
    for i in range(1, 2001):
        jid, _ = s.pop()
        if jid == "small":
            if s.job_pending("small") == 0:
                small_done_at = i
                break
    # The 4-item job should finish well before the 1000-item job dominates.
    assert small_done_at is not None and small_done_at < 20


def test_remove_job_drops_pending():
    s = FairScheduler()
    s.add_job("A", _items("A", 5), Priority.NORMAL)
    s.add_job("B", _items("B", 5), Priority.NORMAL)
    assert s.pending == 10

    removed = s.remove_job("A")
    assert removed == 5
    assert s.pending == 5
    # Only B remains.
    for _ in range(5):
        jid, _ = s.pop()
        assert jid == "B"
    assert s.pop() is None


def test_empty_scheduler_returns_none():
    s = FairScheduler()
    assert s.pop() is None
