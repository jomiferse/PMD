from datetime import datetime, timedelta, timezone

from app.core.alerts import _reversal_flag, _sustained_snapshot_count


def test_sustained_snapshot_count_missing_data_returns_one():
    assert _sustained_snapshot_count([], direction=1) == 1


def test_sustained_snapshot_count_single_snapshot_returns_one():
    now_ts = datetime.now(timezone.utc)
    points = [(now_ts, 0.5)]
    assert _sustained_snapshot_count(points, direction=1) == 1


def test_sustained_snapshot_count_two_snapshots():
    now_ts = datetime.now(timezone.utc)
    points = [
        (now_ts - timedelta(minutes=5), 0.4),
        (now_ts, 0.5),
    ]
    assert _sustained_snapshot_count(points, direction=1) == 2
    assert _sustained_snapshot_count(points, direction=-1) == 1


def test_reversal_flag_partial_and_full():
    now_ts = datetime.now(timezone.utc)
    points_partial = [
        (now_ts - timedelta(minutes=10), 0.4),
        (now_ts - timedelta(minutes=5), 0.6),
        (now_ts, 0.5),
    ]
    assert _reversal_flag(points_partial, direction=1) == "partial"

    points_full = [
        (now_ts - timedelta(minutes=10), 0.4),
        (now_ts - timedelta(minutes=5), 0.6),
        (now_ts, 0.4),
    ]
    assert _reversal_flag(points_full, direction=1) == "full"
