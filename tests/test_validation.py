import json
import random
import uuid
from datetime import datetime, timezone

import events


def test_row_to_event_maps_and_types(sample_row):
    event = events.row_to_event(sample_row)

    assert set(event.keys()) == {
        "event_id", "event_time", "order_id", "region", "country", "item_type",
        "sales_channel", "order_priority", "order_date", "ship_date", "units_sold",
        "unit_price", "unit_cost", "total_revenue", "total_cost", "total_profit",
    }

    uuid.UUID(event["event_id"])  # raises if not a valid UUID

    event_time = datetime.fromisoformat(event["event_time"])
    age = datetime.now(timezone.utc) - event_time
    assert age.total_seconds() < 5

    assert event["order_date"] == "2012-07-27"
    assert event["ship_date"] == "2012-07-28"
    assert isinstance(event["order_id"], int)
    assert isinstance(event["units_sold"], int)
    assert isinstance(event["unit_price"], float)
    assert event["region"] == sample_row["Region"]


def test_build_key(sample_row):
    assert events.build_key(sample_row) == str(sample_row["Order ID"]).encode("utf-8")


def test_maybe_corrupt_zero_pct_never_corrupts(sample_row):
    event = events.row_to_event(sample_row)
    for _ in range(200):
        payload, was_corrupted, variant = events.maybe_corrupt(event, 0.0)
        assert was_corrupted is False
        assert variant is None
        assert json.loads(payload) == event


def test_maybe_corrupt_full_pct_always_corrupts(sample_row):
    event = events.row_to_event(sample_row)
    for _ in range(200):
        _, was_corrupted, variant = events.maybe_corrupt(event, 1.0)
        assert was_corrupted is True
        assert variant in events.CORRUPTION_VARIANTS


def test_maybe_corrupt_rate_matches_pct_statistically(sample_row):
    random.seed(20260705)
    event = events.row_to_event(sample_row)
    corrupt_pct = 0.02
    n = 20000
    corrupted_count = sum(events.maybe_corrupt(event, corrupt_pct)[1] for _ in range(n))
    observed_rate = corrupted_count / n
    # Binomial std error at n=20000, p=0.02 is ~0.001 -> a +-0.01 band is a ~10 sigma
    # margin, not a flaky threshold.
    assert abs(observed_rate - corrupt_pct) < 0.01


def test_missing_field(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._missing_field(event)
    missing = [f for f in events.REQUIRED_FIELDS_POOL if f not in corrupted]
    assert len(missing) == 1
    for key in event:
        if key != missing[0]:
            assert corrupted[key] == event[key]


def test_null_field(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._null_field(event)
    nulled = [f for f in events.REQUIRED_FIELDS_POOL if corrupted[f] is None]
    assert len(nulled) == 1
    for key in event:
        if key != nulled[0]:
            assert corrupted[key] == event[key]


def test_negative_numeric(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._negative_numeric(event)
    changed = [f for f in events.NUMERIC_FIELDS_POOL if corrupted[f] != event[f]]
    assert len(changed) == 1
    field = changed[0]
    assert corrupted[field] == -abs(event[field])
    for key in event:
        if key != field:
            assert corrupted[key] == event[key]


def test_type_mismatch(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._type_mismatch(event)
    changed = [f for f in events.NUMERIC_FIELDS_POOL if corrupted[f] != event[f]]
    assert len(changed) == 1
    assert corrupted[changed[0]] == "N/A"
    for key in event:
        if key != changed[0]:
            assert corrupted[key] == event[key]


def test_invalid_date_order(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._invalid_date_order(event)
    assert corrupted["order_date"] == event["ship_date"]
    assert corrupted["ship_date"] == event["order_date"]
    for key in event:
        if key not in ("order_date", "ship_date"):
            assert corrupted[key] == event[key]


def test_malformed_json(sample_row):
    event = events.row_to_event(sample_row)
    original = json.dumps(event).encode("utf-8")
    corrupted = events._malformed_json(original)
    assert len(corrupted) < len(original)
    try:
        json.loads(corrupted)
        assert False, "expected malformed_json output to fail JSON parsing"
    except json.JSONDecodeError:
        pass


def test_all_corruption_variants_reachable_and_produce_valid_payloads(sample_row):
    event = events.row_to_event(sample_row)
    seen = set()
    for _ in range(500):
        payload, _, variant = events.maybe_corrupt(event, 1.0)
        seen.add(variant)
        if variant != "malformed_json":
            json.loads(payload)  # must still be valid JSON
    assert seen == set(events.CORRUPTION_VARIANTS)


def test_event_id_never_touched_by_corruption(sample_row):
    # streaming/stream_orders.py's malformed_json detection relies on event_id being
    # absent/null ONLY when JSON parsing fails outright -- corruption variants must never
    # touch event_id directly, or that detection signal breaks.
    event = events.row_to_event(sample_row)
    for _ in range(500):
        payload, _, variant = events.maybe_corrupt(event, 1.0)
        if variant == "malformed_json":
            continue
        parsed = json.loads(payload)
        assert parsed.get("event_id") == event["event_id"]
