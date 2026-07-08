import io
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import fastavro
import pytest

import events

_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "producer" / "schemas" / "order_event.avsc"
_PARSED_SCHEMA = fastavro.parse_schema(json.loads(_SCHEMA_PATH.read_text()))


def _decode(payload: bytes) -> dict:
    """Strips the 5-byte Confluent wire-format header and Avro-decodes the rest, using
    fastavro directly (already a transitive dependency of confluent-kafka[avro]) rather
    than round-tripping through another registry client -- these tests only need to
    confirm *what the producer serialized*, not exercise deserialization itself."""
    return fastavro.schemaless_reader(io.BytesIO(payload[5:]), _PARSED_SCHEMA)


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
        assert _decode(payload) == event


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


def test_invalid_date_order(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._invalid_date_order(event)
    assert corrupted["order_date"] == event["ship_date"]
    assert corrupted["ship_date"] == event["order_date"]
    for key in event:
        if key not in ("order_date", "ship_date"):
            assert corrupted[key] == event[key]


def test_extreme_outlier_numeric(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._extreme_outlier_numeric(event)
    changed = [f for f in events.NUMERIC_FIELDS_POOL if corrupted[f] != event[f]]
    assert len(changed) == 1
    field = changed[0]
    assert corrupted[field] == events._EXTREME_VALUES[field]
    for key in event:
        if key != field:
            assert corrupted[key] == event[key]


def test_revenue_quantity_mismatch(sample_row):
    event = events.row_to_event(sample_row)
    corrupted = events._revenue_quantity_mismatch(event)
    assert corrupted["total_revenue"] == round(event["units_sold"] * event["unit_price"] * 5, 2)
    # Individually still schema-valid values -- units_sold/unit_price themselves are
    # untouched, only their relationship to total_revenue is broken.
    assert corrupted["units_sold"] == event["units_sold"]
    assert corrupted["unit_price"] == event["unit_price"]
    for key in event:
        if key != "total_revenue":
            assert corrupted[key] == event[key]


def test_malformed_bytes_corrupts_wire_format_header(sample_row):
    event = events.row_to_event(sample_row)
    original = events.serialize_event(event)
    for _ in range(50):
        corrupted = events._malformed_bytes(original)
        assert len(corrupted) == len(original)  # header bytes flipped, not truncated
        assert corrupted[:5] != original[:5]
        assert corrupted[5:] == original[5:]  # only the header changes, never the payload


def test_all_corruption_variants_reachable_and_produce_valid_payloads(sample_row):
    event = events.row_to_event(sample_row)
    seen = set()
    for _ in range(500):
        payload, _, variant = events.maybe_corrupt(event, 1.0)
        seen.add(variant)
        if variant != "malformed_bytes":
            _decode(payload)  # must still be a valid, schema-conforming Avro record
        # malformed_bytes's header corruption is covered by
        # test_malformed_bytes_corrupts_wire_format_header above.
    assert seen == set(events.CORRUPTION_VARIANTS)


def test_event_id_never_touched_by_corruption(sample_row):
    # streaming/stream_orders.py's malformed_bytes detection relies on the wire-format
    # header alone, not on event_id -- but every OTHER corruption variant must still
    # leave event_id untouched, since it's used elsewhere (DLQ correlation, dashboards)
    # as a stable per-event identifier.
    event = events.row_to_event(sample_row)
    for _ in range(500):
        payload, _, variant = events.maybe_corrupt(event, 1.0)
        if variant == "malformed_bytes":
            continue
        decoded = _decode(payload)
        assert decoded["event_id"] == event["event_id"]


# --- Structurally-impossible-under-Avro corruptions ---
# These used to be produced by dedicated _missing_field/_null_field/_type_mismatch
# corruptors and sent to Kafka as malformed JSON. Under Avro (order_event.avsc declares
# all 16 fields required: no "null" union, no default), the same malformed dicts can no
# longer be serialized at all. These tests assert that directly against the real
# AvroSerializer via events.serialize_event(), rather than just asserting it in a
# comment -- this is the actual architectural guarantee the schema registry buys.
#
# Empirically verified (not assumed): AvroSerializer's registry round-trip
# (register/lookup the schema ID) happens *before* the local fastavro encode step, and
# fastavro raises plain TypeError/ValueError for these cases, not
# confluent_kafka.serialization.SerializationError -- confirmed against a real
# AvroSerializer call against a local fake registry. This is the same class of "don't
# assume, check" issue this codebase already hit with from_json's null-struct behavior.

def test_serializer_rejects_missing_field(sample_row):
    event = events.row_to_event(sample_row)
    del event["order_id"]
    with pytest.raises((TypeError, ValueError)):
        events.serialize_event(event)


def test_serializer_rejects_null_field(sample_row):
    event = events.row_to_event(sample_row)
    event["region"] = None
    with pytest.raises((TypeError, ValueError)):
        events.serialize_event(event)


def test_serializer_rejects_type_mismatch(sample_row):
    event = events.row_to_event(sample_row)
    event["total_revenue"] = "N/A"
    with pytest.raises((TypeError, ValueError)):
        events.serialize_event(event)
