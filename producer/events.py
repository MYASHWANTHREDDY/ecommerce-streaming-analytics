import os
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

NUMERIC_FIELDS_POOL = ["units_sold", "unit_price", "total_revenue"]

# Absurd-but-schema-valid values for the extreme_outlier_numeric variant. Fixed rather
# than a multiplier of the original value -- a multiplier risks overflowing units_sold's
# Avro "int" (32-bit) type for larger sample rows, which would raise here instead of
# reaching Kafka, defeating the point of this variant.
_EXTREME_VALUES = {
    "units_sold": 2_000_000_000,       # near int32 max; absurd for a single order
    "unit_price": 1_000_000_000.0,     # $1B per unit
    "total_revenue": 1_000_000_000_000.0,  # $1T for one order
}

CORRUPTION_VARIANTS = [
    "negative_numeric",
    "invalid_date_order",
    "malformed_bytes",
    "extreme_outlier_numeric",
    "revenue_quantity_mismatch",
]

# --- Why missing_field / null_field / type_mismatch are gone ---
# Before the Avro migration, this module had 6 corruption variants including
# missing_field, null_field, and type_mismatch: pop a required field, null it out, or
# swap in a wrong-typed value, then JSON-encode the result and send it. That's no longer
# possible to construct: order_event.avsc declares all 16 fields required (no union with
# "null", no default), so AvroSerializer.__call__ raises before any such dict reaches
# Kafka -- see test_serializer_rejects_missing_field / _null_field / _type_mismatch in
# tests/test_validation.py, which assert this directly against the real serializer rather
# than just asserting it in prose here. That's the entire point of a schema registry, and
# losing these 3 variants is a deliberate, understood tradeoff (see CHANGELOG.md item 3),
# not a silent coverage gap -- extreme_outlier_numeric and revenue_quantity_mismatch below
# were added specifically so total corruption-path coverage doesn't shrink (6 -> 5, not
# 6 -> 3).

KAFKA_TOPIC_ORDERS = os.environ.get("KAFKA_TOPIC_ORDERS", "orders")
KAFKA_TOPIC_ORDER_SHIPPED = os.environ.get("KAFKA_TOPIC_ORDER_SHIPPED", "order_shipped")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
_SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "order_event.avsc"
_SHIPPED_SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "order_shipped_event.avsc"

# Constructing these doesn't touch the network (confirmed empirically -- SchemaRegistryClient
# and AvroSerializer.__init__ just parse the local schema and store config; the registry
# is only actually contacted inside AvroSerializer.__call__, on first real serialize call).
# That matters because this module is imported by the test suite too, and CI runs `pytest`
# with no live Schema Registry -- see tests/conftest.py's fake in-process registry, which
# tests point SCHEMA_REGISTRY_URL at before importing this module.
_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
_avro_serializer = AvroSerializer(_registry_client, _SCHEMA_PATH.read_text())
_serialization_ctx = SerializationContext(KAFKA_TOPIC_ORDERS, MessageField.VALUE)

# Separate topic, separate schema/subject, separate serializer -- order_shipped is a
# genuinely independent event stream, not a variant of OrderEvent. See
# order_shipped_event.avsc's doc field for why it exists (real streaming-derived
# fulfillment time instead of the dataset's static order_date/ship_date).
_shipped_avro_serializer = AvroSerializer(_registry_client, _SHIPPED_SCHEMA_PATH.read_text())
_shipped_serialization_ctx = SerializationContext(KAFKA_TOPIC_ORDER_SHIPPED, MessageField.VALUE)


def _to_iso_date(mdy: str) -> str:
    return datetime.strptime(mdy, "%m/%d/%Y").date().isoformat()


def row_to_event(row: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "order_id": int(row["Order ID"]),
        "region": row["Region"],
        "country": row["Country"],
        "item_type": row["Item Type"],
        "sales_channel": row["Sales Channel"],
        "order_priority": row["Order Priority"],
        "order_date": _to_iso_date(row["Order Date"]),
        "ship_date": _to_iso_date(row["Ship Date"]),
        "units_sold": int(row["Units Sold"]),
        "unit_price": float(row["Unit Price"]),
        "unit_cost": float(row["Unit Cost"]),
        "total_revenue": float(row["Total Revenue"]),
        "total_cost": float(row["Total Cost"]),
        "total_profit": float(row["Total Profit"]),
    }


def build_key(row: dict) -> bytes:
    return str(row["Order ID"]).encode("utf-8")


def serialize_event(event: dict) -> bytes:
    """Avro-encode + prepend the Confluent wire-format header (magic byte + schema ID).
    The one place this module talks to Schema Registry."""
    return _avro_serializer(event, _serialization_ctx)


def build_shipped_event(order_id: int, placed_event_id: str) -> dict:
    """placed_event_id is the join key streaming/stream_orders.py actually uses -- see
    order_shipped_event.avsc's doc field for why order_id alone isn't safe under
    LOOP=true's repeating replay. shipped_at is real wall-clock time at send, same as
    OrderEvent's event_time."""
    return {
        "event_id": str(uuid.uuid4()),
        "placed_event_id": placed_event_id,
        "order_id": order_id,
        "shipped_at": datetime.now(timezone.utc).isoformat(),
    }


def serialize_shipped_event(event: dict) -> bytes:
    return _shipped_avro_serializer(event, _shipped_serialization_ctx)


def _negative_numeric(event: dict) -> dict:
    corrupted = dict(event)
    field = random.choice(NUMERIC_FIELDS_POOL)
    corrupted[field] = -abs(corrupted[field])
    return corrupted


def _invalid_date_order(event: dict) -> dict:
    corrupted = dict(event)
    corrupted["order_date"], corrupted["ship_date"] = corrupted["ship_date"], corrupted["order_date"]
    return corrupted


def _extreme_outlier_numeric(event: dict) -> dict:
    # Schema-valid (right type, in range for its Avro type) but an absurd magnitude for a
    # single order -- exactly what a schema *can't* catch, same category as negative_numeric.
    corrupted = dict(event)
    field = random.choice(NUMERIC_FIELDS_POOL)
    corrupted[field] = _EXTREME_VALUES[field]
    return corrupted


def _revenue_quantity_mismatch(event: dict) -> dict:
    # Schema-valid, individually-plausible values whose cross-field relationship is
    # broken: total_revenue no longer approximates units_sold * unit_price. A single
    # field's own type/range is fine here, so only an explicit cross-field check (added to
    # streaming/stream_orders.py) can catch it -- this is real new validation logic, not a
    # relabeled old variant.
    corrupted = dict(event)
    corrupted["total_revenue"] = round(corrupted["units_sold"] * corrupted["unit_price"] * 5, 2)
    return corrupted


def _malformed_bytes(payload: bytes) -> bytes:
    # Confluent wire format: [magic byte = 0x00][4-byte big-endian schema ID][avro payload].
    # Corrupting the header (not the payload) occupies the same architectural slot
    # malformed_json used to, but is a more realistic failure mode for a binary Avro
    # pipeline: a truncated/garbled Avro payload might still happen to decode into a
    # wrong-but-schema-valid record, but a bad magic byte or unrecognized schema ID is
    # always immediately, unambiguously invalid under the Confluent wire protocol --
    # streaming/stream_orders.py checks exactly these 5 bytes before ever attempting
    # from_avro.
    corrupted = bytearray(payload)
    if random.random() < 0.5:
        corrupted[0] = 0xFF  # magic byte must be 0x00
    else:
        corrupted[1:5] = (0xFFFFFFFF).to_bytes(4, "big")  # schema ID no consumer resolved
    return bytes(corrupted)


_DICT_CORRUPTORS = {
    "negative_numeric": _negative_numeric,
    "invalid_date_order": _invalid_date_order,
    "extreme_outlier_numeric": _extreme_outlier_numeric,
    "revenue_quantity_mismatch": _revenue_quantity_mismatch,
}


def maybe_corrupt(event: dict, corrupt_pct: float) -> tuple[bytes, bool, str | None]:
    if random.random() >= corrupt_pct:
        return serialize_event(event), False, None

    variant = random.choice(CORRUPTION_VARIANTS)
    if variant == "malformed_bytes":
        payload = _malformed_bytes(serialize_event(event))
    else:
        payload = serialize_event(_DICT_CORRUPTORS[variant](event))
    return payload, True, variant
