import json
import random
import uuid
from datetime import datetime, timezone

REQUIRED_FIELDS_POOL = ["order_id", "region", "units_sold", "unit_price", "total_revenue"]
NUMERIC_FIELDS_POOL = ["units_sold", "unit_price", "total_revenue"]
CORRUPTION_VARIANTS = [
    "missing_field",
    "null_field",
    "negative_numeric",
    "type_mismatch",
    "invalid_date_order",
    "malformed_json",
]


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


def _missing_field(event: dict) -> dict:
    corrupted = dict(event)
    corrupted.pop(random.choice(REQUIRED_FIELDS_POOL), None)
    return corrupted


def _null_field(event: dict) -> dict:
    corrupted = dict(event)
    corrupted[random.choice(REQUIRED_FIELDS_POOL)] = None
    return corrupted


def _negative_numeric(event: dict) -> dict:
    corrupted = dict(event)
    field = random.choice(NUMERIC_FIELDS_POOL)
    corrupted[field] = -abs(corrupted[field])
    return corrupted


def _type_mismatch(event: dict) -> dict:
    corrupted = dict(event)
    corrupted[random.choice(NUMERIC_FIELDS_POOL)] = "N/A"
    return corrupted


def _invalid_date_order(event: dict) -> dict:
    corrupted = dict(event)
    corrupted["order_date"], corrupted["ship_date"] = corrupted["ship_date"], corrupted["order_date"]
    return corrupted


def _malformed_json(payload: bytes) -> bytes:
    text = payload.decode("utf-8")
    cut = max(1, len(text) - random.randint(1, 5))
    return text[:cut].encode("utf-8")


_DICT_CORRUPTORS = {
    "missing_field": _missing_field,
    "null_field": _null_field,
    "negative_numeric": _negative_numeric,
    "type_mismatch": _type_mismatch,
    "invalid_date_order": _invalid_date_order,
}


def maybe_corrupt(event: dict, corrupt_pct: float) -> tuple[bytes, bool, str | None]:
    if random.random() >= corrupt_pct:
        return json.dumps(event).encode("utf-8"), False, None

    variant = random.choice(CORRUPTION_VARIANTS)
    if variant == "malformed_json":
        payload = _malformed_json(json.dumps(event).encode("utf-8"))
    else:
        payload = json.dumps(_DICT_CORRUPTORS[variant](event)).encode("utf-8")
    return payload, True, variant
