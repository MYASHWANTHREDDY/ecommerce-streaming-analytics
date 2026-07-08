import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "producer"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Must happen before any test module does `import events`: events.py builds its
# AvroSerializer at import time from SCHEMA_REGISTRY_URL, and CI runs `pytest` with no
# live Schema Registry (see .github/workflows/ci.yml). Starting the fake here, at
# conftest module scope, guarantees it's up and the env var is set before pytest
# collects tests/test_validation.py.
import fake_schema_registry  # noqa: E402

_SCHEMA_TEXT = (REPO_ROOT / "producer" / "schemas" / "order_event.avsc").read_text()
os.environ["SCHEMA_REGISTRY_URL"] = fake_schema_registry.start(_SCHEMA_TEXT)


@pytest.fixture
def sample_row():
    return {
        "Order ID": "443368995",
        "Region": "Sub-Saharan Africa",
        "Country": "South Africa",
        "Item Type": "Fruits",
        "Sales Channel": "Offline",
        "Order Priority": "M",
        "Order Date": "7/27/2012",
        "Ship Date": "7/28/2012",
        "Units Sold": "1593",
        "Unit Price": "9.33",
        "Unit Cost": "6.92",
        "Total Revenue": "14862.69",
        "Total Cost": "11023.56",
        "Total Profit": "3839.13",
    }
