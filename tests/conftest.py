import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "producer"))


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
