
"""Ingest Billboard Hot 100 chart data."""

from subsets_utils import get, save_raw_json

DATA_URL = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"


def run():
    """Fetch Billboard Hot 100 data from GitHub."""
    print("  Fetching Billboard Hot 100 data...")
    response = get(DATA_URL)
    response.raise_for_status()
    charts_data = response.json()
    print(f"  Downloaded {len(charts_data)} weekly charts")
    save_raw_json(charts_data, "charts", compress=True)
