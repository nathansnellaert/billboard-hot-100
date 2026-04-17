"""Billboard Hot 100 chart data.

Downloads complete chart history from GitHub mirror, persists raw parquet,
then transforms and publishes with merge key.
"""
import pyarrow as pa
from subsets_utils import (
    get, save_raw_parquet, load_raw_parquet,
    merge, validate, publish,
    load_state, save_state, data_hash,
)
from subsets_utils.testing import assert_valid_date, assert_in_range

DATA_URL = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"
RAW_ASSET_ID = "hot_100_all"
DATASET_ID = "billboard_hot_100"

METADATA = {
    "id": DATASET_ID,
    "title": "Billboard Hot 100 Charts",
    "description": "Weekly Billboard Hot 100 chart rankings from 1958 to present. Includes song position, artist, peak position, and weeks on chart.",
    "license": "Open Data (Public GitHub Repository)",
    "column_descriptions": {
        "chart_date": "Date of the chart (YYYY-MM-DD, Saturdays)",
        "rank": "Current week position (1-100)",
        "song": "Song title",
        "artist": "Artist name",
        "last_week": "Previous week position (null if new entry)",
        "peak_position": "Highest position achieved on the chart",
        "weeks_on_chart": "Total number of weeks the song has been on chart",
    },
}

SCHEMA = pa.schema([
    ("chart_date", pa.string()),
    ("rank", pa.int64()),
    ("song", pa.string()),
    ("artist", pa.string()),
    ("last_week", pa.int64()),
    ("peak_position", pa.int64()),
    ("weeks_on_chart", pa.int64()),
])


def download():
    """Fetch complete chart history and persist as raw parquet."""
    print("  Fetching Billboard Hot 100 data...")
    response = get(DATA_URL)
    response.raise_for_status()
    charts_data = response.json()
    print(f"  Downloaded {len(charts_data)} weekly charts")

    rows = []
    for chart in charts_data:
        chart_date = chart["date"]
        for song in chart["data"]:
            rows.append({
                "chart_date": chart_date,
                "rank": song["this_week"],
                "song": song["song"],
                "artist": song["artist"],
                "last_week": song["last_week"],
                "peak_position": song["peak_position"],
                "weeks_on_chart": song["weeks_on_chart"],
            })

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    print(f"  {table.num_rows:,} total records")
    save_raw_parquet(table, RAW_ASSET_ID)


def transform():
    """Load raw parquet, validate, merge, and publish."""
    table = load_raw_parquet(RAW_ASSET_ID)

    h = data_hash(table)
    if load_state(DATASET_ID).get("hash") == h:
        print(f"  Skipping {DATASET_ID} - unchanged")
        return

    validate(table, {
        "columns": {
            "chart_date": "string",
            "rank": "int",
            "song": "string",
            "artist": "string",
            "last_week": "int",
            "peak_position": "int",
            "weeks_on_chart": "int",
        },
        "not_null": ["chart_date", "rank", "song", "artist"],
        "min_rows": 100,
    })
    assert_valid_date(table, "chart_date")
    assert_in_range(table, "rank", 1, 100)
    assert_in_range(table, "peak_position", 1, 100)

    merge(table, DATASET_ID, key=["chart_date", "rank"])
    publish(DATASET_ID, METADATA)
    save_state(DATASET_ID, {"hash": h})
    print("  Done!")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
