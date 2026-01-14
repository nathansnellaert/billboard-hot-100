"""Billboard Hot 100 chart data.

Full refresh with merge key - no state tracking needed.
"""
import pyarrow as pa
from subsets_utils import get, upload_data, validate
from subsets_utils.testing import assert_valid_date, assert_in_range

DATA_URL = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"
DATASET_ID = "billboard_hot_100"

METADATA = {
    "title": "Billboard Hot 100 Charts",
    "description": "Weekly Billboard Hot 100 chart rankings. Includes song position, artist, and chart history.",
    "column_descriptions": {
        "chart_date": "Date of the chart (YYYY-MM-DD)",
        "rank": "Current week position (1-100)",
        "song": "Song title",
        "artist": "Artist name",
        "last_week": "Previous week position (null if new entry)",
        "peak_position": "Highest position achieved",
        "weeks_on_chart": "Number of weeks on chart",
    }
}

SCHEMA = pa.schema([
    ('chart_date', pa.string()),
    ('rank', pa.int64()),
    ('song', pa.string()),
    ('artist', pa.string()),
    ('last_week', pa.int64()),
    ('peak_position', pa.int64()),
    ('weeks_on_chart', pa.int64()),
])


def test(table: pa.Table) -> None:
    """Validate Billboard Hot 100 output."""
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


def run():
    """Fetch and upload Billboard Hot 100 data."""
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

    test(table)
    upload_data(table, DATASET_ID, mode="merge", merge_key=["chart_date", "rank"])
    print("  Done!")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
