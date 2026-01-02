"""Transform Billboard Hot 100 chart data."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

DATASET_ID = "billboard_hot_100"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform chart data."""
    charts_data = load_raw_json("charts")

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
                "weeks_on_chart": song["weeks_on_chart"]
            })

    if not rows:
        raise ValueError("No chart data found")

    print(f"  Transformed {len(rows):,} chart entries")

    table = pa.Table.from_pylist(rows)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
