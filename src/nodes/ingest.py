"""Ingest Billboard Hot 100 chart data with date-based partitioning.

- Fetches all chart data from GitHub
- Saves one parquet file per chart date: raw/charts/{YYYY-MM-DD}.parquet
- Tracks fetched_dates in state for transform to diff against
"""
import pyarrow as pa
from subsets_utils import get, save_raw_parquet, load_state, save_state

DATA_URL = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"

SCHEMA = pa.schema([
    ('chart_date', pa.string()),
    ('rank', pa.int64()),
    ('song', pa.string()),
    ('artist', pa.string()),
    ('last_week', pa.int64()),
    ('peak_position', pa.int64()),
    ('weeks_on_chart', pa.int64()),
])


def run():
    """Fetch Billboard Hot 100 data and save as date-partitioned parquet."""
    print("  Fetching Billboard Hot 100 data...")
    response = get(DATA_URL)
    response.raise_for_status()
    charts_data = response.json()
    print(f"  Downloaded {len(charts_data)} weekly charts")

    state = load_state("ingest")
    fetched_dates = set(state.get("fetched_dates", []))

    # Check for new charts
    source_dates = {chart["date"] for chart in charts_data}
    new_dates = source_dates - fetched_dates

    if not new_dates:
        print("  No new chart dates to save")
        return

    print(f"  Saving {len(new_dates)} new chart dates as parquet...")

    # Save each chart date as separate parquet file
    for chart in charts_data:
        chart_date = chart["date"]
        if chart_date not in new_dates:
            continue

        rows = []
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
        save_raw_parquet(table, f"charts/{chart_date}")
        fetched_dates.add(chart_date)

    save_state("ingest", {"fetched_dates": sorted(fetched_dates)})
    print(f"  Saved {len(new_dates)} parquet files")
