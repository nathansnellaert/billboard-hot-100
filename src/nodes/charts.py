"""Transform Billboard Hot 100 chart data with state diffing.

- Diffs ingest state vs transform state to find new dates
- Uses DuckDB for efficient transformation
- Merges to Delta table by chart_date + rank (composite key)
"""
import duckdb
import pyarrow as pa
from subsets_utils import load_state, save_state, upload_data, sync_metadata, validate
from subsets_utils.duckdb import raw
from subsets_utils.testing import assert_valid_date, assert_in_range

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

    # Check weeks_on_chart is positive
    weeks = [w for w in table.column("weeks_on_chart").to_pylist() if w is not None]
    assert all(w >= 1 for w in weeks), "weeks_on_chart should be >= 1"


def run():
    """Transform new dates incrementally."""
    print("  Transforming Billboard Hot 100 charts...")

    # Diff ingest vs transform state
    ingest_state = load_state("ingest")
    transform_state = load_state("charts")

    fetched = set(ingest_state.get("fetched_dates", []))
    transformed = set(transform_state.get("transformed_dates", []))
    new_dates = sorted(fetched - transformed)

    if not new_dates:
        print("  No new dates to transform")
        return

    print(f"  Processing {len(new_dates)} new chart dates")

    # Transform all new dates in one DuckDB query
    assets = [f"charts/{d}" for d in new_dates]
    table = duckdb.sql(f"""
        SELECT
            chart_date,
            rank,
            song,
            artist,
            last_week,
            peak_position,
            weeks_on_chart
        FROM {raw(assets)}
        ORDER BY chart_date, rank
    """).arrow()

    print(f"  {table.num_rows:,} total records")

    test(table)
    upload_data(table, DATASET_ID, mode="append")

    # Update state with all new dates
    transformed.update(new_dates)
    save_state("charts", {"transformed_dates": sorted(transformed)})

    sync_metadata(DATASET_ID, METADATA)
    print("  Done!")
