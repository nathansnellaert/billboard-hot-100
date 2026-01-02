import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_in_range


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
        "min_rows": 10000,
    })

    assert_valid_date(table, "chart_date")
    assert_in_range(table, "rank", 1, 100)
    assert_in_range(table, "peak_position", 1, 100)

    # Check weeks_on_chart is positive
    weeks = [w for w in table.column("weeks_on_chart").to_pylist() if w is not None]
    assert all(w >= 1 for w in weeks), "weeks_on_chart should be >= 1"

    print(f"  Validated {len(table):,} records")
