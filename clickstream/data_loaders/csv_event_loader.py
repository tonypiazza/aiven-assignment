# ==============================================================================
# CSV Data Loader - Load Clickstream Events
# ==============================================================================
"""
Loads clickstream events directly from CSV file.
"""

import polars as pl

if "data_loader" not in dir():
    from mage_ai.data_preparation.decorators import data_loader, test

from clickstream.utils.config import get_settings


@data_loader
def load_csv_events(*args, **kwargs) -> pl.DataFrame:
    """
    Load clickstream events from CSV file.

    Configuration via environment variables:
        PRODUCER_DATA_FILE: Path to the CSV file

    Keyword Args:
        limit: Maximum rows to load (default: None = all rows)

    Returns:
        Polars DataFrame with event data
    """
    settings = get_settings()
    limit = kwargs.get("limit")

    df = pl.read_csv(settings.producer.data_file_path)

    # Rename columns to match our schema
    df = df.rename(
        {
            "visitorid": "visitor_id",
            "itemid": "item_id",
            "transactionid": "transaction_id",
        }
    )

    # Sort by timestamp
    df = df.sort("timestamp")

    if limit:
        df = df.head(limit)

    return df


@test
def test_output(output: pl.DataFrame, *args) -> None:
    """Validate the output DataFrame."""
    assert output is not None, "Output is undefined"
    assert len(output) > 0, "Output is empty"

    required_columns = ["timestamp", "visitor_id", "event", "item_id"]
    for col in required_columns:
        assert col in output.columns, f"Missing required column: {col}"

    # Validate event types
    valid_events = {"view", "addtocart", "transaction"}
    actual_events = set(output["event"].unique().to_list())
    assert actual_events.issubset(valid_events), (
        f"Invalid event types: {actual_events - valid_events}"
    )
