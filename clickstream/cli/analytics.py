# ==============================================================================
# Analytics Command
# ==============================================================================
"""
Analytics command for the clickstream pipeline CLI.

Displays conversion funnel metrics from the PostgreSQL database.
"""

from typing import Annotated

import typer

from clickstream.cli.shared import (
    BOX_WIDTH,
    C,
    I,
    _box_bottom,
    _box_header,
    _box_line,
    _empty_line,
)


# ==============================================================================
# Commands
# ==============================================================================


def show_analytics(
    json_output: Annotated[
        bool, typer.Option("--json", "-j", help="Output as JSON for scripting")
    ] = False,
) -> None:
    """Show funnel analytics metrics.

    Displays conversion funnel metrics for three time windows:
    - All Time: All sessions in the database
    - Last Day: Sessions that started within 24 hours of the most recent data
    - Last Month: Sessions that started within 30 days of the most recent data

    Time windows are based on data timestamps, not current wall clock time.

    Examples:
        clickstream analytics          # Formatted table output
        clickstream analytics --json   # JSON output for scripting
    """
    import json

    from clickstream.utils.db import get_funnel_metrics

    W = BOX_WIDTH
    INNER = W - 2  # inner width

    # Get metrics from database
    metrics = get_funnel_metrics()

    if metrics is None:
        if json_output:
            print(json.dumps({"error": "No data available or database unreachable"}))
        else:
            print(f"\n{C.BRIGHT_RED}{I.CROSS} No data available or database unreachable{C.RESET}\n")
        raise typer.Exit(1)

    # JSON output mode
    if json_output:
        print(json.dumps(metrics, indent=2))
        return

    # Check if last_day/last_month have same data as all_time
    # (happens when data range is less than the window)
    all_time = metrics["all_time"]
    last_day = metrics["last_day"]
    last_month = metrics["last_month"]

    # Formatted table output
    print()
    print(_box_header("CLICKSTREAM ANALYTICS", W))
    print(_empty_line(W))

    # Column headers - fixed width columns
    header = f"  {'':26}{'All Time':>12}  {'Last Day':>10}  {'Last Month':>10}"
    print(_box_line(header, W))

    # Separator line
    sep = "  " + "─" * (INNER - 4)  # -4 for leading/trailing padding
    print(_box_line(sep, W))

    # Sessions row
    row = f"  {'Sessions':<26}{all_time['sessions']:>12,}  {last_day['sessions']:>10,}  {last_month['sessions']:>10,}"
    print(_box_line(row, W))

    # Arrow + Added to Cart
    row = f"  {'  -> Added to Cart':<26}{all_time['cart_sessions']:>12,}  {last_day['cart_sessions']:>10,}  {last_month['cart_sessions']:>10,}"
    print(_box_line(row, W))

    # Arrow + Purchased
    row = f"  {'  -> Purchased':<26}{all_time['purchase_sessions']:>12,}  {last_day['purchase_sessions']:>10,}  {last_month['purchase_sessions']:>10,}"
    print(_box_line(row, W))

    print(_empty_line(W))

    # Conversion Rate
    row = f"  {'Conversion Rate':<26}{all_time['conversion_rate']:>11.1f}%  {last_day['conversion_rate']:>9.1f}%  {last_month['conversion_rate']:>9.1f}%"
    print(_box_line(row, W))

    # Cart Abandonment
    row = f"  {'Cart Abandonment':<26}{all_time['cart_abandonment']:>11.1f}%  {last_day['cart_abandonment']:>9.1f}%  {last_month['cart_abandonment']:>9.1f}%"
    print(_box_line(row, W))

    # Avg Items/Purchase
    row = f"  {'Avg Items/Purchase':<26}{all_time['avg_items_per_purchase']:>12.1f}  {last_day['avg_items_per_purchase']:>10.1f}  {last_month['avg_items_per_purchase']:>10.1f}"
    print(_box_line(row, W))

    print(_empty_line(W))
    print(_box_bottom(W))
    print()
