# from future import annotations
from pathlib import Path

import polars as pl
import typer

app = typer.Typer(help="Build daily KPIs into a compact metric store.")


def build_metric_store(events: pl.DataFrame) -> pl.DataFrame:
    """
    Assumes columns: user_id, event_name, ts (datetime-like), success (0/1).
    Produces: date, DAU, WAU(approx), MAU(approx) + placeholders for activation/retention.
    """
    df = events.with_columns(
        pl.col("ts").str.to_datetime(strict=False).dt.date().alias("d")
    )
    dau = df.group_by("d").agg(pl.col("user_id").n_unique().alias("DAU")).sort("d")
    dau = dau.with_columns(
        [
            pl.col("DAU").rolling_sum(window_size=7).alias("WAU"),
            pl.col("DAU").rolling_sum(window_size=30).alias("MAU"),
            (pl.col("DAU") * 0.2).alias("activation_rate_placeholder"),
            (pl.col("DAU") * 0.5).alias("d7_retention_placeholder"),
        ]
    )
    return dau


@app.command()
def main(events_path: Path, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    events = (
        pl.read_parquet(events_path)
        if events_path.suffix == ".parquet"
        else pl.read_csv(events_path, try_parse_dates=True)
    )
    metric_store = build_metric_store(events)
    if out_path.suffix == ".parquet":
        metric_store.write_parquet(out_path)
    else:
        metric_store.write_csv(out_path)
    typer.echo(f"Wrote metric store to {out_path}")


if __name__ == "main":
    app()
