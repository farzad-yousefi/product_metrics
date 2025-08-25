from __future__ import annotations
from pathlib import Path
import polars as pl
import typer

app = typer.Typer(help="Builds daily KPIs (DAU/WAU/MAU + placeholders) into a metric store.")

REQUIRED = ["user_id", "event_name", "ts"]
ALT_NAMES = {
    "user_id": ["user", "uid", "member_id", "account_id"],
    "event_name": ["event", "name", "action"],
    "ts": ["timestamp", "event_time", "event_ts", "time", "datetime"],
}

def _read_events(path: Path, verbose: bool = False) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Events file not found: {path}")
    df = pl.read_parquet(path) if path.suffix.lower()==".parquet" else pl.read_csv(path, try_parse_dates=True)
    # Auto-rename common alternatives
    rename_map: dict[str, str] = {}
    for target in REQUIRED:
        if target not in df.columns:
            for cand in ALT_NAMES.get(target, []):
                if cand in df.columns:
                    rename_map[cand] = target
                    break
    if rename_map:
        df = df.rename(rename_map)

    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Present: {df.columns}")

    # Parse/normalize timestamp
    ts_dtype = df.schema["ts"]
    if ts_dtype == pl.Utf8:
        df = df.with_columns(pl.col("ts").str.to_datetime(strict=False))
    elif ts_dtype == pl.Date:
        df = df.with_columns(pl.col("ts").cast(pl.Datetime))
    # else assume Datetime/DatetimeTZ already

    if verbose:
        typer.echo(f"[read_events] rows={df.height:,}, cols={df.width}, dtypes={df.dtypes}")
        typer.echo(f"[read_events] head:\n{df.head(3)}")
    return df

def build_metric_store(events: pl.DataFrame, verbose: bool = False) -> pl.DataFrame:
    # date key
    df = events.with_columns(pl.col("ts").dt.date().alias("d")).select(["d", "user_id"])
    # daily active users
    dau = df.group_by("d").agg(pl.col("user_id").n_unique().alias("DAU")).sort("d")

    # WAU/MAU approximations via rolling sum of DAU (distinct-by-window requires raw events; fine for demo)
    dau = dau.with_columns([
        pl.col("DAU").rolling_sum(window_size=7, min_periods=1).alias("WAU"),
        pl.col("DAU").rolling_sum(window_size=30, min_periods=1).alias("MAU"),
    ])

    # Placeholders to be replaced later with true definitions
    dau = dau.with_columns([
        (pl.col("DAU") * 0.20).alias("activation_rate_placeholder"),
        (pl.col("DAU") * 0.50).alias("d7_retention_placeholder"),
    ])

    if verbose:
        typer.echo(f"[build_metric_store] days={dau.height}, date_range={dau['d'][0]} .. {dau['d'][-1]}")
    return dau

@app.command()
def main(
    events_path: Path = typer.Option(..., help="Path to events file (parquet or csv)"),
    out_path: Path = typer.Option(..., help="Where to write the metric store (.parquet or .csv)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print debug info"),
):
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        events = _read_events(events_path, verbose=verbose)
        metric_store = build_metric_store(events, verbose=verbose)
        if out_path.suffix.lower() == ".parquet":
            metric_store.write_parquet(out_path)
        else:
            metric_store.write_csv(out_path)
        typer.echo(f"✅ Wrote metric store to {out_path}  (rows={metric_store.height})")
    except Exception as e:
        typer.echo(f"❌ Failed: {e}")
        raise

if __name__ == "__main__":
    app()
