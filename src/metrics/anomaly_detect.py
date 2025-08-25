from __future__ import annotations
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt
import typer

app = typer.Typer(help="Detect DAU anomalies from a metric store (Polars)")

def detect_anomalies(df: pl.DataFrame, col: str, win: int = 7, z: float = 2.0) -> pl.DataFrame:
    df = df.sort("d").with_columns([
        pl.col(col).rolling_mean(win).alias("roll_mean"),
        pl.col(col).rolling_std(win).alias("roll_std"),
    ])
    df = df.with_columns(((pl.col(col) - pl.col("roll_mean")) / pl.col("roll_std")).alias("zscore"))
    return df.with_columns((pl.col("zscore").abs() >= z).alias("anomaly"))

@app.command()
def main(
    in_path: Path = typer.Option(None, help="Path to metric_store.parquet"),
    out_csv: Path = typer.Option(None, help="Output CSV (defaults to artifacts/anomalies.csv)"),
    out_png: Path = typer.Option(None, help="Output PNG chart (defaults to artifacts/dau_trend.png)"),
    col: str = typer.Option("DAU", help="Metric column to analyze"),
    win: int = typer.Option(7, help="Rolling window size"),
    z: float = typer.Option(2.0, help="Z-score threshold"),
):
    # Resolve repo root from this file location
    repo = Path(__file__).resolve().parents[2]
    in_path = in_path or (repo / "data/metric_store.parquet")
    out_csv = out_csv or (repo / "artifacts/anomalies.csv")
    out_png = out_png or (repo / "artifacts/dau_trend.png")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df = pl.read_parquet(in_path)
    out = detect_anomalies(df, col=col, win=win, z=z)
    out.select(["d", col, "roll_mean", "roll_std", "zscore", "anomaly"]).write_csv(out_csv)

    # Plot
    fig = plt.figure(figsize=(10, 4))
    plt.plot(out["d"].to_list(), out[col].to_list(), label=col)
    plt.plot(out["d"].to_list(), out["roll_mean"].to_list(), label=f"{win}d mean")
    ax = plt.gca()
    for d, is_anom, v in zip(out["d"], out["anomaly"], out[col]):
        if is_anom:
            ax.scatter([d], [v], marker="o", s=30)
    plt.title(f"{col} with {win}d mean and anomalies (|z|≥{z})")
    plt.legend()
    plt.tight_layout()
    fig.savefig(out_png)
    print(f"Wrote {out_csv} and {out_png}")

if __name__ == "__main__":
    app()
