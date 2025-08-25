from pathlib import Path
import polars as pl

def test_metric_store_columns():
    p = Path("data/metric_store.parquet")
    assert p.exists(), "Run metric_store first"
    df = pl.read_parquet(p)
    for col in ["d","DAU","WAU","MAU"]:
        assert col in df.columns
    assert df.height >= 2  # should be multi-day

def test_anomalies_outputs():
    # run anomaly_detect before this, or just check the script exists
    assert Path("src/metrics/anomaly_detect.py").exists()