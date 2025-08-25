# Product Metrics — Metric Store, Driver Tree & A/B Scaffolding

Implements a **metrics health** system:
- Event ingestion → **metric store** tables (DAU/WAU/MAU, activation, retention)
- **Driver tree** for a North Star metric with **guardrails** (crash rate, latency, support tickets)
- **Anomaly detection** (STL + rolling z-score)
- **A/B testing** scaffolding with CUPED + power calc

## Quickstart
```bash
# create fake events
python data/make_fake_events.py --out data/events.parquet
# build metric store
python -m src.metrics.metric_store --events_path data/events.parquet --out_path data/metric_store.parquet
# CUPED demo (see data/ab_demo.csv)
python -m src.experiments.ab_cuped data/ab_demo.csv
