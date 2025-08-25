from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--out", type=Path, default=Path("data/events.parquet"))
parser.add_argument("--days", type=int, default=60)
parser.add_argument("--users", type=int, default=500)
parser.add_argument("--seed", type=int, default=42)
args = parser.parse_args()

rng = np.random.default_rng(args.seed)

# Build an explicit day index ending today, with exactly `args.days` days
end = pd.Timestamp.today().normalize()
dates = pd.date_range(end=end, periods=args.days, freq="D")

user_ids = np.arange(1, args.users + 1)
rows = []
for d in dates:
    # 15–35% of users active each day
    active = rng.choice(user_ids, size=max(1, int(len(user_ids)*rng.uniform(0.15, 0.35))), replace=False)
    for u in active:
        n = rng.integers(1, 6)  # 1–5 events per active user/day
        for _ in range(n):
            ts = d + pd.Timedelta(minutes=int(rng.uniform(0, 1440)))
            ev = rng.choice(["open","pair","action","action","action","crash"])
            rows.append((u, ev, ts, int(ev != "crash")))

df = pd.DataFrame(rows, columns=["user_id","event_name","ts","success"])
args.out.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(args.out, index=False)

# Mini report
df["d"] = df["ts"].dt.date
print(f"Wrote {len(df):,} events to {args.out}")
print(f"Distinct days: {df['d'].nunique()}  |  span: {df['d'].min()} → {df['d'].max()}")
