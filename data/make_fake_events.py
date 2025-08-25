# from future import annotations
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--out", type=Path, default=Path("data/events.parquet"))
parser.add_argument("--days", type=int, default=60)
parser.add_argument("--users", type=int, default=500)
args = parser.parse_args()

rng = np.random.default_rng(42)
dates = pd.date_range(
    pd.Timestamp.today().normalize() - pd.Timedelta(days=args.days - 1),
    periods=args.days,
    freq="D",
)
user_ids = np.arange(1, args.users + 1)

rows = []
for d in dates:
    active = rng.choice(
        user_ids, size=int(len(user_ids) * rng.uniform(0.15, 0.35)), replace=False
    )
for u in active:
    n = rng.integers(1, 6)
for _ in range(n):
    rows.append(
        (
            u,
            rng.choice(["open", "pair", "log", "share"]),
            d + pd.Timedelta(minutes=int(rng.uniform(0, 1440))),
            int(rng.uniform(0, 2)),
        )
    )
df = pd.DataFrame(rows, columns=["user_id", "event_name", "ts", "success"])
args.out.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(args.out, index=False)
print(f"Wrote {len(df)} events to {args.out}")
