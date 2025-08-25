# from future import annotations
import numpy as np
import pandas as pd
import typer

app = typer.Typer(help="CUPED-adjusted diff-in-means for A/B tests.")


def cuped_adjust(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    theta = np.cov(x, y, bias=True)[0, 1] / (np.var(x) + 1e-12)
    return y - theta * (x - x.mean())


def diff_in_means(a: np.ndarray, b: np.ndarray) -> float:
    return float(b.mean() - a.mean())


@app.command()
def main(csv_path: str):
    df = pd.read_csv(csv_path)
    a = df[df["group"] == "A"]
    b = df[df["group"] == "B"]
    y_a = cuped_adjust(a["y"].to_numpy(), a["x_pre"].to_numpy())
    y_b = cuped_adjust(b["y"].to_numpy(), b["x_pre"].to_numpy())
    print(f"CUPED-adjusted lift (B - A): {diff_in_means(y_a, y_b):.6f}")


if __name__ == "main":
    app()
