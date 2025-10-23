import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import Library as mylib


def get_last_employee(x: str) -> str:
    if not x or pd.isna(x):
        return "0"
    parts = x.split(",")
    for i in reversed(parts):
        if i != "n/a":
            return i
    return "0"


def normalize(x: float) -> float:
    for a in range(40000):
        top = (a + 1) * 10
        bottom = a * 10
        if x > bottom and x <= top:
            return top
    return 0


def _load_firms() -> pd.DataFrame:
    df = pd.read_parquet("DB_Out/DB_firms.parquet", columns=["company_id", "employee_number"])
    df["employee_number"] = df["employee_number"].apply(get_last_employee)
    df["employee_number"] = pd.to_numeric(df["employee_number"], errors="coerce")
    df["employee_norm"] = df["employee_number"].apply(normalize)
    # keep only valid classes (>0)
    df = df[df["employee_norm"] > 0].copy()
    return df


def _prepare_distribution(df: pd.DataFrame, col: str = "employee_norm") -> pd.DataFrame:
    counts = df[col].value_counts().sort_index()
    total = counts.sum()
    pct = (counts / total * 100.0).round(2)
    out = (
        pd.DataFrame({"class": counts.index.astype(int), "count": counts.values, "percent": pct.values})
        .sort_values("class")
        .reset_index(drop=True)
    )
    return out


def _plot_bar(dist: pd.DataFrame, title: str, outfile: Path) -> None:
    x = dist["class"].astype(int).tolist()
    y = dist["percent"].tolist()

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(x, y, width=8, color="#4C78A8")
    ax.set_title(title)
    ax.set_xlabel("Employee class (upper bound of bin)")
    ax.set_ylabel("Firms (%)")
    ax.set_ylim(0, max(5.0, min(100.0, (max(y) if y else 0) * 1.2)))
    ax.set_xticks(x)

    # annotate bars with percentages
    #for rect, pct in zip(bars, y):
    #    ax.annotate(f"{pct:.1f}%", xy=(rect.get_x() + rect.get_width() / 2, rect.get_height()),
     #               xytext=(0, 3), textcoords="offset points",
     #               ha="center", va="bottom", fontsize=8)

    fig.tight_layout()
    plt.show()


def main() -> None:
    df = _load_firms()

    # All firms distribution
    dist_all = _prepare_distribution(df)
    _plot_bar(dist_all, "Employee Size Distribution — All Firms", Path("DB_Out/firm_size_distribution_all.png"))

    # Space firms distribution (Space == 1)
    df_space = mylib.space(df[["company_id", "employee_norm"]], "company_id", True)
    dist_space = _prepare_distribution(df_space)
    _plot_bar(dist_space, "Employee Size Distribution — Space Firms", Path("DB_Out/firm_size_distribution_space.png"))


if __name__ == "__main__":
    main()

