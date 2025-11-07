import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spaceSpecialization, spacePercentage


def _prepare_updown_merged(rounds: pd.DataFrame) -> pd.DataFrame:
    """Merge rounds with upstream/downstream classification for space companies only.

    Returns a DataFrame with columns including:
      - company_id, investor_id, round_amount_usd
      - upstream, downstream (binary flags)
    """
    updown = mylib.openDB("updown")
    needed_round_cols = ["company_id", "investor_id", "round_amount_usd"]
    missing = [c for c in needed_round_cols if c not in rounds.columns]
    if missing:
        raise KeyError(f"Missing columns in rounds table: {missing}")

    needed_ud_cols = ["company_id", "upstream", "downstream", "space"]
    missing_ud = [c for c in needed_ud_cols if c not in updown.columns]
    if missing_ud:
        raise KeyError(f"Missing columns in updown table: {missing_ud}")

    # Restrict to space companies in classification and keep only U/D flags
    updown = (
        updown[updown["space"] == 1]
        .loc[:, ["company_id", "upstream", "downstream"]]
        .dropna(subset=["company_id"])
        .copy()
    )
    updown[["upstream", "downstream"]] = (
        updown[["upstream", "downstream"]].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
    )

    # Merge on company_id
    df = pd.merge(rounds[needed_round_cols], updown, how="inner", on="company_id")

    # Keep rows where at least one of the flags is set
    up = (df["upstream"].fillna(0) == 1)
    down = (df["downstream"].fillna(0) == 1)
    df = df[up | down]

    # Normalize amount
    df["round_amount_usd"] = pd.to_numeric(df["round_amount_usd"], errors="coerce")
    df = df.dropna(subset=["round_amount_usd"])  # ensure valid amounts
    return df


def _sum_up_down(df: pd.DataFrame) -> tuple[float, float]:
    """Return total amounts (USD) for upstream and downstream in the provided merged DF."""
    up_mask = (df["upstream"].fillna(0) == 1)
    down_mask = (df["downstream"].fillna(0) == 1)

    up_sum = float(df.loc[up_mask, "round_amount_usd"].sum())
    down_sum = float(df.loc[down_mask, "round_amount_usd"].sum())
    return up_sum, down_sum


def build_comparison() -> pd.DataFrame:
    """Compute upstream/downstream amounts for specialized investors, grouped by specialization bins.

    Bins: Normal investors (non-specialized, flag = 0), then 0.2–0.4, 0.4–0.6,
    0.6–0.8, 0.8–1.0 (inclusive of 1.0) for specialized investors.
    Returns a dataframe with columns [Group, upstream, downstream] in USD.
    """
    # Load investors and compute specialization flags and ratios (window starting 2015)
    investors = mylib.openDB("investors")
    investors = spaceSpecialization(investors, 2015, 0.2)
    investors = spacePercentage(investors, 2015, 0.2)

    # Split investors: non‑specialized vs specialized
    inv_normal = investors[investors["investor_flag_space"] == 0].copy()
    inv_spec = investors[investors["investor_flag_space"] == 1].copy()

    # Build bins over the ratio [0.2, 1.0]
    edges = [0.2, 0.4, 0.6, 0.8, 1.0000001]
    labels = ["20%-40%", "40%-60%", "60%-80%", "80%-100%"]
    inv_spec["ratio_bin"] = pd.cut(inv_spec["space_percentage"].clip(lower=0.0, upper=1.0), bins=edges, labels=labels, right=False, include_lowest=True)

    # Load rounds and merge up/down labels
    rounds = mylib.openDB("rounds")
    merged = _prepare_updown_merged(rounds)

    results = []

    # Normal investors (non-specialized) across the full dataset
    normal_ids = set(inv_normal["investor_id"].dropna())
    df_normal = merged[merged["investor_id"].isin(normal_ids)]
    up_sum, down_sum = _sum_up_down(df_normal)
    results.append(["Normal investors", up_sum, down_sum])

    # Per ratio bin
    for label in labels:
        ids = set(inv_spec.loc[inv_spec["ratio_bin"] == label, "investor_id"].dropna())
        df_bin = merged[merged["investor_id"].isin(ids)]
        up_sum, down_sum = _sum_up_down(df_bin)
        results.append([label, up_sum, down_sum])

    out = pd.DataFrame(results, columns=["Group", "upstream", "downstream"])    
    return out


def plot_comparison(df: pd.DataFrame) -> None:
    if df.empty:
        print("No data available for the requested comparison.")
        return

    # Compute shares per group and plot 100% stacked bars
    plot_df = df.copy()
    plot_df["Total"] = plot_df["upstream"].fillna(0) + plot_df["downstream"].fillna(0)
    eps = 1e-12
    plot_df["upstream_share"] = np.where(plot_df["Total"] > eps, plot_df["upstream"] / plot_df["Total"], 0.0)
    plot_df["downstream_share"] = np.where(plot_df["Total"] > eps, plot_df["downstream"] / plot_df["Total"], 0.0)

    x = np.arange(len(plot_df))
    width = 0.6

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.bar(x, plot_df["upstream_share"], width, label="upstream", color="#4C78A8")
    ax.bar(x, plot_df["downstream_share"], width, bottom=plot_df["upstream_share"], label="downstream", color="#F58518")

    ax.set_xticks(x)
    ax.set_xticklabels(plot_df["Group"], rotation=30, ha="right")
    ax.set_ylabel("Share of investment")
    ax.yaxis.set_major_formatter(PercentFormatter(1.0))
    ax.set_title("upstream vs downstream share by specialization ratio (100% stacked)")
    ax.legend()

    # Annotate segments with percentages
    for idx, (u, d) in enumerate(zip(plot_df["upstream_share"], plot_df["downstream_share"])):
        if u > 0.03:
            ax.text(idx, u / 2, f"{u*100:.0f}%", ha="center", va="center", color="white", fontsize=9)
        if d > 0.03:
            ax.text(idx, u + d / 2, f"{d*100:.0f}%", ha="center", va="center", color="black", fontsize=9)

    plt.tight_layout()
    plt.show()


def main() -> None:
    df = build_comparison()
    plot_comparison(df)


if __name__ == "__main__":
    main()
