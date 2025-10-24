import pandas as pd
import matplotlib.pyplot as plt
import Library as mylib
import Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec as flagS


def main() -> None:
    # Load DB tables
    rounds = mylib.openDB("rounds")
    investors = mylib.openDB("investors")

    # Flag space‑specialized investors and keep only VC + specialized
    # Thresholds aligned with the yearly script in this folder
    investors = flagS.spaceSpecialization(investors, 2015, 0.2)
    inv_ids = investors[(investors["investor_flag_space"] == 1) & (investors["investor_flag_venture_capital"] == 1)]

    # In some DB exports the id column is named "ID" (string) while rounds uses "investor_id"
    # Prefer "ID" when present to match existing scripts; fallback to investor_id
    if "ID" in inv_ids.columns:
        inv_ids = set(inv_ids["ID"].dropna())
    else:
        inv_ids = set(inv_ids["investor_id"].dropna())

    # Filter rounds to those by specialized investors and sensible date range
    cols_needed = [c for c in ["investor_id", "round_label", "Round date", "round_amount_usd"] if c in rounds.columns]
    df = rounds[cols_needed].copy()

    if "investor_id" in df.columns:
        df = df[df["investor_id"].isin(inv_ids)]

    # Optional time filter (keeps post-2010 as in related analyses)
    if "Round date" in df.columns:
        # Robust parse via Library helper
        def _parse_round_date(v):
            if pd.isna(v):
                return pd.NaT
            if isinstance(v, pd.Timestamp):
                return v
            try:
                return mylib.convertToDatetime(str(v).lower())
            except Exception:
                return pd.NaT

        df["Round date"] = df["Round date"].apply(_parse_round_date)
        df = df[df["Round date"] > pd.to_datetime("2010")]

    # Normalize amounts and exclude exit rounds
    if "round_amount_usd" in df.columns:
        df["round_amount_usd"] = pd.to_numeric(df["round_amount_usd"], errors="coerce")
    else:
        # If amount column is missing, nothing to aggregate
        print("Column 'round_amount_usd' missing in rounds DB; cannot aggregate amounts.")
        return

    df = mylib.filterExits(df)
    df = df.dropna(subset=["round_label", "round_amount_usd"])  # ensure valid rows

    # Aggregate total amount by round_label
    totals = (
        df.groupby("round_label", as_index=False)["round_amount_usd"].sum()
        .sort_values("round_amount_usd", ascending=False)
    )

    if totals.empty:
        print("No data available after filtering specialized investors and exits.")
        return

    # Shorten long label for readability
    totals["round_label"] = totals["round_label"].mask(
        totals["round_label"] == "PROJECT, REAL ESTATE, INFRASTRUCTURE FINANCE",
        other="PROJ, RE, IF",
    )

    # Keep only top 10 round_labels by invested amount
    top10 = totals.head(10).copy()

    # Plot as horizontal bar chart in USD billions
    top10["amount_billions"] = top10["round_amount_usd"] / 1_000_000_000
    top10.sort_values("amount_billions", ascending=True, inplace=True)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top10["round_label"], top10["amount_billions"], color="steelblue")
    ax.set_xlabel("Total invested (USD billions)")
    ax.set_ylabel("round_label")
    ax.set_title("Top 10 round_labels by amount — space‑specialized investors")

    # Annotate bars with values
    for i, v in enumerate(top10["amount_billions"]):
        ax.text(v, i, f" {v:.1f}", va="center")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()

