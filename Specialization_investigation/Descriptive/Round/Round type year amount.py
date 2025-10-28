import pandas as pd
import matplotlib.pyplot as plt
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spaceSpecialization


ROUNDS_PATH = "DB_Out/DB_rounds.parquet"
EXPORT_PATH = "DB_Out/DB_export.parquet"

def get_space_rounds() -> pd.DataFrame:
    """Return investment rounds limited to space companies."""
    columns_rounds = ["company_id", "Round type", "Round date", "round_amount_usd", "investor_id"]
    rounds = pd.read_parquet(ROUNDS_PATH, columns=columns_rounds)
    investor=mylib.openDB("investors")
    investor=spaceSpecialization(investor, 2015, 0.2)
    investor=investor[(investor["investor_flag_space"]==1) & (investor["investor_flag_venture_capital"]==1)]["ID"]
    rounds=rounds[(rounds["Round date"]>pd.to_datetime("2010")) & (rounds["investor_id"].isin(investor))]

    export_columns = ["company_id", "company_all_tags"]
    companies = pd.read_parquet(EXPORT_PATH, columns=export_columns)
    #companies = mylib.space(companies, "company_id",True)
    space_ids = companies["company_id"].dropna().astype(str)

    rounds = rounds.dropna(subset=["company_id"])
    rounds["company_id"] = rounds["company_id"].astype(str)
    rounds = rounds[rounds["company_id"].isin(set(space_ids))]
    return rounds

def _parse_round_date(value) -> pd.Timestamp:
    """Normalise date values through the helper in Library, returning NaT when parsing fails."""
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value
    value_str = str(value).strip()
    if value_str == "":
        return pd.NaT
    lowered = value_str.lower()
    if lowered in {"nan", "nat", "none"}:
        return pd.NaT
    try:
        return mylib.convertToDatetime(lowered)
    except Exception:
        return pd.NaT

def prepare_yearly_amounts(rounds: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total USD invested by year and round type, excluding exits."""
    filtered = mylib.filterExits(rounds.copy())
    filtered["round_amount_usd"] = pd.to_numeric(filtered["round_amount_usd"], errors="coerce")
    filtered["Round date"] = filtered["Round date"].apply(_parse_round_date)
    filtered = filtered.dropna(subset=["Round type", "Round date", "round_amount_usd"])
    filtered["Year"] = filtered["Round date"].dt.year.astype(int)

    yearly_amounts = (
        filtered.groupby(["Year", "Round type"], as_index=False)["round_amount_usd"].sum()
    )
    return yearly_amounts

def plot_yearly_round_type(aggregated: pd.DataFrame) -> None:
    if aggregated.empty:
        print("No data available after filtering for space companies and removing exits.")
        return

    pivot = aggregated.pivot(index="Year", columns="Round type", values="round_amount_usd").fillna(0)
    pivot.sort_index(inplace=True)

    # Order stacked bars by total volume so the legend highlights the largest contributors first.
    totals = pivot.sum(axis=0).sort_values(ascending=False)
    pivot = pivot[totals.index]
    pivot_billions = pivot / 1_000_000_000

    fig, ax = plt.subplots(figsize=(14, 8))
    bottom = pd.Series(0.0, index=pivot_billions.index)
    for round_type in pivot_billions.columns:
        ax.bar(
            pivot_billions.index,
            pivot_billions[round_type],
            bottom=bottom,
            label=round_type,
        )
        bottom += pivot_billions[round_type]

    ax.set_xlabel("Year")
    ax.set_ylabel("Total investment (USD billions)")
    ax.set_title("Space investments by round type per year (exits excluded)")
    ax.set_xticks(pivot_billions.index)
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), title="Round type")
    ax.tick_params(axis="x", rotation=90)

    plt.tight_layout()
    plt.show()

def main() -> None:
    rounds = get_space_rounds()
    aggregated = prepare_yearly_amounts(rounds)
    plot_yearly_round_type(aggregated)

main()
