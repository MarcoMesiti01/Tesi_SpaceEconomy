import pandas as pd
import Library as mylib


def main() -> None:
    """
    Produce yearly investment totals (USD) in space companies located in Europe
    and the United States, split by upstream and downstream segments, and export
    the pivoted table to Excel.
    """
    rounds = mylib.openDB("rounds")
    firms = pd.read_parquet(
        "DB_Out/DB_firms.parquet", columns=["company_id", "company_continent", "company_country"]
    )
    classifications = mylib.openDB("updown")[["Upstream", "Downstream", "Space"]]

    merged = (
        rounds.merge(firms, on="company_id", how="left", suffixes=("", "_firm"))
        .merge(classifications, left_on="company_id", right_index=True, how="left")
    )

    filtered = merged[(merged["Space"] == 1) & merged["round_date"].notna()].copy()
    filtered[["Upstream", "Downstream"]] = filtered[["Upstream", "Downstream"]].fillna(0)

    filtered["region"] = pd.NA
    filtered.loc[filtered["company_continent"] == "Europe", "region"] = "Europe"
    filtered.loc[
        filtered["company_country"].eq("United States")
        | filtered["company_country_firm"].eq("United States"),
        "region",
    ] = "United States"

    filtered = filtered[filtered["region"].notna()].copy()
    filtered["year"] = filtered["round_date"].dt.year
    filtered["round_amount_usd"] = filtered["round_amount_usd"].fillna(0)

    long_df = filtered.melt(
        id_vars=["year", "region", "round_amount_usd"],
        value_vars=["Upstream", "Downstream"],
        var_name="segment",
        value_name="flag",
    )
    long_df = long_df[long_df["flag"] == 1]

    aggregated = long_df.groupby(
        ["year", "region", "segment"], as_index=False
    )["round_amount_usd"].sum()

    desired_years = list(range(2010, 2026))
    aggregated = aggregated[aggregated["year"].isin(desired_years)]

    pivot = aggregated.pivot_table(
        index=["region", "segment"],
        columns="year",
        values="round_amount_usd",
        fill_value=0,
        aggfunc="sum",
    )

    row_order = [
        ("Europe", "Downstream"),
        ("Europe", "Upstream"),
        ("United States", "Downstream"),
        ("United States", "Upstream"),
    ]
    pivot = pivot.reindex(row_order, fill_value=0)
    pivot = pivot.reindex(columns=desired_years, fill_value=0).astype(float)

    output_path = "DB_Out/space_investments_us_eu.xlsx"
    pivot.to_excel(output_path, sheet_name="Investments")

    display_df = pivot.copy()
    display_df.index = [f"{region} - {segment}" for region, segment in display_df.index]

    pd.options.display.float_format = "{:,.0f}".format
    print(display_df.to_string())
    print(f"\nSaved pivot table to {output_path}")


if __name__ == "__main__":
    main()
