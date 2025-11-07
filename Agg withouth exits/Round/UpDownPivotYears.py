from pathlib import Path

import pandas as pd

_CURRENT_FILE = Path(__file__).resolve()
_CURRENT_DIR = _CURRENT_FILE.parent
_ALLOWED_TABLES = {"investors", "rounds", "valuation", "export", "updown"}


def _find_db_out_dir(start: Path | None = None) -> Path:
    """
    Locate the nearest 'DB_Out' directory walking up from this file.
    Mirrors the logic from Library.openDB but avoids third-party dependencies.
    """
    start_path = start or _CURRENT_FILE
    start_dir = start_path if start_path.is_dir() else start_path.parent
    for base in (start_dir, *start_dir.parents):
        candidate = base / "DB_Out"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("DB_Out directory not found in the project tree.")


def _open_db_table(name: str, *, db_dir: Path | None = None) -> pd.DataFrame:
    """Read `DB_<name>.parquet` from DB_Out with validation."""
    if not isinstance(name, str):
        raise TypeError("Table name must be a string.")

    key = name.strip().lower()
    if key not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table '{name}'. Allowed: {sorted(_ALLOWED_TABLES)}")

    db_dir = db_dir or _find_db_out_dir(start=_CURRENT_DIR)
    parquet_path = db_dir / f"DB_{key}.parquet"
    if not parquet_path.is_file():
        available = ", ".join(p.name for p in sorted(db_dir.glob('*.parquet')))
        raise FileNotFoundError(
            f"Expected '{parquet_path.name}' in {db_dir}."
            f" Available: {available if available else 'none'}"
        )
    return pd.read_parquet(parquet_path)


def main() -> None:
    """
    Produce yearly investment totals (USD) in space companies located in Europe
    and the United States, split by upstream and downstream segments, and export
    the pivoted table to Excel.
    """
    db_dir = _find_db_out_dir()

    rounds = _open_db_table("rounds", db_dir=db_dir)
    rounds["round_date"] = pd.to_datetime(rounds["round_date"], errors="coerce")

    firms = pd.read_parquet(
        db_dir / "DB_firms.parquet", columns=["company_id", "company_continent", "company_country"]
    )
    classifications = _open_db_table("updown", db_dir=db_dir)[
        ["company_id", "upstream", "downstream", "space"]
    ]

    merged = (
        rounds.merge(firms, on="company_id", how="left", suffixes=("", "_firm"))
        .merge(classifications, on="company_id", how="left")
    )

    filtered = merged[(merged["space"] == 1) & merged["round_date"].notna()].copy()
    filtered[["upstream", "downstream"]] = filtered[["upstream", "downstream"]].fillna(0)

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
        value_vars=["upstream", "downstream"],
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
        ("Europe", "downstream"),
        ("Europe", "upstream"),
        ("United States", "downstream"),
        ("United States", "upstream"),
    ]
    pivot = pivot.reindex(row_order, fill_value=0)
    pivot = pivot.reindex(columns=desired_years, fill_value=0).astype(float)

    output_path = db_dir / "space_investments_us_eu.xlsx"
    pivot.to_excel(output_path, sheet_name="Investments")

    display_df = pivot.copy()
    display_df.index = [f"{region} - {segment}" for region, segment in display_df.index]

    pd.options.display.float_format = "{:,.0f}".format
    print(display_df.to_string())
    print(f"\nSaved pivot table to {output_path}")


if __name__ == "__main__":
    main()
