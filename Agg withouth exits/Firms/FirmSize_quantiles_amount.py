import pandas as pd
import numpy as np
from pathlib import Path
import Library as mylib


def get_last_employee(x: str) -> str:
    """Return the latest (2024) employee value; if 'n/a' return 0.

    The input is a comma-separated history where the last entry corresponds
    to the most recent year (2024). We do not backfill from earlier years.
    """
    if not x or pd.isna(x):
        return "0"
    parts = str(x).split(",")
    last = parts[-1].strip() if parts else ""
    if last == "n/a" or last == "":
        return "0"
    return last


def load_space_firms_with_size() -> pd.DataFrame:
    df_f = pd.read_parquet("DB_Out/DB_firms.parquet", columns=["company_id", "employee_number"])
    df_f["employee_number"] = df_f["employee_number"].apply(get_last_employee)
    df_f["employee_number"] = pd.to_numeric(df_f["employee_number"], errors="coerce")
    # space-only view identical to FirmSize.py
    df_f = mylib.space(df_f, "company_id", True)
    return df_f[["company_id", "employee_number"]].copy()


def load_space_round_amounts() -> pd.DataFrame:
    # rounds are per-investor shares; summing by company_id yields total capital raised
    df_r = mylib.openDB("rounds")
    df_r = mylib.space(df_r, "company_id", True)
    # keep necessary columns and sum in USD
    df_amt = (
        df_r[["company_id", "round_amount_usd"]]
        .dropna(subset=["company_id"])  # guard
        .groupby("company_id", as_index=False)["round_amount_usd"].sum()
    )
    df_amt.rename(columns={"round_amount_usd": "amount_usd"}, inplace=True)
    return df_amt


def _quantile_label(q: float) -> str:
    pct = q * 100
    if abs(pct - round(pct)) < 1e-9:
        return f"{int(round(pct))}%"
    return f"{pct:.1f}%"


def build_quantile_table(df_firms: pd.DataFrame, df_amounts: pd.DataFrame,
                         quantiles: list[float]) -> pd.DataFrame:
    # compute quantile thresholds on 2024 employee counts
    s = df_firms["employee_number"].astype(float)
    q_series = s.quantile(quantiles)
    q_vals = q_series.values.astype(float)

    # merge firm sizes with amounts (per company total)
    df_company = df_firms.copy().merge(df_amounts, on="company_id", how="left")
    df_company["amount_usd"].fillna(0, inplace=True)

    # compute sums for each original quantile interval to avoid duplicate-edge errors
    col_labels = [_quantile_label(q) for q in quantiles]
    sums_by_quant = {}
    counts_by_quant = {}
    sizes = df_company["employee_number"].astype(float)
    prev_upper = -np.inf
    for label, upper in zip(col_labels, q_vals):
        mask = (sizes > prev_upper) & (sizes <= upper)
        sums_by_quant[label] = float(df_company.loc[mask, "amount_usd"].sum()) / 1_000_000_000
        counts_by_quant[label] = int(mask.sum())
        prev_upper = upper

    # assemble 2-row table
    first_row = {col: val for col, val in zip(col_labels, q_vals)}
    second_row = {col: sums_by_quant.get(col, 0.0) for col in col_labels}
    third_row = {col: counts_by_quant.get(col, 0) for col in col_labels}
    table = pd.DataFrame([first_row, second_row, third_row])
    table.index = ["quantile_value", "amount_busd", "firm_count"]
    return table


def main():
    quantiles = [0.25, 0.5, 0.75, 0.9, 0.99, 0.999, 1.0]
    df_firms = load_space_firms_with_size()
    df_amounts = load_space_round_amounts()
    table = build_quantile_table(df_firms, df_amounts, quantiles)

    out_dir = Path("DB_Out")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "amount_by_employee_quantiles_space.csv"
    xlsx_path = out_dir / "amount_by_employee_quantiles_space.xlsx"
    table.to_csv(csv_path)
    try:
        table.to_excel(xlsx_path)
    except Exception:
        pass

    # also print for quick inspection
    print(table)


if __name__ == "__main__":
    main()
