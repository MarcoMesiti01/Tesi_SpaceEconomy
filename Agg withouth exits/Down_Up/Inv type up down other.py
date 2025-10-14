import pandas as pd
import plotly.express as px
from pathlib import Path

import Library as mylib


def classify_investment(tags: object) -> str:
    if isinstance(tags, str):
        normalized = tags.lower()
        has_upstream = "upstream" in normalized
        has_downstream = "downstream" in normalized
        if has_upstream and not has_downstream:
            return "Upstream"
        if has_downstream and not has_upstream:
            return "Downstream"
    return "Other"


def main() -> None:
    df_inv = pd.read_parquet("DB_Out/DB_investors.parquet")
    df_round = pd.read_parquet("DB_Out/RoundSplit.parquet")
    df_temp = pd.read_parquet("DB_Out/DB_temp.parquet")

    df_round = mylib.filterExits(df_round)
    df_round["Amount in EUR"] = pd.to_numeric(df_round["Amount in EUR"], errors="coerce").fillna(0.0)

    tag_column = "Target tags"
    # Pull downstream/upstream labels from the main company table.
    merge_key = next((col for col in ["Target firm ID", "Firm ID"] if col in df_round.columns), None)

    if merge_key and {"ID", "Tags"}.issubset(df_temp.columns):
        tags_lookup = (
            df_temp[["ID", "Tags"]]
            .drop_duplicates(subset="ID")
            .rename(columns={"ID": merge_key, "Tags": tag_column})
        )
        df_round = df_round.merge(tags_lookup, on=merge_key, how="left")
    elif "Target firm" in df_round.columns and {"Name", "Tags"}.issubset(df_temp.columns):
        tags_lookup = (
            df_temp[["Name", "Tags"]]
            .drop_duplicates(subset="Name")
            .rename(columns={"Name": "Target firm", "Tags": tag_column})
        )
        df_round = df_round.merge(tags_lookup, on="Target firm", how="left")

    if tag_column not in df_round.columns:
        df_round[tag_column] = None

    df_round["Investment category"] = df_round[tag_column].apply(classify_investment)

    df_round = df_round.merge(df_inv[["Investor", "Investor type"]], on="Investor", how="left")
    df_round["Investor type"].fillna("Unknown", inplace=True)

    grouped = df_round.groupby(["Investor type", "Investment category"], dropna=False)["Amount in EUR"].sum()
    pivot = grouped.unstack(fill_value=0.0)

    for column in ["Upstream", "Downstream", "Other"]:
        if column not in pivot.columns:
            pivot[column] = 0.0
    pivot = pivot[["Upstream", "Downstream", "Other"]]

    pivot.reset_index(inplace=True)
    pivot.rename(columns={
        "Upstream": "Amount upstream",
        "Downstream": "Amount downstream",
        "Other": "Other"
    }, inplace=True)

    pivot["Total"] = pivot[["Amount upstream", "Amount downstream", "Other"]].sum(axis=1)
    pivot.sort_values(by="Total", ascending=False, inplace=True)

    top10 = pivot.head(10).copy()
    chart_data = top10.drop(columns="Total").copy()
    investor_order = chart_data["Investor type"].tolist()

    chart_long = chart_data.melt(
        id_vars="Investor type",
        value_vars=["Amount upstream", "Amount downstream", "Other"],
        var_name="Investment split",
        value_name="Amount in EUR"
    )
    chart_long["Investor type"] = pd.Categorical(
        chart_long["Investor type"], categories=investor_order, ordered=True
    )

    fig = px.bar(
        chart_long,
        x="Investor type",
        y="Amount in EUR",
        color="Investment split",
        title="Top 10 investor types by investment composition",
        labels={
            "Investor type": "Investor type",
            "Amount in EUR": "Amount invested (EUR)",
            "Investment split": "Investment category",
        },
    )
    fig.update_layout(barmode="stack", xaxis_title="Investor type", yaxis_title="Amount invested (EUR)")
    fig.show()

    pivot.drop(columns="Total", inplace=True)

    print(pivot)


if __name__ == "__main__":
    main()
