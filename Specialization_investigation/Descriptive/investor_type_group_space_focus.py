import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt


df_round = mylib.openDB("rounds")
df_inv = mylib.openDB("investors")
df_inv=df_inv[(df_inv["Flag space"]==1) & (df_inv["Venture capital flag"]==1)].copy()
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp, "company_id", True)
db_exp=db_exp["company_id"]
df_round=df_round[df_round["Target firm ID"].isin(db_exp)]


df_round_inv = pd.merge(
    left=df_round,
    right=df_inv,
    how="left",
    left_on="Investor ID",
    right_on="ID",
)
print(df_round_inv.size)
df_round_inv = mylib.filterExits(df_round_inv)
df_round_inv = df_round_inv[["Investor type", "AmountUSD"]]
print(df_round_inv.size)

df_round_inv = df_round_inv[df_round_inv["AmountUSD"] != 0]
df_round_inv = df_round_inv[df_round_inv["Investor type"].notna()]

# split multi-category investors and apportion funding equally across categories
df_round_inv["Investor type"] = (
    df_round_inv["Investor type"]
    .astype(str)
    .str.split(",")
    .apply(lambda types: [t.strip() for t in types if t.strip()])
)
df_round_inv = df_round_inv[df_round_inv["Investor type"].map(len) > 0]

df_round_inv["split_count"] = df_round_inv["Investor type"].map(len)
df_round_inv["round_share"] = 1 / df_round_inv["split_count"]
df_round_inv["AmountUSD"] = df_round_inv["AmountUSD"] / df_round_inv["split_count"]

df_round_inv = df_round_inv.explode("Investor type")
df_round_inv["Investor type"] = df_round_inv["Investor type"].str.strip()
df_round_inv.drop(columns="split_count", inplace=True)

df_round_inv = df_round_inv.reset_index(drop=True)
print(df_round_inv[:10])

# pre-compute aggregates so every chart shares the same base numbers
df_round_inv_agg = (
    df_round_inv.groupby("Investor type", as_index=False)
    .agg(round_share=("round_share", "count"), total_amount=("AmountUSD", "sum"))
)
df_round_inv_agg.rename(columns={"round_share": "count", "total_amount": "sum"}, inplace=True)
df_round_inv_agg["mean"] = df_round_inv_agg["sum"] / df_round_inv_agg["count"].replace(0, pd.NA)
df_round_inv_agg["mean"] = df_round_inv_agg["mean"].fillna(0)
df_round_inv_agg["mean"]=df_round_inv_agg["count"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
df_round_inv_agg["sum"]=df_round_inv_agg["sum"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)


df_round_inv_number = df_round_inv_agg.sort_values(by="count").tail(13)[["Investor type", "count"]].copy()
print(df_round_inv_number)
df_round_inv_amount = df_round_inv_agg.sort_values(by="sum").tail(13)[["Investor type", "sum"]].copy()
df_round_inv_mean = df_round_inv_agg.sort_values(by="mean")[["Investor type", "mean"]].copy()


def add_percentage(df, value_column):
    total = df[value_column].sum()
    if total == 0:
        df["percentage"] = 0
    else:
        df["percentage"] = df[value_column] / total * 100
    return df


def plot_bar_with_share(df, value_column, title, y_label):
    plot_df = add_percentage(df.copy(), value_column)
    fig, ax_primary = plt.subplots(figsize=(10, 6))
    ax_primary.bar(plot_df["Investor type"], plot_df[value_column], color="steelblue", label=y_label)
    ax_primary.set_xlabel("Investor type")
    ax_primary.set_ylabel(y_label)
    ax_primary.tick_params(axis="x", rotation=90)
    ax_primary.set_title(title)

    ax_secondary = ax_primary.twinx()
    ax_secondary.plot(
        plot_df["Investor type"],
        plot_df["percentage"],
        color="darkorange",
        marker="o",
        label="Share of total (%)",
    )
    ax_secondary.set_ylabel("Share of total (%)")
    ax_secondary.set_ylim(0, max(100, plot_df["percentage"].max() * 1.1))

    handles_primary, labels_primary = ax_primary.get_legend_handles_labels()
    handles_secondary, labels_secondary = ax_secondary.get_legend_handles_labels()
    ax_primary.legend(handles_primary + handles_secondary, labels_primary + labels_secondary, loc="upper left")

    plt.tight_layout()
    plt.show()


plot_bar_with_share(
    df_round_inv_number,
    "count",
    "Number of rounds funded by each investor category",
    "Number of rounds",
)

plot_bar_with_share(
    df_round_inv_amount,
    "sum",
    "Amount invested for each investor type category",
    "Amount invested (B USD)",
)

plot_bar_with_share(
    df_round_inv_mean,
    "mean",
    "Average round size by each investor category",
    "Average round size (B USD)",
)
