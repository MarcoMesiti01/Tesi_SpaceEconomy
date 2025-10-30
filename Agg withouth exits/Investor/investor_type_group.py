import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 18,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
})


df_round = mylib.openDB("rounds")
df_inv = mylib.openDB("investors")
db_exp=pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id","company_all_tags"])
db_exp=mylib.space(db_exp, "company_id", True)
db_exp=db_exp["company_id"]
df_round=df_round[df_round["company_id"].isin(db_exp)]


df_round_inv = pd.merge(
    left=df_round,
    right=df_inv,
    how="left",
    left_on="investor_id",
    right_on="investor_id",
)
print(df_round_inv.size)
df_round_inv = mylib.filterExits(df_round_inv)
df_round_inv = df_round_inv[["investor_types", "round_amount_usd"]]
df_round_inv=df_round_inv[(~df_round_inv["investor_types"].str.contains("Venture capital", case=False, na=False))&(~df_round_inv["investor_types"].str.contains("venture_capital", case=False, na=False))]
print(df_round_inv.size)

df_round_inv = df_round_inv[df_round_inv["round_amount_usd"] != 0]
df_round_inv = df_round_inv[(df_round_inv["investor_types"].notna()) & (~df_round_inv["investor_types"].str.contains("Not defined", case=False, na=False))]

# split multi-category investors and apportion funding equally across categories
df_round_inv["investor_types"] = (
    df_round_inv["investor_types"]
    .astype(str)
    .str.split(",")
    .apply(lambda types: [t.strip() for t in types if t.strip()])
)
df_round_inv = df_round_inv[df_round_inv["investor_types"].map(len) > 0]

df_round_inv["split_count"] = df_round_inv["investor_types"].map(len)
df_round_inv["round_share"] = 1 / df_round_inv["split_count"]
df_round_inv["round_amount_usd"] = df_round_inv["round_amount_usd"] / df_round_inv["split_count"]

df_round_inv = df_round_inv.explode("investor_types")
df_round_inv["investor_types"] = df_round_inv["investor_types"].str.strip()
df_round_inv.drop(columns="split_count", inplace=True)

df_round_inv = df_round_inv.reset_index(drop=True)
print(df_round_inv[:10])

# pre-compute aggregates so every chart shares the same base numbers
df_round_inv_agg = (
    df_round_inv.groupby("investor_types", as_index=False)
    .agg(round_share=("round_share", "count"), total_amount=("round_amount_usd", "sum"))
)
df_round_inv_agg.rename(columns={"round_share": "count", "total_amount": "sum"}, inplace=True)
df_round_inv_agg["mean"] = df_round_inv_agg["sum"] / df_round_inv_agg["count"].replace(0, pd.NA)
df_round_inv_agg["mean"] = df_round_inv_agg["mean"].fillna(0)
df_round_inv_agg["mean"]=df_round_inv_agg["count"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
df_round_inv_agg["sum"]=df_round_inv_agg["sum"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)


df_round_inv_number = df_round_inv_agg.sort_values(by="count").tail(13)[["investor_types", "count"]].copy()
print(df_round_inv_number)
df_round_inv_amount = df_round_inv_agg.sort_values(by="sum").tail(13)[["investor_types", "sum"]].copy()
df_round_inv_mean = df_round_inv_agg.sort_values(by="mean")[["investor_types", "mean"]].copy()


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
    ax_primary.bar(plot_df["investor_types"], plot_df[value_column], color="steelblue", label=y_label)
    ax_primary.set_xlabel("investor_types")
    ax_primary.set_ylabel(y_label)
    ax_primary.tick_params(axis="x", rotation=90)
    ax_primary.set_title(title)

    ax_secondary = ax_primary.twinx()
    ax_secondary.plot(
        plot_df["investor_types"],
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
