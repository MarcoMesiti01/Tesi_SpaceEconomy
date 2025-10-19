import pandas as pd
import Library as mylib
import plotly.express as px

#doing an aggregation with the following columns: Investor type, Upstream, Downstream, Other
df_temp=mylib.openDB("export")
df_inv=mylib.openDB("investors")
df_inv=df_inv[(df_inv["Flag space"]==1) & (df_inv["Venture capital flag"]==1)].copy()
df_round=mylib.openDB("rounds")


#classify investments down, up, other
def classify(row) -> str:
    if "Downstream" in row and "upstream" not in row:
        return "Downstream"
    elif "Upstream" in row and "downstream" not in row:
        return "Upstream"
    elif "Upstream" in row and "Downstream" in row: 
        return "Both"
    else:
        return "Other"

df_temp=df_temp[["company_name", "company_id", "company_all_tags"]]
df_temp=mylib.space(df_temp, "company_id", True)
df_temp=df_temp[["company_name","company_id","company_all_tags"]].copy()
df_temp.columns=["Name", "ID", "Tags"]
df_temp["Class"]=df_temp["Tags"].apply(lambda x: classify(x) if not pd.isna(x) else "Other")

#merging the information in the round dataframe
df_round=mylib.filterExits(df_round)
df_fin=pd.merge(left=df_round, right=df_temp, how="left", left_on="Target firm ID", right_on="ID")
df_fin=df_fin[["Investor", "Target firm ID", "Class", "AmountUSD"]]
df_fin["AmountUSD"]=df_fin["AmountUSD"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
df_fin=pd.merge(left=df_fin, right=df_inv, how="left", on="Investor")
df_fin=df_fin[["Investor type", "Class", "AmountUSD"]]

# split multi-category investors and share the amount evenly across categories
df_fin = df_fin[df_fin["AmountUSD"].notna()]
df_fin = df_fin[df_fin["Investor type"].notna()]
df_fin["Investor type"] = (
    df_fin["Investor type"]
    .astype(str)
    .str.split(",")
    .apply(lambda types: [t.strip() for t in types if t.strip()])
)
df_fin = df_fin[df_fin["Investor type"].map(len) > 0]
df_fin["split_count"] = df_fin["Investor type"].map(len)
df_fin["AmountUSD"] = df_fin["AmountUSD"] / df_fin["split_count"]
df_fin = df_fin.explode("Investor type")
df_fin["Investor type"] = df_fin["Investor type"].str.strip()
df_fin.drop(columns="split_count", inplace=True)

df_fin=df_fin[df_fin["AmountUSD"] != 0]

#creating the dataframe for up, down, other
pivot=df_fin.pivot_table(index="Investor type", columns="Class", values="AmountUSD", aggfunc="sum", fill_value=0)
pivot.sort_values(by="Downstream", inplace=True, ascending=False)
pivot=pivot[["Downstream", "Upstream", "Other"]]
print(pivot)

#plotting it
pivot["Total"]=pivot[["Downstream", "Upstream", "Other"]].sum(axis=1)
pivot.sort_values(by="Total", inplace=True, ascending=False)
chart_data=pivot.head(10).reset_index()
investor_order = chart_data["Investor type"].tolist()

chart_long = chart_data.melt(
    id_vars="Investor type",
    value_vars=["Upstream", "Downstream", "Other"],
    var_name="Investment split",
    value_name="AmountUSD"
)
chart_long["Investor type"] = pd.Categorical(
    chart_long["Investor type"], categories=investor_order, ordered=True
)

fig = px.bar(
    chart_long,
    x="Investor type",
    y="AmountUSD",
    color="Investment split",
    title="Top 10 investor types by investment composition",
    labels={
        "Investor type": "Investor type",
        "AmountUSD": "Amount invested (B USD)",
        "Investment split": "Investment category",
    },
)
fig.update_layout(barmode="stack", xaxis_title="Investor type", yaxis_title="Amount invested (B USD)")
fig.show()
