import pandas as pd
import Library as mylib
import plotly.express as px

#doing an aggregation with the following columns: investor_types, Upstream, Downstream, Other
df_temp=mylib.openDB("export")
df_inv=mylib.openDB("investors")
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
df_fin=pd.merge(left=df_round, right=df_temp, how="left", left_on="company_id", right_on="ID")
df_fin=df_fin[["investor_id", "company_id", "Class", "round_amount_usd"]]
df_fin["round_amount_usd"]=df_fin["round_amount_usd"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)
df_fin=pd.merge(left=df_fin, right=df_inv, how="left", on="investor_id")
df_fin=df_fin[["investor_types", "Class", "round_amount_usd"]]

# split multi-category investors and share the amount evenly across categories
df_fin = df_fin[df_fin["round_amount_usd"].notna()]
df_fin = df_fin[df_fin["investor_types"].notna()]
df_fin["investor_types"] = (
    df_fin["investor_types"]
    .astype(str)
    .str.split(",")
    .apply(lambda types: [t.strip() for t in types if t.strip()])
)
df_fin = df_fin[df_fin["investor_types"].map(len) > 0]
df_fin["split_count"] = df_fin["investor_types"].map(len)
df_fin["round_amount_usd"] = df_fin["round_amount_usd"] / df_fin["split_count"]
df_fin = df_fin.explode("investor_types")
df_fin["investor_types"] = df_fin["investor_types"].str.strip()
df_fin.drop(columns="split_count", inplace=True)

df_fin=df_fin[df_fin["round_amount_usd"] != 0]

#creating the dataframe for up, down, other
pivot=df_fin.pivot_table(index="investor_types", columns="Class", values="round_amount_usd", aggfunc="sum", fill_value=0)
pivot.sort_values(by="Downstream", inplace=True, ascending=False)
pivot=pivot[["Downstream", "Upstream", "Other"]]
print(pivot)

#plotting it
pivot["Total"]=pivot[["Downstream", "Upstream", "Other"]].sum(axis=1)
pivot.sort_values(by="Total", inplace=True, ascending=False)
chart_data=pivot.head(10).reset_index()
investor_order = chart_data["investor_types"].tolist()

chart_long = chart_data.melt(
    id_vars="investor_types",
    value_vars=["Upstream", "Downstream", "Other"],
    var_name="Investment split",
    value_name="round_amount_usd"
)
chart_long["investor_types"] = pd.Categorical(
    chart_long["investor_types"], categories=investor_order, ordered=True
)

fig = px.bar(
    chart_long,
    x="investor_types",
    y="round_amount_usd",
    color="Investment split",
    title="Top 10 investor_typess by investment composition",
    labels={
        "investor_types": "investor_types",
        "round_amount_usd": "Amount invested (B USD)",
        "Investment split": "Investment category",
    },
)
fig.update_layout(barmode="stack", xaxis_title="investor_types", yaxis_title="Amount invested (B USD)")
fig.show()
