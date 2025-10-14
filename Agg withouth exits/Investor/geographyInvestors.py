import pandas as pd
import Library as mylib
import plotly.express as px
import plotly.graph_objects as go

df_loc=pd.read_parquet("DB_Out/DB_inv_loc.parquet")
df_exp=pd.read_parquet("DB_Out/DB_export.parquet")
df_exp=mylib.space(df_exp)
df_exp=df_exp["investor_id"].drop_duplicates()
df_loc=df_loc[df_loc["Investor ID"].isin(df_exp)]
df_loc=df_loc[["Investor ID", "Country"]]
df_loc=df_loc.groupby("Country").count()
df_loc.reset_index(inplace=True)
df_loc["Country"]=df_loc["Country"].apply(mylib.to_iso3)
listColumns=["Country", "Investor ID"]
df_loc[["Country", "Investor ID"]].drop_duplicates(inplace=True)
fig=px.choropleth(df_loc, locations="Country", color="Investor ID", hover_name="Country", color_continuous_scale="Reds", projection="natural earth")
fig.update_layout(title="Investors per Country", coloraxis_colorbar_title="Value", margin=dict(l=0, r=0, t=40, b=0),)
for i, row in df_loc.iterrows():
    fig.add_trace(go.Scattergeo(locationmode="country names", locations=[row["Country"]], text=[round(row["Investor ID"])], mode="text", showlegend=False ))
fig.show() 