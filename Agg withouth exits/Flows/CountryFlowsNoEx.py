import pandas as pd
from pathlib import Path
import Library as mylib

def addPercentage(df : pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        df[column]=pd.to_numeric(df[column], errors="coerce")
    total=df.apply(sum, axis=1).sum()
    df["Row percentage"]=df.apply(lambda x : (sum(x)/total)*100, axis=1)
    append=dict()
    for column in df.columns:
        if column != "Row percentage":
            percentage=(df[column].sum()/total)*100
            append[column]=percentage
        else:
            append["Row percentage"]=100
    df.loc["Column percentage"]=append
    return df

def orderColumns(df: pd.DataFrame, focus_countries: list[str]) -> pd.DataFrame:
    ordered=list()
    for country in focus_countries:
        if country in df.columns:
            ordered.append(country)
    if "Others" in df.columns:
        ordered.append("Others")
    if "Row percentage" in df.columns and "Row percentage" not in ordered:
        ordered.append("Row percentage")
    df=df[ordered]
    return df

"""df_inv_loc=pd.read_parquet("DB_Out/DB_inv_loc.parquet")
df_round=pd.read_parquet("DB_Out/RoundSplit.parquet")
df_inv=pd.read_parquet("DB_Out/DB_investors.parquet")

#excluding non exits and setting the EU as a single country
df_round.rename(columns={"company_country" : "Country"}, inplace=True)
df_round=mylib.toEU(df_round)
df_round=mylib.filterExits(df_round)
df_round.rename(columns={"Country" : "company_country"}, inplace=True)
print(df_round[df_round["company_country"]=="EU"])

#polishing locations to make 1 investor 1 country
#df_inv_loc=mylib.polish_loc(df_inv_loc)
df_inv_loc=df_inv_loc[["investor_id", "Country"]]
df_inv_loc=mylib.toEU(df_inv_loc)
df_inv_loc.rename(columns={"Country" : "Investor country"}, inplace=True)

#merging the two dataset
df=pd.merge(left=df_round, right=df_inv_loc, how="left", on="investor_id")"""

df=mylib.openDB("export")
df=mylib.space(df, "company_id", True)
df=df[df["company_country"].notna()]
df=df[["round_id","company_id", "company_country", "round_label","round_amount_usd","investor_id","investor_country"]].copy()
df.columns=["Round ID", "Firm ID", "company_country", "Round type", "round_amount_usd","investor_id","Investor country"]
df=mylib.filterExits(df)
"""df.rename(columns={"company_country" : "Country"}, inplace=True)
df=mylib.toEU(df)
df.rename(columns={"Country" : "company_country", "Investor country" : "Country"}, inplace=True)
df=mylib.toEU(df)
df.rename(columns={"Country" : "Investor country"}, inplace=True)"""
investor_counts=df.groupby("Round ID")["investor_id"].transform(lambda s: s.dropna().nunique())
investor_counts=investor_counts.replace(0, pd.NA).fillna(1)
df["amount_allocated_usd"]=df["round_amount_usd"]/investor_counts
df["amount_allocated_usd"]=df["amount_allocated_usd"].fillna(0)
df["amount_allocated_busd"]=df["amount_allocated_usd"]/1_000_000_000

#selecting the top 5 countries by amount received (space-tagged companies only)
df_top_countries=(df[["company_country", "amount_allocated_busd"]]
                  .groupby("company_country")
                  .sum()
                  .sort_values(by="amount_allocated_busd", ascending=False)
                  .head(5))
focus_countries=df_top_countries.index.to_list()

#changing the countries that are not in the focus list to "Others"
df["company_country"]=df["company_country"].where(df["company_country"].isin(focus_countries), other="Others")
df["Investor country"]=df["Investor country"].where(df["Investor country"].isin(focus_countries), other="Others")

#creating a matrix 
mat=df.pivot_table(index="company_country", columns="Investor country", values="amount_allocated_busd", aggfunc="sum", fill_value=0)
#ensuring the matrix shows the same focus countries on rows and columns
ordered_index=list(focus_countries)
if "Others" in mat.index:
    ordered_index.append("Others")
mat=mat.reindex(index=ordered_index, fill_value=0)
ordered_columns=list(focus_countries)
if "Others" in mat.columns:
    ordered_columns.append("Others")
mat=mat.reindex(columns=ordered_columns, fill_value=0)
mat=addPercentage(mat)
mat=orderColumns(mat, focus_countries)
output_path=Path(__file__).with_suffix(".xlsx")
output_path.parent.mkdir(parents=True, exist_ok=True)
mat.to_excel(output_path)
print(f"Matrix saved to {output_path}")
print(mat.to_string())
