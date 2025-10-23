import pandas as pd
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

def orderColumns(df: pd.DataFrame) -> pd.DataFrame:
    listCol=list()
    for column in df.columns:
        if column!= "Others" and column!="Row percentage":
            listCol.append(column)
    listCol.append("Others")
    listCol.append("Row percentage")
    df=df[listCol]
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
df=df[df["company_continent"]=="Europe"]
df=df[["round_id","company_id", "company_country", "round_label","round_amount_usd","investor_id","investor_country"]].copy()
df.columns=["Round ID", "Firm ID", "company_country", "Round type", "round_amount_usd","investor_id","Investor country"]
df=mylib.filterExits(df)
"""df.rename(columns={"company_country" : "Country"}, inplace=True)
df=mylib.toEU(df)
df.rename(columns={"Country" : "company_country", "Investor country" : "Country"}, inplace=True)
df=mylib.toEU(df)
df.rename(columns={"Country" : "Investor country"}, inplace=True)"""
df["round_amount_usd"]=df["round_amount_usd"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)

#selecting the top 5 investing countries
df_top5inv=df[["Investor country", "round_amount_usd"]].groupby("Investor country").sum()
df_top5inv.reset_index(inplace=True)
df_top5inv.sort_values(by="round_amount_usd", ascending=False, inplace=True)
top5inv=df_top5inv["Investor country"].head(5).to_list()

#selecting the top 5 target countries
df_top5targ=df[["company_country", "round_amount_usd"]].groupby("company_country").sum()
df_top5targ.reset_index(inplace=True)
df_top5targ.sort_values(by="round_amount_usd", ascending=False, inplace=True)
top5targ=df_top5targ["company_country"].head(5).to_list()

#changing the countries that are not in the top 5 to "Other"
df["company_country"]=df["company_country"].where(df["company_country"].isin(top5targ), other="Others")
df["Investor country"]=df["Investor country"].where(df["Investor country"].isin(top5inv), other="Others")

#creating a matrix 
mat=df.pivot_table(index="company_country", columns="Investor country", values="round_amount_usd", aggfunc="sum", fill_value=0)
mat=addPercentage(mat)
mat=orderColumns(mat)
print(mat.to_string())
