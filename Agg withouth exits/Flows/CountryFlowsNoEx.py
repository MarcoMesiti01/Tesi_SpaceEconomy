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
df_round.rename(columns={"Firm country" : "Country"}, inplace=True)
df_round=mylib.toEU(df_round)
df_round=mylib.filterExits(df_round)
df_round.rename(columns={"Country" : "Firm country"}, inplace=True)
print(df_round[df_round["Firm country"]=="EU"])

#polishing locations to make 1 investor 1 country
#df_inv_loc=mylib.polish_loc(df_inv_loc)
df_inv_loc=df_inv_loc[["Investor ID", "Country"]]
df_inv_loc=mylib.toEU(df_inv_loc)
df_inv_loc.rename(columns={"Country" : "Investor country"}, inplace=True)

#merging the two dataset
df=pd.merge(left=df_round, right=df_inv_loc, how="left", on="Investor ID")"""

df=pd.read_parquet("DB_Out/DB_export.parquet")
#df=mylib.space(df)
df=df[["round_id","company_id", "company_country", "round_label","round_amount_usd","investor_id","investor_country"]]
df.columns=["Round ID", "Firm ID", "Firm country", "Round type", "AmountUSD","Investor ID","Investor country"]
df=mylib.filterExits(df)
df.rename(columns={"Firm country" : "Country"}, inplace=True)
df=mylib.toEU(df)
df.rename(columns={"Country" : "Firm country", "Investor country" : "Country"}, inplace=True)
df=mylib.toEU(df)
df.rename(columns={"Country" : "Investor country"}, inplace=True)
df["AmountUSD"]=df["AmountUSD"].apply(lambda x: x/1000000000 if not pd.isna(x) else x)

#selecting the top 5 investing countries
df_top5inv=df[["Investor country", "AmountUSD"]].groupby("Investor country").sum()
df_top5inv.reset_index(inplace=True)
df_top5inv.sort_values(by="AmountUSD", ascending=False, inplace=True)
top5inv=df_top5inv["Investor country"].head(5).to_list()

#selecting the top 5 target countries
df_top5targ=df[["Firm country", "AmountUSD"]].groupby("Firm country").sum()
df_top5targ.reset_index(inplace=True)
df_top5targ.sort_values(by="AmountUSD", ascending=False, inplace=True)
top5targ=df_top5targ["Firm country"].head(5).to_list()

#changing the countries that are not in the top 5 to "Other"
df["Firm country"]=df["Firm country"].where(df["Firm country"].isin(top5targ), other="Others")
df["Investor country"]=df["Investor country"].where(df["Investor country"].isin(top5inv), other="Others")

#creating a matrix 
mat=df.pivot_table(index="Firm country", columns="Investor country", values="AmountUSD", aggfunc="sum", fill_value=0)
mat=addPercentage(mat)
mat=orderColumns(mat)
print(mat.to_string())
