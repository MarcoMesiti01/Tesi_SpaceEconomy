import pandas as pd
import Library as mylib
from pathlib import Path
from typing import Optional, Literal

investor = mylib.openDB("investors")

def spaceSpecialization(df_investor: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a flag to the investor dataset
    """
    round = mylib.openDB("rounds")
    round.fillna({"AmountUSD":0}, inplace=True)
    upDown=pd.read_parquet("DB_Out/DB_updown.parquet")
    #amount invested in space by each investor
    roundSpace=pd.merge(left=round, right=upDown, left_on="Target firm ID", right_index=True, how="inner")
    roundSpace=roundSpace[roundSpace["Round date"]<=pd.to_datetime("2016", format="%Y")]
    roundSpace=roundSpace[["Investor ID", "AmountUSD"]].copy()
    roundSpace=roundSpace.groupby(by="Investor ID").sum()
    roundSpace.rename(columns={"AmountUSD":"Amount Space"}, inplace=True)

    #recording the information in the investor table
    df_investor=pd.merge(left=df_investor, right=roundSpace, left_on="ID", right_index=True, how="left")
    df_investor.fillna({"Amount Space":0}, inplace=True)

    #total amount invested by each investor in total
    round=round[round["Round date"]<=pd.to_datetime("2016", format="%Y")]
    invAmount=round[["Investor ID", "AmountUSD", "Round investor number"]].groupby(by="Investor ID").agg({"AmountUSD":"sum", "Round investor number":"count"})
    invAmount.rename(columns={"AmountUSD":"Total amount", "Round investor number":"Number of rounds"}, inplace=True)
    invAmount.fillna(0, inplace=True)

    #recording the information in the investor table
    df_investor=pd.merge(left=df_investor, right=invAmount, how="left", right_index=True, left_on="ID")
    print(df_investor)
    df_investor.fillna({"Total amount" : 0, "Number of rounds" : 0}, inplace=True)
    df_investor["Flag space"]=df_investor.apply(lambda x: 1 if x["Total amount"]>0 and x["Amount Space"]/x["Total amount"]>=0.2 else 0, axis=1)
    df_investor["Venture capital flag"]=df_investor["Investor type"].apply(lambda x: 1 if "Venture capital" in x else 0)
    #df_investor=df_investor[(df_investor["Flag space"]==1) & (df_investor["Venture capital flag"]==1) & (df_investor["Number of rounds"]>1)]
    return df_investor







