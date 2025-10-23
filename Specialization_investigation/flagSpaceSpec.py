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
    round.fillna({"round_amount_usd":0}, inplace=True)
    upDown=mylib.openDB("updown")
    #amount invested in space by each investor
    roundSpace=pd.merge(left=round, right=upDown, left_on="company_id", right_index=True, how="inner")
    roundSpace=roundSpace[roundSpace["round_date"]<=pd.to_datetime("2016", format="%Y")]
    roundSpace=roundSpace[["investor_id", "round_amount_usd"]].copy()
    roundSpace=roundSpace.groupby(by="investor_id").sum()
    roundSpace.rename(columns={"round_amount_usd":"investor_amount_space"}, inplace=True)

    #recording the information in the investor table
    df_investor=pd.merge(left=df_investor, right=roundSpace, left_on="investor_id", right_index=True, how="left")
    df_investor.fillna({"investor_amount_space":0}, inplace=True)

    #investor_total_amount invested by each investor in total
    round=round[round["round_date"]>pd.to_datetime("2015", format="%Y")]
    invAmount=round[["investor_id", "round_amount_usd", "investors_round"]].groupby(by="investor_id").agg({"round_amount_usd":"sum", "investors_round":"count"})
    invAmount.rename(columns={"round_amount_usd":"investor_total_amount", "investors_round":"investor_number_rounds"}, inplace=True)
    invAmount.fillna(0, inplace=True)

    #recording the information in the investor table
    df_investor=pd.merge(left=df_investor, right=invAmount, how="left", right_index=True, left_on="investor_id")
    print(df_investor)
    df_investor.fillna({"investor_total_amount" : 0, "Number of rounds" : 0}, inplace=True)
    df_investor["investor_flag_space"]=df_investor.apply(lambda x: 1 if x["investor_total_amount"]>0 and x["investor_amount_space"]/x["investor_total_amount"]>=0.2 else 0, axis=1)
    df_investor["investor_flag_venture_capital"]=df_investor["investor_types"].apply(lambda x: 1 if "Venture capital" in x or "venture_capital" in x else 0)
    #df_investor=df_investor[(df_investor["investor_flag_space"]==1) & (df_investor["investor_flag_venture_capital"]==1) & (df_investor["Number of rounds"]>1)]
    return df_investor







