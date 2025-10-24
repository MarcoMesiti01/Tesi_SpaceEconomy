import pandas as pd
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spaceSpecialization

inv=mylib.openDB("investors")
round=mylib.openDB("rounds")
inv=spaceSpecialization(inv, 2020, 0.2)

#filtering the dataframe
inv=inv[inv["investor_flag_venture_capital"]==1]
roundAll=round[round["investor_id"].isin(inv["investor_id"])]
inv=inv[inv["investor_flag_space"]==1]
roundSpace=round[round["investor_id"].isin(inv["investor_id"])].copy()

#average round size
mean=[roundSpace["round_amount_usd"].mean()/1000000, roundAll["round_amount_usd"].mean()/1000000]

#avergae number of rounds 
numberOfRounds=list()
roundSpace=roundSpace[["investor_id","round_date"]]
roundSpace=roundSpace.groupby("investor_id").count()
roundAll=roundAll[["investor_id","round_date"]]
roundAll=roundAll.groupby("investor_id").count()
numberOfRounds=[roundSpace["round_date"].mean(), roundAll["round_date"].mean()]

#distribution 

#creating a dataframe to save the informations
dfout=pd.DataFrame(columns=["Investors","Average round size","Average number of rounds"])
dfout.loc[0]=["Space focused investors", mean[0], numberOfRounds[0]]
dfout.loc[1]=["Generic venture capital", mean[1], numberOfRounds[1]]

print(dfout)




