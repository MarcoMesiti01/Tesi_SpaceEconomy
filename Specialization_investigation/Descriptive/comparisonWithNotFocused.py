import pandas as pd
import Library as mylib

inv=mylib.openDB("investors")
round=mylib.openDB("rounds")

#filtering the dataframe
inv=inv[inv["investor_flag_venture_capital"]==1]
roundAll=round[round["investor_id"].isin(inv["ID"])]
inv=inv[inv["investor_flag_space"]==1]
roundSpace=round[round["investor_id"].isin(inv["ID"])].copy()

#average round size
mean=[roundSpace["round_amount_usd"].mean()/1000000, roundAll["round_amount_usd"].mean()/1000000]

#avergae number of rounds 
numberOfRounds=list()
roundSpace=roundSpace[["investor_id","Round date"]]
roundSpace=roundSpace.groupby("investor_id").count()
roundAll=roundAll[["investor_id","Round date"]]
roundAll=roundAll.groupby("investor_id").count()
numberOfRounds=[roundSpace["Round date"].mean(), roundAll["Round date"].mean()]

#creating a dataframe to save the informations
dfout=pd.DataFrame(columns=["Investors","Average round size","Average number of rounds"])
dfout.loc[0]=["Space focused investors", mean[0], numberOfRounds[0]]
dfout.loc[1]=["Generic venture capital", mean[1], numberOfRounds[1]]

print(dfout)




