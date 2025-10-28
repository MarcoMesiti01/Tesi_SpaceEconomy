import pandas as pd
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spacePercentage, spaceSpecialization

inv=mylib.openDB("investors")
round=mylib.openDB("rounds")
invPerc=spacePercentage(inv, 2020, 0.2)
inv=spaceSpecialization(inv, 2020, 0.2)

#filtering the dataframe for venture capitals
inv=inv[inv["investor_flag_venture_capital"]==1]
round=round[round["investor_id"].isin(inv["investor_id"])].copy()

#filtering space
round=mylib.space(round, "company_id",False)

#filtering time frame
round=round[(round["round_date"].dt.year>=2020) & (round["round_date"].dt.year<=2025)].copy()

#adding the percentage to rounds
round=pd.merge(left=round, right=invPerc[["investor_id","space_percentage"]], on="investor_id", how="left").fillna(0)



#dividing the dataset into smaller ones
labels=["0.2","0.4","0.6","0.8","1"]
edges = [0, 0.2, 0.4, 0.6, 0.8, 1.0000001]
round["class"]=pd.cut(round["space_percentage"], bins=edges, labels=labels, include_lowest=True)


#finding average round size
dffin=pd.DataFrame(columns=["Percentage","Average round size (others)","Average number of rounds (others)", "Average round size (space only)" , "Average number of rounds (space only)"])
i=0
for l in labels: 
    i+=1
    dffin.loc[i, "Percentage"]= l
    dffin.loc[i, "Average round size (others)"]=round[(round["class"]==l) & (round["Space"]==0)]["round_amount_usd"].mean()/1000000
    dffin.loc[i, "Average number of rounds (others)"]=len(round[(round["class"]==l) & (round["Space"]==0)]["round_amount_usd"])/len(round[(round["class"]==l) & (round["Space"]==0)]["investor_id"].drop_duplicates())
    dffin.loc[i, "Average round size (space only)"]=round[(round["class"]==l) & (round["Space"]==1)]["round_amount_usd"].mean()/1000000
    dffin.loc[i, "Average number of rounds (space only)"]=round[(round["class"]==l) & (round["Space"]==1)]["round_amount_usd"].fillna(0).count()/round[(round["class"]==l) & (round["Space"]==1)]["investor_id"].fillna(0).drop_duplicates().count()

for index, row in dffin.iterrows():
    print(index, row)








