
import pandas as pd
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spacePercentage
#seleziono i dati
round=mylib.openDB("rounds")
inv=mylib.openDB("investors")

#seleziono top 8 round types
roundType8=round[["round_amount_usd","round_label"]].groupby(by="round_label").sum().sort_values(by=["round_amount_usd"], inplace=False, ascending=False).reset_index()
roundType8=roundType8["round_label"].head(8)

#filtro i rounds
round=round[round["round_label"].isin(roundType8)].copy()
#qua va messo filtro space nel caso
inv=spacePercentage(inv, 2020, 0.2)

#divido i round in insiemi in base alla percentuale
edges=[0, 0.2, 0.4, 0.6, 0.8, 1.0001]
labels=["0.2","0.4","0.6","0.8","1"]
round=pd.merge(left=round, right=inv, on="investor_id", how="left")
round["class"]=pd.cut(round["space_percentage"], bins=edges, include_lowest=True, right=True, labels=labels)

#preparo colonne e creo df
columns=roundType8.tolist()
columns.append("total amount")
df=pd.DataFrame(columns=columns)


#pivoting
round=round[["space_percentage","round_label","round_amount_usd"]]
pivot=round.pivot_table(values="round_amount_usd", index="class",columns="round_label",aggfunc="sum")

#diving for the total
total=list()
for l in labels:
    total.append(round[round["class"]==l]["round_amount_usd"].sum())




"""for l in labels:
    tot=round[round["class"]==l]["round_amount_usd"].sum(skipna=True)
    print(tot)
    df.loc[l, "total amount"]=tot
    for c in columns: 
        df.loc[l, c]=round[(round["class"]==l) & (round["round_label"]==c)]["round_amount_usd"].sum(skipna=True)/tot"""

for index, row in df.iterrows():
    print(index, row) 
