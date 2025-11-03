import pandas as pd
import Library as mylib
import Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec as flag


inv=mylib.openDB("investors")
rounds=mylib.openDB("rounds")
firm=pd.read_parquet("DB_Out/DB_firms.parquet")
print(len(inv))

#filtro gli investitori che hanno un deal in aziende space europee
rounds=mylib.space(rounds, "company_id", False)
firmEu=firm[firm["company_continent"]=="Europe"]["company_id"] 
roundsLen=rounds[(rounds["Space"]==1) & (rounds["company_id"].isin(firmEu))]["investor_id"].drop_duplicates()
ids=set(roundsLen)
inv=inv[inv["investor_id"].isin(roundsLen)]

#troviamo la distribuzione degli investitori definiti sinora (la funzione filtra per venture capital e almeno 4 deals)
inv=flag.spacePercentage(inv, 2015, 0)
inv=inv[["investor_id","space_percentage"]]
print(len(inv))
print(inv["space_percentage"].quantile([0.5, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99]))