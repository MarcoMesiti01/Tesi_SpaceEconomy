import pandas as pd
import Library as mylib
import requests as r
import json

"""API_KEY="YOUR_API_KEY"
url="https://places.googleapis.com/v1/places:searchText"

df=pd.read_parquet("DB_Out/DB_investors.parquet")
df=df["Investor"]
df.drop_duplicates(inplace=True)
df=df[8000:8940]
listadd=list()
for i in range(len(df)):
    response=mylib.findLocation(str(df.iloc[i]))
    print(response)
    if  isinstance(response, list):
        for dict in response:
            listadd.append(dict)
    else:
        listadd.append(response)
    print(listadd)
df_final=pd.DataFrame(listadd).astype("string")
print(df_final.head())
df_final.to_parquet("DB_Out/Inv_locations17.parquet")"""
df=pd.read_parquet("DB_Out/DB_export.parquet")
df=df[["investor_id","investor_name","investor_continent","investor_country","investor_city","investor_metros"]]
df.columns=["Investor ID","Investor","Continent","Country","City","Metros"]
df.to_parquet("DB_inv_loc.parquet")



