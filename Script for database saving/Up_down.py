import pandas as pd
import numpy as np
import Library as mylib

df_round=pd.read_parquet("DB_Out/DB_export.parquet")
df_round=df_round[["company_id","company_name","company_all_tags"]]
df_round.columns=["Target firm ID","Target firm","Tags"]
df_round["Tags"]=df_round["Tags"].apply(mylib.splitString, by_row="compat")
df_round["Upstream"]=df_round["Tags"].apply(lambda x: "Space Upstream" in x)
df_round["Downstream"]=df_round["Tags"].apply(lambda x: "Space Downstream" in x)
df_round.to_parquet("DB_Out/DB_export_updown.parquet")