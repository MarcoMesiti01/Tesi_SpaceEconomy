import pandas as pd
import numpy as np
import Library as mylib

df=pd.read_parquet("DB_temp.parquet", columns=["ID", "Name", "Historical valuations - dates", "Historical valuations - values (EUR M)"])

df_final=mylib.valuations(df)

df_final.to_parquet("DB_Out/DB_Valuations.parquet")