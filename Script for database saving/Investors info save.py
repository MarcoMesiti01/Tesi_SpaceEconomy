import pandas as pd
import Library as mylib

df=pd.read_parquet("DB_Out/DB_export.parquet")
#info_inv=mylib.investorInfo(df)
info_inv=df[["investor_id",	"investor_name", "investor_url", "investor_launch_year", "investor_types"]]
info_inv.columns=["ID", "Investor", "Url", "Launch year", "Investor type"]
info_inv.drop_duplicates(inplace=True, ignore_index=True)
print(info_inv)
info_inv.to_parquet("DB_Out/DB_investors.parquet")