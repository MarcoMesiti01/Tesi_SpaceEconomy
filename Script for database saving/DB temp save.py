import pandas as pd
import Library as mylib

df=pd.read_excel("polito_new_export 1.xlsx")
df["round_date"]=pd.to_datetime(df["round_date"], errors="coerce")
df["investor_launch_year"]=pd.to_datetime(df["investor_launch_year"], errors="coerce")
df["company_name"] = df["company_name"].astype("string[pyarrow]")
df["company_city"] = df["company_city"].astype("string[pyarrow]")
df["valuation_number"] = df["valuation_number"].astype("string[pyarrow]")
df["revenue_number"] = df["revenue_number"].astype("string[pyarrow]")
df["employee_number"] = df["employee_number"].astype("string[pyarrow]")
df["ebitda_number"] = df["ebitda_number"].astype("string[pyarrow]")
df["investor_name"] = df["investor_name"].astype("string[pyarrow]")
df.to_parquet("DB_Out/DB_export.parquet")