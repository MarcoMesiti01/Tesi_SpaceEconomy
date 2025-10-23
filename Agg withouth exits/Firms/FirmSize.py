import pandas as pd
import Library as mylib

def getMaxEmployee(x : str) -> int:
    if not x:
        return "0"
    else:
        list=x.split(",")
        max="0"
        for i in list:
            a=list.pop().strip()
            if a != "n/a":
                max=a
                break
            else:
                continue
        return max



df=pd.read_parquet("DB_Out/DB_firms.parquet", columns=["company_id","employee_number"])

df["employee_number"]=df["employee_number"].apply(getMaxEmployee)
df["employee_number"].astype("float64[pyarrow]")
df.sort_values(by="employee_number", ascending=False, inplace=True)
print(df["employee_number"])

