import pandas as pd
import matplotlib.pyplot as plt
import Library as mylib

def continueSpec(x) -> int:
    if x.empty:
        return 0
    specialised=False
    for flag in x.tolist():
        if flag==1:
            if not specialised:
                specialised=True
            else:
                continue
        else:
            if specialised:
                return 0
            else:
                continue
    if specialised:
        return 1
    else:
        return 0

df=pd.read_parquet("DB_Out/Fact/FactInvestorYearSpecialization.parquet")

df_not=df.copy()
print(df_not[2025].sum())
df_not["flag"]=df.apply(continueSpec, axis=1)
df_not_1=df_not[df_not["flag"]==1]["flag"].copy()
print(df_not_1.sum())
df_graph=df.sum()


#plotting the data on a time-series chart
plt.figure(figsize=(9, 4.5))
plt.plot(df_graph.index.to_list(), df_graph.to_list(), marker="o")
plt.xlabel("Year")
plt.ylabel("Number of specialised VC")
plt.grid(True, linewidth=0.5, alpha=0.6)
plt.tight_layout()
plt.show()