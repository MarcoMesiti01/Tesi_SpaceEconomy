import pandas as pd
import Library as mylib
import matplotlib.pyplot as plt

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 20,
    'axes.labelsize': 20,
    'xtick.labelsize': 20,
    'ytick.labelsize': 20,
    'legend.fontsize': 20,
})

#we want to define the number of startup founded in each year and represent it on a time series chart
#years - number of firms launched
df_firm=pd.read_parquet("DB_Out/DB_export.parquet")
df_firm=mylib.space(df_firm, "company_id", True)
df_firm=df_firm[df_firm["company_launch_year"]>pd.to_datetime("2000", format="%Y")]
df_firm=df_firm[["company_id","company_name","company_launch_year"]]
df_firm.drop_duplicates(inplace=True, ignore_index=True)
df_firm=df_firm[["company_name","company_launch_year"]]
df_firm=df_firm.groupby("company_launch_year").count()
df_firm.reset_index(inplace=True)
df_firm.sort_values(by="company_launch_year", inplace=True)

#plotting them into a time series chart
plt.plot(df_firm["company_launch_year"], df_firm["company_name"], marker='o')
plt.title("Number of space related firms founded per year")
plt.xlabel("Year")
plt.ylabel("#Firms")
plt.grid(True)
plt.show()
