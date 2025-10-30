import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import Library as mylib
import matplotlib.ticker as mticker

# Increase default font sizes for readability
plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 18,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
})

def getLastEmployee(x:str)->str:
    if not x or pd.isna(x):
        return "0"
    else:
        list=x.split(",")
        for i in reversed(list):
            if i!="n/a":
                return i
        return "0"

def normalize(x : float ) -> float:
    for a in range(40000):
        top=(a+1)*10
        bottom=a*10
        if x>bottom and x<=top:
            return top
    return 0



# Load company id and raw employee history column
df = pd.read_parquet("DB_Out/DB_firms.parquet", columns=["company_id", "employee_number"])

# Extract latest bucket and convert to numeric employee counts
df["employee_number"]=df["employee_number"].apply(getLastEmployee)
df["employee_number"]=pd.to_numeric(df["employee_number"], errors="coerce")
#df["employee_norm"]=df["employee_number"].apply(normalize)

# Keep a Space-only view (Space==1)
df_space = mylib.space(df, "company_id", True)
df_print=df[(df["employee_number"]>0) & (df["employee_number"]<df["employee_number"].quantile(0.8))]
print(df_print["employee_number"].mean())
df_print=df_space[(df_space["employee_number"]>0) & (df_space["employee_number"]<df_space["employee_number"].quantile(0.8))]
print(df_print["employee_number"].mean())
print(df["employee_number"].quantile([0.25, 0.5, 0.75, 0.9, 0.99, 0.999]))
print(df_space["employee_number"].quantile([0.25, 0.5, 0.75, 0.9, 0.99, 0.999]))
#modify here and make the graph for this distribution


# Build comparable 50-employee bins for both groups without altering prior logic
emp_all = pd.to_numeric(df["employee_number"], errors="coerce").fillna(0)
emp_space = pd.to_numeric(df_space.get("employee_number", pd.Series(dtype=float)), errors="coerce").fillna(0)
sizeBin=20

# Use a robust upper bound (99.5th percentile) to avoid extreme outliers exploding bin count
combined = pd.concat([emp_all, emp_space], ignore_index=True) if (not emp_all.empty or not emp_space.empty) else pd.Series([0.0])
try:
    max_emp = float(np.nanpercentile(combined.to_numpy(dtype=float), 99.5))
except Exception:
    max_emp = float(combined.max()) if not combined.empty else sizeBin
if not np.isfinite(max_emp) or max_emp <= 0:
    max_emp = sizeBin
max_emp=1000
bins = np.arange(0.0, np.ceil(max_emp / sizeBin) * sizeBin + sizeBin, sizeBin)

out_dir = Path("Graphs"); out_dir.mkdir(parents=True, exist_ok=True)

# Plot distribution for all companies
if not emp_all.empty:
    plt.figure(figsize=(9, 5.5))
    plt.hist(emp_all.values, bins=bins, color="#1f77b4", edgecolor="white", density=True)
    plt.xlabel("Number of employees")
    plt.ylabel("Firms (%)")
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    plt.title("Firm size distribution – All companies")
    plt.grid(True, axis="y", alpha=0.2)
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        pass

# Plot distribution for space companies
if not emp_space.empty:
    plt.figure(figsize=(9, 5.5))
    plt.hist(emp_space.values, bins=bins, color="#d62728", edgecolor="white", density=True)
    plt.xlabel("Number of employees")
    plt.ylabel("Firms (%)")
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    plt.title("Firm size distribution – Space companies")
    plt.grid(True, axis="y", alpha=0.2)
    plt.tight_layout()
    try:
        plt.show()
    except Exception:
        pass
