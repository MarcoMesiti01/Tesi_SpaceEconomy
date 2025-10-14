import pandas as pd
import Library as mylib
import plotly.express as px
import plotly.graph_objects as go

# Load exported DB and keep only Space-related entries
df_exp = pd.read_parquet("DB_Out/DB_export.parquet")
df_exp = mylib.space(df_exp)

# Deduplicate firms so each firm is counted once
# Prefer unique firm company_identifier if available, otherwise fallback to company_name
if "company_id" in df_exp.columns:
    df_firms = df_exp[["company_id", "company_country"]].drop_duplicates(subset=["company_id"])  # one row per firm
else:
    df_firms = df_exp[["company_name", "company_country"]].drop_duplicates(subset=["company_name"])  # fallback

# Aggregate: count firms by company_country
df_counts = (
    df_firms.groupby("company_country").size().reset_index(name="Firms")
)

# Prepare for mapping: recompany_nam to match mylib.makeMap expectations
df_counts = df_counts.rename(columns={"company_country": "Firm country"})

# Build and show the map (choropleth with text labels)
mylib.makeMap(df_counts, "Firms")
