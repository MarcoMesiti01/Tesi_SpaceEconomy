import pandas as pd
import Library as mylib
import Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec as flag
from sklearn.preprocessing import RobustScaler
import json
from pathlib import Path

def _load_round_normalization(json_path: Path) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Build reverse map: raw_label_lower -> standardized_category
    reverse = {}
    for std_cat, raw_list in data.items():
        for raw in raw_list:
            if raw is None:
                continue
            key = str(raw).strip().lower()
            if key:
                reverse[key] = std_cat
    return reverse

def _standardize_round_labels(df: pd.DataFrame, normalizer: dict) -> pd.Series:
    # Accept either 'round_label' or 'Round type' as the source column
    source_col = None
    for c in ("round_label", "Round type"):
        if c in df.columns:
            source_col = c
            break
    if source_col is None:
        raise KeyError("Neither 'round_label' nor 'Round type' column found in rounds DB")

    def map_label(val):
        if pd.isna(val):
            return None
        key = str(val).strip().lower()
        return normalizer.get(key)

    return df[source_col].apply(map_label)

#Open the tables with the data
inv=mylib.openDB("investors")
rounds=mylib.openDB("rounds")

#Normalise the round label
norm=_load_round_normalization("Tesi_SpaceEconomy\Specialization_investigation\Descriptive\Round\RoundNormaliz.JSON")
rounds["round_label"]=_standardize_round_labels(rounds, norm)

#Add the information of investor country, investor Launch Year and space specialization
inv=flag.spacePercentage(inv, 2020, 0)
rounds=pd.merge(left=rounds, right=inv[["investor_id", "investor_country", "investor_launch_year", "space_percentage"]], how="left", on="investor_id")
rounds["investor_launch_year"]=pd.to_datetime(rounds["investor_launch_year"], errors="coerce")



#For each investor calculating the requested metrics
listDf=list()
for inv_id, inv_round in rounds.groupby(by="investor_id"):
    row=dict()
    row["investor_id"]=inv_id
    row["number_of_rounds"]=len(inv_round)
    row["average_round_size"]=inv_round["round_amount_usd"].fillna(0).mean()
    row["total_capital_employed"]=inv_round["round_amount_usd"].fillna(0).sum()
    row["seed"]=(inv_round[inv_round["round_label"]=="Seed"]["round_amount_usd"].fillna(0).sum()/row.get("total_capital_employed") if row.get("total_capital_employed")>0 else 0)
    row["early_stage"]=(inv_round[inv_round["round_label"]=="Early Stage"]["round_amount_usd"].fillna(0).sum()/row.get("total_capital_employed") if row.get("total_capital_employed")>0 else 0)
    row["early_growth"]=(inv_round[inv_round["round_label"]=="Early Growth"]["round_amount_usd"].fillna(0).sum()/row.get("total_capital_employed") if row.get("total_capital_employed")>0 else 0)
    row["later_stage"]=(inv_round[inv_round["round_label"]=="Later Stage"]["round_amount_usd"].fillna(0).sum()/row.get("total_capital_employed") if row.get("total_capital_employed")>0 else 0)
    row["geographical_focus"]=(inv_round[inv_round["investor_country"]==inv_round["company_country"]]["round_amount_usd"].fillna(0).sum()/row.get("total_capital_employed") if row.get("total_capital_employed")>0 else 0)
    row["average_time_investments"]=inv_round["round_date"].sort_values().diff().dt.days.mean()
    if inv_round["investor_launch_year"].notna().any():
        launch = inv_round["investor_launch_year"].dropna().iloc[0]
        row["active_years"] = int(pd.Timestamp.today().year - int(pd.to_datetime(launch).year))
    else:
        row["active_years"] = 0
    row["space_percentage"]=inv_round["space_percentage"].iloc[0]
    listDf.append(row)
dfFin=pd.DataFrame(listDf, columns=["investor_id","number_of_rounds","average_round_size","total_capital_employed","seed","early_stage","early_growth","later_stage","geographical_focus","average_time_investments","active_years","space_percentage"])
dfFin.set_index(keys="investor_id", inplace=True)
dfFin["average_time_investments"]=dfFin["average_time_investments"].fillna(0)
dfFin=dfFin[dfFin["number_of_rounds"]>=4]
dfFin=dfFin.astype("float64")
dfFin.to_parquet("Tesi_SpaceEconomy\Clustering\DimDataClusterNoNorm.parquet")
scaler=RobustScaler()
cols = ["number_of_rounds","average_round_size","total_capital_employed","seed","early_stage","early_growth","later_stage","geographical_focus","average_time_investments","active_years","space_percentage"]
dfFin.loc[:, cols] = scaler.fit_transform(dfFin.loc[:, cols].astype(float))
print(dfFin[["number_of_rounds", "active_years"]])
dfFin.to_parquet("Tesi_SpaceEconomy\Clustering\DimDataCluster.parquet")



