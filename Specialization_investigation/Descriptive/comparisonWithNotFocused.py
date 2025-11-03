import pandas as pd
from pathlib import Path
import Library as mylib
from Tesi_SpaceEconomy.Specialization_investigation.flagSpaceSpec import spacePercentage, spaceSpecialization

inv=mylib.openDB("investors")
round=mylib.openDB("rounds")
invPerc=spacePercentage(inv, 2015, 0.2)
inv=spaceSpecialization(inv, 2015, 0.2)

#filtering the dataframe for venture capitals
inv=inv[inv["investor_flag_venture_capital"]==1]
round=round[round["investor_id"].isin(inv["investor_id"])].copy()

#filtering space
round=mylib.space(round, "company_id",False)

#filtering time frame
round=round[(round["round_date"].dt.year>=2015) & (round["round_date"].dt.year<=2025)].copy()

#adding the percentage to rounds
round=pd.merge(left=round, right=invPerc[["investor_id","space_percentage"]], on="investor_id", how="left").fillna(0)



#dividing the dataset into smaller ones
labels=["0.2","0.4","0.6","0.8","1"]
edges = [0, 0.2, 0.4, 0.6, 0.8, 1.0000001]
round["class"]=pd.cut(round["space_percentage"], bins=edges, labels=labels, include_lowest=True)


dffin = pd.DataFrame(columns=[
    "Percentage",
    "Average round size (others)",
    "Average number of rounds (others)",
    "Average round size (space only)",
    "Average number of rounds (space only)",
])

for l in labels:
    subset = round[round["class"] == l]
    other = subset[subset["Space"] == 0]
    space_only = subset[subset["Space"] == 1]

    other_rounds = other["round_amount_usd"]
    other_investors = other["investor_id"].drop_duplicates()
    space_rounds = space_only["round_amount_usd"]
    space_investors = space_only["investor_id"].drop_duplicates()

    other_avg_size = other_rounds.mean() / 1_000_000 if not other_rounds.empty else 0
    other_avg_rounds = (
        other_rounds.count() / len(other_investors) if len(other_investors) > 0 else 0
    )
    space_avg_size = space_rounds.mean() / 1_000_000 if not space_rounds.empty else 0
    space_avg_rounds = (
        space_rounds.count() / len(space_investors) if len(space_investors) > 0 else 0
    )

    dffin.loc[len(dffin)] = {
        "Percentage": l,
        "Average round size (others)": other_avg_size,
        "Average number of rounds (others)": other_avg_rounds,
        "Average round size (space only)": space_avg_size,
        "Average number of rounds (space only)": space_avg_rounds,
    }

dffin = dffin.round(2)

output_path = Path(__file__).with_name("comparison_with_not_focused.xlsx")
dffin.to_excel(output_path, index=False)

print("Summary table by investor space exposure class:")
print(dffin)
print(f"\nExcel output saved to: {output_path}")








