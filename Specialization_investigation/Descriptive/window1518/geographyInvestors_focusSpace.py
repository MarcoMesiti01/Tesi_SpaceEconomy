import re
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import Library as mylib

# Optional dependency for city -> US state mapping (works offline)
try:
    import pgeocode  # uses local GeoNames data packaged in the library
    HAS_PGEO = True
except ImportError:
    HAS_PGEO = False
# Constants for the specialization window logic (aligned with window1518 analysis)
WINDOW_REFERENCE_YEAR = 2021  # column containing the 2016-2020 specialization ratio
SPECIALIZATION_THRESHOLD = 0.20

CURRENT_FILE = Path(__file__).resolve()


def find_project_root(script_path: Path) -> Path:
    """Locate repo root (folder that hosts DB_Out)."""
    for parent in script_path.parents:
        if (parent / "DB_Out").exists():
            return parent
    return script_path.parent


def load_specialization(project_root: Path) -> pd.DataFrame:
    """Load the reference specialization column (2021) from the fact table."""
    fact_path = project_root / "DB_Out" / "Fact" / "FactInvestorYearSpecialization.parquet"
    fact = pd.read_parquet(fact_path)
    if WINDOW_REFERENCE_YEAR not in fact.columns:
        raise KeyError(
            f"Column {WINDOW_REFERENCE_YEAR} not found in {fact_path.name}. "
            f"Available columns: {list(fact.columns)}"
        )
    spec = (
        fact[[WINDOW_REFERENCE_YEAR]]
        .rename(columns={WINDOW_REFERENCE_YEAR: "space_percentage"})
        .reset_index()
    )
    spec["space_percentage"] = (
        pd.to_numeric(spec["space_percentage"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    return spec


def build_investor_universe() -> pd.DataFrame:
    """Apply window1518 specialization filters and return investor geography info."""
    project_root = find_project_root(CURRENT_FILE)
    investors = mylib.openDB("investors")
    rounds = mylib.openDB("rounds")
    rounds_space = mylib.space(rounds.copy(), "company_id", False)

    specialization = load_specialization(project_root)
    investors = investors.merge(specialization, on="investor_id", how="left")
    investors["space_percentage"] = investors["space_percentage"].fillna(0.0)
    investors = investors[investors["space_percentage"] >= SPECIALIZATION_THRESHOLD].copy()

    if investors.empty:
        return investors

    # Original VC cohort
    original_vc_df = mylib.isOriginalVC(investors[["investor_id"]].drop_duplicates(), True)
    original_ids = set(original_vc_df["investor_id"].dropna().unique())
    investors = investors[investors["investor_id"].isin(original_ids)].copy()
    if investors.empty:
        return investors

    # >=4 deals overall
    deals_per_investor = (
        rounds.dropna(subset=["investor_id"]).groupby("investor_id").size().rename("deal_count")
    )
    four_plus_ids = set(deals_per_investor[deals_per_investor >= 4].index)
    investors = investors[investors["investor_id"].isin(four_plus_ids)].copy()
    if investors.empty:
        return investors

    # At least one European space deal (across full history)
    firms = pd.read_parquet(project_root / "DB_Out" / "DB_firms.parquet")
    if "company_continent" in firms.columns:
        continent_series = firms["company_continent"].astype(str).str.strip().str.casefold()
    else:
        continent_series = pd.Series("", index=firms.index)
    europe_company_ids = set(
        firms.loc[continent_series == "europe", "company_id"].dropna().unique()
    )
    europe_space_ids = set(
        rounds_space.loc[
            (rounds_space["space"] == 1) & (rounds_space["company_id"].isin(europe_company_ids)),
            "investor_id",
        ]
        .dropna()
        .unique()
    )
    investors = investors[investors["investor_id"].isin(europe_space_ids)].copy()
    return investors


# Load data from DB_Out using Library helpers
df_inv = build_investor_universe()
if df_inv.empty:
    raise ValueError("No investors satisfy the window1518 specialization filters.")
print(len(df_inv))
df_exp = mylib.openDB("export")

# Keep only space-related companies and the corresponding investor IDs
df_exp = mylib.space(df_exp, "company_id", True)
df_exp_ids = (
    df_exp[["investor_id"]]
    .dropna()
    .astype({"investor_id": int}, errors="ignore")
    .drop_duplicates()
)

# Filter investors to only those that appear in the (space-filtered) export
df_inv = df_inv[df_inv["investor_id"].isin(df_exp_ids["investor_id"])].copy()

# Normalize columns we need
df_inv.rename(columns={
    "investor_country": "Country",
    "investor_city": "City",
}, inplace=True)

# Build world-level country counts excluding the USA (will map USA at state level separately)
df_world = (
    df_inv[df_inv["Country"].notna()]
    .copy()
)

# Exclude the USA with exact match (data is standardized)
df_world = df_world[df_world["Country"] != "United States"]
df_world = df_world[["Country", "investor_id"]].groupby("Country").count().reset_index()
df_world["CountryISO3"] = df_world["Country"].apply(mylib.to_iso3)
df_world = df_world[df_world["CountryISO3"].notna()]

# World map (countries only, USA excluded)
fig_world = px.choropleth(
    df_world,
    locations="CountryISO3",
    color="investor_id",
    hover_name="Country",
    color_continuous_scale="Reds",
    projection="natural earth",
)
fig_world.update_layout(
    title="Investors per Country (USA detailed separately)",
    coloraxis_colorbar_title="Investors",
    margin=dict(l=0, r=0, t=40, b=0),
)
for _, row in df_world.iterrows():
    fig_world.add_trace(
        go.Scattergeo(
            locationmode="ISO-3",
            locations=[row["CountryISO3"]],
            text=[int(row["investor_id"])],
            mode="text",
            showlegend=False,
        )
    )
fig_world.show()

# ----- USA state-level map -----
def build_city_to_state_map(cities):
    """Resolve a set of US city names to USPS state codes using pgeocode.

    Returns (mapping: dict[str,str], ambiguous: set[str], missing: set[str])
    - mapping: only cities that resolve unambiguously to a single state
    - ambiguous: cities that map to more than one state in GeoNames
    - missing: cities not found in GeoNames dataset
    """
    mapping = {}
    ambiguous = set()
    missing = set()

    if not HAS_PGEO:
        # If pgeocode is not available, instruct to install it.
        raise ImportError(
            "pgeocode is required for city->state resolution. Install with: pip install pgeocode"
        )

    nomi = pgeocode.Nominatim("US")
    data = nomi._data  # pandas DataFrame with columns including place_name and state_code

    # Normalize once
    series_place = data["place_name"].astype(str)
    cf_place = series_place.str.casefold()

    for city in cities:
        if not isinstance(city, str) or not city.strip():
            continue
        key = city.strip()
        key_cf = key.casefold()
        mask = cf_place == key_cf
        matches = data.loc[mask]
        if matches.empty:
            # try a looser match (start of place name), e.g., "st louis" vs "saint louis"
            # but do not auto-resolve across multiple states
            loose = cf_place.str.startswith(key_cf)
            matches = data.loc[loose]

        if matches.empty:
            missing.add(key)
            continue

        states = set(matches["state_code"].dropna().astype(str).str.upper().tolist())
        if len(states) == 1:
            mapping[key] = next(iter(states))
        else:
            mapping[key] = next(iter(states))

    return mapping, ambiguous, missing


df_usa = df_inv[df_inv["Country"] == "United States"].copy()
df_usa["City"]=df_usa["City"].replace({"New York City":"New York", "Washington DC":"Washington"})
print(len(df_usa))

# Resolve US cities to state codes using pgeocode to avoid manual catalogs
unique_cities = (
    df_usa["City"].dropna().astype(str).str.strip().drop_duplicates().tolist()
)
try:
    city_state_map, ambiguous_cities, missing_cities = build_city_to_state_map(unique_cities)
except ImportError as e:
    # If pgeocode isn't installed, provide a clear message and stop US mapping gracefully
    print(str(e))
    city_state_map, ambiguous_cities, missing_cities = {}, set(), set()

if ambiguous_cities:
    print(f"Ambiguous cities (need manual resolution): {sorted(ambiguous_cities)[:20]}... total={len(ambiguous_cities)}")
if missing_cities:
    print(f"Cities not found in GeoNames: {sorted(missing_cities)[:20]}... total={len(missing_cities)}")

df_usa["StateCode"] = df_usa["City"].map(city_state_map)
df_usa = df_usa[df_usa["StateCode"].notna()]

df_usa_counts = df_usa[["StateCode", "investor_id"]].groupby("StateCode").count().reset_index()
df_usa_counts.rename(columns={"investor_id": "Investors"}, inplace=True)

fig_usa = go.Figure(
    data=go.Choropleth(
        locations=df_usa_counts["StateCode"],
        z=df_usa_counts["Investors"],
        locationmode="USA-states",
        colorscale="Reds",
        colorbar_title="Investors",
        text="Investors"
    )
)
fig_usa.update_layout(
    title_text="Investors per US State",
    geo_scope="usa",
    margin=dict(l=0, r=0, t=40, b=0),
)

# Overlay numbers on each state using existing counts
fig_usa.add_trace(
    go.Scattergeo(
        locationmode="USA-states",
        locations=df_usa_counts["StateCode"],
        text=df_usa_counts["Investors"].astype(int).astype(str),
        mode="text",
        textfont=dict(color="black", size=10),
        showlegend=False,
        hoverinfo="skip",
    )
)

# Show both figures
fig_usa.show()
