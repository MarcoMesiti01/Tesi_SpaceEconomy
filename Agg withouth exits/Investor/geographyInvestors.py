import re
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

# Load data from DB_Out using Library helpers
df_inv = mylib.openDB("investors")  # columns include: ID, Investor country, Investor city
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
df_inv = df_inv[df_inv["ID"].isin(df_exp_ids["investor_id"])].copy()

# Normalize columns we need
df_inv.rename(columns={
    "Investor country": "Country",
    "Investor city": "City",
}, inplace=True)

# Build world-level country counts excluding the USA (will map USA at state level separately)
df_world = (
    df_inv[df_inv["Country"].notna()]
    .copy()
)

# Exclude the USA with exact match (data is standardized)
df_world = df_world[df_world["Country"] != "United States"]
df_world = df_world[["Country", "ID"]].groupby("Country").count().reset_index()
df_world["CountryISO3"] = df_world["Country"].apply(mylib.to_iso3)
df_world = df_world[df_world["CountryISO3"].notna()]

# World map (countries only, USA excluded)
fig_world = px.choropleth(
    df_world,
    locations="CountryISO3",
    color="ID",
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
            text=[int(row["ID"])],
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

df_usa_counts = df_usa[["StateCode", "ID"]].groupby("StateCode").count().reset_index()
df_usa_counts.rename(columns={"ID": "Investors"}, inplace=True)

fig_usa = go.Figure(
    data=go.Choropleth(
        locations=df_usa_counts["StateCode"],
        z=df_usa_counts["Investors"],
        locationmode="USA-states",
        colorscale="Reds",
        colorbar_title="Investors",
    )
)
fig_usa.update_layout(
    title_text="Investors per US State",
    geo_scope="usa",
    margin=dict(l=0, r=0, t=40, b=0),
)

# Show both figures
fig_usa.show()
