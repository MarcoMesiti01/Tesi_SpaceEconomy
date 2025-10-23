import pandas as pd
import Library as mylib
import plotly.express as px
import plotly.graph_objects as go

# Optional dependency for city -> US state mapping (works offline)
try:
    import pgeocode
    HAS_PGEO = True
except ImportError:
    HAS_PGEO = False


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
        raise ImportError(
            "pgeocode is required for city->state resolution. Install with: pip install pgeocode"
        )

    nomi = pgeocode.Nominatim("US")
    data = nomi._data  # pandas DataFrame with columns including place_name and state_code
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
            loose = cf_place.str.startswith(key_cf)
            matches = data.loc[loose]

        if matches.empty:
            missing.add(key)
            continue

        states = set(matches["state_code"].dropna().astype(str).str.upper().tolist())
        if len(states) == 1:
            mapping[key] = next(iter(states))
        else:
            mapping[key]=next(iter(states))

    return mapping, ambiguous, missing


# Load exported DB and keep only Space-related entries
df_exp = mylib.openDB("export")
df_exp = mylib.space(df_exp, "company_id", True)

# Deduplicate firms so each firm is counted once, keep city too for US mapping
if "company_id" in df_exp.columns:
    df_firms = df_exp[["company_id", "company_country", "company_city"]].drop_duplicates(subset=["company_id"])  # one row per firm
else:
    df_firms = df_exp[["company_name", "company_country", "company_city"]].drop_duplicates(subset=["company_name"])  # fallback
df_firms["company_city"]=df_firms["company_city"].replace({"New York City":"New York", "Washington DC":"Washington","St. Louis":"Saint Louis"})


# ---------------- World map excluding USA ----------------
df_world = df_firms[df_firms["company_country"].notna()].copy()
df_world = df_world[df_world["company_country"] != "United States"]
df_world_counts = df_world.groupby("company_country").size().reset_index(name="Firms")
df_world_counts.rename(columns={"company_country": "company_country"}, inplace=True)

# Convert to ISO3 using Library helper and drop NAs
df_world_counts["CountryISO3"] = df_world_counts["company_country"].apply(mylib.to_iso3)
df_world_counts = df_world_counts[df_world_counts["CountryISO3"].notna()]

fig_world = px.choropleth(
    df_world_counts,
    locations="CountryISO3",
    color="Firms",
    hover_name="company_country",
    color_continuous_scale="Reds",
    projection="natural earth",
)
fig_world.update_layout(
    title="Space Firms per Country (USA detailed separately)",
    coloraxis_colorbar_title="Firms",
    margin=dict(l=0, r=0, t=40, b=0),
)
for _, row in df_world_counts.iterrows():
    fig_world.add_trace(
        go.Scattergeo(
            locationmode="ISO-3",
            locations=[row["CountryISO3"]],
            text=[int(row["Firms"])],
            mode="text",
            showlegend=False,
        )
    )
fig_world.show()


# ---------------- USA state-level map using company_city ----------------
df_usa = df_firms[df_firms["company_country"] == "United States"].copy()
unique_cities = df_usa["company_city"].dropna().astype(str).str.strip().drop_duplicates().tolist()

try:
    city_state_map, ambiguous_cities, missing_cities = build_city_to_state_map(unique_cities)
except ImportError as e:
    print(str(e))
    city_state_map, ambiguous_cities, missing_cities = {}, set(), set()

if ambiguous_cities:
    print(f"Ambiguous US cities (manual resolution advised): {sorted(ambiguous_cities)[:20]}... total={len(ambiguous_cities)}")
if missing_cities:
    print(f"US cities not found in GeoNames: {sorted(missing_cities)[:20]}... total={len(missing_cities)}")

df_usa["StateCode"] = df_usa["company_city"].map(city_state_map)
df_usa = df_usa[df_usa["StateCode"].notna()]

df_usa_counts = df_usa.groupby("StateCode").size().reset_index(name="Firms")

fig_usa = go.Figure(
    data=go.Choropleth(
        locations=df_usa_counts["StateCode"],
        z=df_usa_counts["Firms"],
        locationmode="USA-states",
        colorscale="Reds",
        colorbar_title="Firms",
    )
)
fig_usa.update_layout(
    title_text="Space Firms per US State",
    geo_scope="usa",
    margin=dict(l=0, r=0, t=40, b=0),
)
fig_usa.add_trace(
    go.Scattergeo(
        locationmode="USA-states",
        locations=df_usa_counts["StateCode"],
        text=df_usa_counts["Firms"].astype(int).astype(str),
        mode="text",
        textfont=dict(color="black", size=9),
        showlegend=False,
    )
)
fig_usa.show()
