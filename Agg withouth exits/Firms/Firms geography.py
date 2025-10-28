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
            ambiguous.add(key)

    return mapping, ambiguous, missing


# Load exported DB and keep only Space-related entries
df_exp = mylib.openDB("export")
df_exp = mylib.space(df_exp, "company_id", True)

# Deduplicate firms so each firm is counted once, keep city too for US mapping
if "company_id" in df_exp.columns:
    df_firms = df_exp[["company_id", "company_country", "company_city"]].drop_duplicates(subset=["company_id"])  # one row per firm
else:
    df_firms = df_exp[["company_name", "company_country", "company_city"]].drop_duplicates(subset=["company_name"])  # fallback
df_firms["company_city"]=df_firms["company_city"].replace({
    "New York City":"New York",
    "Washington DC":"Washington",
    "St. Louis":"Saint Louis"
})

# Deterministic overrides for ambiguous US city names -> state code
CITY_STATE_OVERRIDE = {
    "Laconia": "NH","Lafayette": "LA","Livermore": "CA","Long Beach": "CA","Louisville": "KY",
    "Manassas": "VA","Marietta": "GA","McLean": "VA","Mesa": "AZ","Miami": "FL","Midland": "TX",
    "Monrovia": "CA","Mountain View": "CA","Newark": "NJ","North Bethesda": "MD","Orange": "CA",
    "Orlando": "FL","Pasadena": "CA","Portland": "OR","Raleigh": "NC","Redmond": "WA","Reno": "NV",
    "Rockville": "MD","Saint Louis": "MO","Saint Petersburg": "FL","San Diego": "CA","San Jose": "CA",
    "San Mateo": "CA","Santa Clara": "CA","Santa Cruz": "CA","Santa Fe": "NM","Somerville": "MA",
    "Stanford": "CA","Sullivan's Island": "SC","Sullivan\"s Island": "SC","Sunnyvale": "CA","Syracuse": "NY",
    "Titusville": "FL","Toledo": "OH","Torrance": "CA","Troy": "MI","Tysons": "VA","Wakefield": "MA",
    "Washington": "DC","Westfield": "NJ",
    "Alexandria": "VA","Arlington": "VA","Atlanta": "GA","Austin": "TX","Bedminster Township": "NJ",
    "Bellevue": "WA","Berkeley": "CA","Bishop": "CA","Boston": "MA","Boulder": "CO","Brownsville": "TX",
    "Buffalo": "NY","Cambridge": "MA","Carlsbad": "CA","Chatsworth": "CA","Cincinnati": "OH","Dallas": "TX",
    "Dearborn": "MI","Denver": "CO","Detroit": "MI","Durham": "NC","Fremont": "CA","Glendale": "CA",
    "Golden": "CO","Grandview": "MO","Hawthorne": "CA","Herndon": "VA","Houston": "TX","Huntsville": "AL",
    "Irvine": "CA","Ithaca": "NY","Jacksonville": "FL","Kansas City": "MO","Kirkland": "WA"
}


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

ambiguous_to_report = []
missing_to_report = []
unresolved_to_report = []
try:
    city_state_map, _amb, _miss = build_city_to_state_map(unique_cities)
    override_keys = set(CITY_STATE_OVERRIDE.keys())
    ambiguous_to_report = sorted(set(_amb) - override_keys)
    missing_to_report = sorted(set(_miss) - override_keys)
    unresolved_to_report = sorted(set(c for c in unique_cities if c and c not in city_state_map and c not in override_keys))
except ImportError as e:
    print(str(e))
    unresolved_to_report = sorted(unique_cities)
    city_state_map = {}

df_usa["StateCode"] = df_usa["company_city"].map(city_state_map)
# Apply deterministic overrides
df_usa["StateCode"] = df_usa["company_city"].map(CITY_STATE_OVERRIDE).fillna(df_usa["StateCode"])
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

# Consolidated reporting of remaining cities needing intervention
needing_intervention = sorted(set(ambiguous_to_report) | set(missing_to_report) | set(unresolved_to_report))
if needing_intervention:
    print("US cities needing manual intervention (ambiguous or unresolved):")
    print("  " + ", ".join(needing_intervention))
