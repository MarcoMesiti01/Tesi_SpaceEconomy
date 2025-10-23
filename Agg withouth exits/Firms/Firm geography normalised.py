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


# ---------------- Data loading ----------------
# Load exported DB and keep only Space-related entries
df_exp = mylib.openDB("export")
df_exp = mylib.space(df_exp, "company_id", True)

# Deduplicate firms so each firm is counted once, keep city too for US mapping
if "company_id" in df_exp.columns:
    df_firms = df_exp[["company_id", "company_country", "company_city"]].drop_duplicates(subset=["company_id"])  # one row per firm
else:
    df_firms = df_exp[["company_name", "company_country", "company_city"]].drop_duplicates(subset=["company_name"])  # fallback
df_firms["company_city"]=df_firms["company_city"].replace({"New York City":"New York", "Washington DC":"Washington","St. Louis":"Saint Louis"})

# ---------------- Population baselines ----------------
# US state populations (approx. 2020 Census)
US_STATE_POP = {
    "AL": 5024279, "AK": 733391, "AZ": 7151502, "AR": 3011524, "CA": 39538223,
    "CO": 5773714, "CT": 3605944, "DE": 989948, "FL": 21538187, "GA": 10711908,
    "HI": 1455271, "ID": 1839106, "IL": 12812508, "IN": 6785528, "IA": 3190369,
    "KS": 2937880, "KY": 4505836, "LA": 4657757, "ME": 1362359, "MD": 6177224,
    "MA": 7029917, "MI": 10077331, "MN": 5706494, "MS": 2961279, "MO": 6154913,
    "MT": 1084225, "NE": 1961504, "NV": 3104614, "NH": 1377529, "NJ": 9288994,
    "NM": 2117522, "NY": 20201249, "NC": 10439388, "ND": 779094, "OH": 11799448,
    "OK": 3959353, "OR": 4237256, "PA": 13002700, "RI": 1097379, "SC": 5118425,
    "SD": 886667, "TN": 6910840, "TX": 29145505, "UT": 3271616, "VT": 643077,
    "VA": 8631393, "WA": 7705281, "WV": 1793716, "WI": 5893718, "WY": 576851,
    "DC": 689545,
}

# Europe countries ISO3 and populations (approx. 2020)
EUROPE_POP_ISO3 = {
    "ALB": 2877797, "AND": 77265, "AUT": 8917205, "BEL": 11555997, "BGR": 6927288,
    "BIH": 3280815, "BLR": 9398861, "CHE": 8654618, "CYP": 1207359, "CZE": 10693939,
    "DEU": 83166711, "DNK": 5831404, "ESP": 47351567, "EST": 1331057, "FIN": 5540718,
    "FRA": 67391582, "GBR": 67215293, "GRC": 10715549, "HRV": 4047200, "HUN": 9749763,
    "IRL": 4994724, "ISL": 368792, "ITA": 59554023, "LTU": 2790845, "LUX": 634814,
    "LVA": 1901548, "MCO": 39242, "MDA": 2617820, "MKD": 2083459, "MLT": 514564,
    "MNE": 628066, "NLD": 17441139, "NOR": 5421241, "POL": 37950802, "PRT": 10196709,
    "ROU": 19286123, "RUS": 146171015, "SMR": 33938, "SRB": 6908224, "SVK": 5458827,
    "SVN": 2100126, "SWE": 10353442, "TUR": 84339067, "UKR": 44134693, "VAT": 825,
    "XKX": 1831000, "LIE": 38137, "KOS": 1831000,
}

# Define the set of ISO3 considered as Europe for this map
EUROPE_ISO3 = set(EUROPE_POP_ISO3.keys())


# ---------------- USA state-level normalised map (firms per 1M people) ----------------
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
df_usa_counts["Population"] = df_usa_counts["StateCode"].map(US_STATE_POP)
df_usa_counts = df_usa_counts[df_usa_counts["Population"].notna() & (df_usa_counts["Population"] > 0)]
df_usa_counts["FirmsPerM"] = df_usa_counts["Firms"] / df_usa_counts["Population"] * 1_000_000

fig_usa = go.Figure(
    data=go.Choropleth(
        locations=df_usa_counts["StateCode"],
        z=df_usa_counts["FirmsPerM"],
        locationmode="USA-states",
        colorscale="Blues",
        colorbar_title="Firms per 1M",
    )
)
fig_usa.update_layout(
    title_text="Space Firms per US State (normalised per 1M people)",
    geo_scope="usa",
    margin=dict(l=0, r=0, t=40, b=0),
)
fig_usa.add_trace(
    go.Scattergeo(
        locationmode="USA-states",
        locations=df_usa_counts["StateCode"],
        text=df_usa_counts["FirmsPerM"].round(2).astype(str),
        mode="text",
        textfont=dict(color="black", size=9),
        showlegend=False,
    )
)
fig_usa.show()


# ---------------- Europe normalised map (firms per 1M people) ----------------
df_eu = df_firms[df_firms["company_country"].notna()].copy()
df_eu["ISO3"] = df_eu["company_country"].apply(mylib.to_iso3)
df_eu = df_eu[df_eu["ISO3"].isin(EUROPE_ISO3)]
df_eu_counts = df_eu.groupby("ISO3").size().reset_index(name="Firms")
df_eu_counts["Population"] = df_eu_counts["ISO3"].map(EUROPE_POP_ISO3)
df_eu_counts = df_eu_counts[df_eu_counts["Population"].notna() & (df_eu_counts["Population"] > 0)]
df_eu_counts["FirmsPerM"] = df_eu_counts["Firms"] / df_eu_counts["Population"] * 1_000_000

fig_europe = go.Figure(
    data=go.Choropleth(
        locations=df_eu_counts["ISO3"],
        z=df_eu_counts["FirmsPerM"],
        locationmode="ISO-3",
        colorscale="Blues",
        colorbar_title="Firms per 1M",
    )
)
fig_europe.update_layout(
    title_text="Space Firms per Country — Europe (normalised per 1M people)",
    geo_scope="europe",
    margin=dict(l=0, r=0, t=40, b=0),
)
fig_europe.add_trace(
    go.Scattergeo(
        locationmode="ISO-3",
        locations=df_eu_counts["ISO3"],
        text=df_eu_counts["FirmsPerM"].round(2).astype(str),
        mode="text",
        textfont=dict(color="black", size=9),
        showlegend=False,
    )
)
fig_europe.show()
