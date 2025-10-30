import pandas as pd
import Library as mylib
import plotly.graph_objects as go

# Optional dependency for US city -> state mapping
try:
    import pgeocode
    _HAS_PGEO = True
except ImportError:
    _HAS_PGEO = False


def _build_city_to_state_map(cities):
    if not _HAS_PGEO:
        raise ImportError("pgeocode is required for city->state resolution. Install with: pip install pgeocode")
    nomi = pgeocode.Nominatim("US")
    data = nomi._data
    series_place = data["place_name"].astype(str)
    cf_place = series_place.str.casefold()
    mapping = {}
    ambiguous, missing = set(), set()
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
        if len(states):
            mapping[key] = next(iter(states))
        else:
            ambiguous.add(key)
    return mapping, ambiguous, missing


# Minimal population references (approx. 2022/2023). Values are total people.
# Country populations are keyed by ISO3 codes.
COUNTRY_POP = {
    "USA": 333287557, "GBR": 67508936, "DEU": 83294633, "FRA": 68042591,
    "ITA": 58870762, "ESP": 47450795, "CAN": 38929902, "CHN": 1411750000,
    "IND": 1380004385, "JPN": 125171000, "AUS": 26177413, "NLD": 17650200,
    "SWE": 10549347, "NOR": 5455260, "FIN": 5536146, "DNK": 5910912,
    "CHE": 8740443, "AUT": 9006400, "BEL": 11655930, "IRL": 5070000,
    "ISR": 9732000, "BRA": 215313498, "KOR": 51780579, "SGP": 5703600,
    "ARE": 9276129, "RUS": 144444359, "LUX": 654768, "NZL": 5135300,
    "PRT": 10310211, "GRC": 10341277, "POL": 37797200, "CZE": 10736784,
    "HUN": 9596000, "ROU": 19053800, "BGR": 6843000, "HRV": 3871833,
    "SVN": 2119777, "SVK": 5459642, "LTU": 2860000, "LVA": 1890000,
    "EST": 1331000, "TUR": 85341241, "MEX": 126705138, "ISL": 387800,
    "CYP": 1244184, "MLT": 535000, "UKR": 41130432, "ZAF": 60414495,
    "EGY": 109262178, "CHL": 19603733, "COL": 51520000, "ARG": 45773884,
}

# US state populations (2020 Census approx.) keyed by 2-letter codes
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


def _amount_per_million(amount_musd: float, population: float) -> float:
    if pd.isna(amount_musd) or population is None or population <= 0:
        return None
    # amount is already in million USD. Divide by (pop / 1e6) -> MUSD per million people
    return float(amount_musd) / (population / 1_000_000)


# Load rounds and export; filter only space firms
df = mylib.openDB("rounds")
db_exp = mylib.openDB("export")[
    ["company_id", "company_all_tags", "company_city", "company_country"]
]
db_exp = mylib.space(db_exp, "company_id", True)
db_ids = db_exp["company_id"]
df = df[df["company_id"].isin(db_ids)]

# Keep needed fields and scale amount to millions
df = df[["company_id", "company_country", "round_amount_usd"]].copy()
df["round_amount_usd"] = df["round_amount_usd"].apply(lambda x: x/1000000 if not pd.isna(x) else x)

# ---------------- World map normalized by population (exclude USA) ----------------
df_world = df[df["company_country"].notna()].copy()
df_world = df_world[df_world["company_country"] != "United States"]
df_world_agg = (
    df_world.groupby("company_country")["round_amount_usd"].agg(["sum", "mean", "count", "std"]).reset_index()
)
df_world_agg.rename(
    columns={
        "sum": "Total amount invested",
        "mean": "Average round size",
        "count": "Number of round",
        "std": "Variance measure (std)",
    },
    inplace=True,
)

# ISO3 codes and population join
df_world_agg["iso3"] = df_world_agg["company_country"].apply(mylib.to_iso3)
df_world_agg["population"] = df_world_agg["iso3"].map(COUNTRY_POP)
df_world_agg = df_world_agg[df_world_agg["iso3"].notna()].copy()
df_world_agg = df_world_agg[df_world_agg["population"].notna()].copy()
df_world_agg["Amount per 1M people (MUSD)"] = df_world_agg.apply(
    lambda r: _amount_per_million(r["Total amount invested"], r["population"]), axis=1
)
df_world_agg = df_world_agg[df_world_agg["Amount per 1M people (MUSD)"].notna()].copy()

fig_world = go.Figure(
    data=go.Choropleth(
        locations=df_world_agg["iso3"],
        z=df_world_agg["Amount per 1M people (MUSD)"],
        locationmode="ISO-3",
        colorscale="Blues",
        colorbar_title="M USD per 1M people",
    )
)
fig_world.update_layout(
    title_text="Amount Invested per 1M People by Country (M USD)",
    geo=dict(scope="world", projection_type="natural earth"),
    margin=dict(l=0, r=0, t=40, b=0),
)
fig_world.update_layout(font=dict(size=16))
fig_world.add_trace(
    go.Scattergeo(
        locationmode="country names",
        locations=df_world_agg["company_country"],
        text=df_world_agg["Amount per 1M people (MUSD)"].round(2).astype(str),
        mode="text",
        textfont=dict(color="black", size=12),
        showlegend=False,
    )
)
fig_world.write_html("Countries_map_amount_norm.html")
fig_world.show()


# ---------------- USA state-level map normalized by population ----------------
df_us = df[df["company_country"] == "United States"].copy()
if not df_us.empty:
    df_us = df_us.merge(
        db_exp[["company_id", "company_city"]],
        on="company_id",
        how="left",
    )
    # Normalize a few common variants so mapping is consistent with the total map
    df_us["company_city"] = df_us["company_city"].replace({
        "New York City": "New York",
        "Washington DC": "Washington",
        "St. Louis": "Saint Louis",
    })
    cities = df_us["company_city"].dropna().astype(str).str.strip().drop_duplicates().tolist()
    try:
        city_state_map, _amb, _miss = _build_city_to_state_map(cities)
        if _amb:
            print("Ambiguous city->state matches (manual intervention needed):")
            print("  " + ", ".join(sorted(_amb)))
        if _miss:
            print("Unresolved cities (no match found):")
            print("  " + ", ".join(sorted(_miss)))
        unresolved = sorted(set(c for c in cities if c and c not in city_state_map))
        if unresolved:
            print(f"Cities without a unique mapping: {len(unresolved)}")
            print("  " + ", ".join(unresolved))
    except ImportError as e:
        print(str(e))
        print("Could not resolve any US city due to missing dependency. Cities needing intervention:")
        print("  " + ", ".join(sorted(cities)))
        city_state_map = {}

    df_us["StateCode"] = df_us["company_city"].map(city_state_map)
    df_us = df_us[df_us["StateCode"].notna()]

    if not df_us.empty:
        df_us_agg = df_us.groupby("StateCode")["round_amount_usd"].sum().reset_index()
        df_us_agg.rename(columns={"round_amount_usd": "Total amount invested (M)"}, inplace=True)
        df_us_agg["population"] = df_us_agg["StateCode"].map(US_STATE_POP)
        df_us_agg = df_us_agg[df_us_agg["population"].notna()].copy()
        df_us_agg["Amount per 1M people (MUSD)"] = df_us_agg.apply(
            lambda r: _amount_per_million(r["Total amount invested (M)"], r["population"]), axis=1
        )
        df_us_agg = df_us_agg[df_us_agg["Amount per 1M people (MUSD)"].notna()].copy()

        fig_usa = go.Figure(
            data=go.Choropleth(
                locations=df_us_agg["StateCode"],
                z=df_us_agg["Amount per 1M people (MUSD)"],
                locationmode="USA-states",
                colorscale="Greens",
                colorbar_title="M USD per 1M people",
            )
        )
        fig_usa.update_layout(
            title_text="Amount Invested per 1M People by US State (M USD)",
            geo_scope="usa",
            margin=dict(l=0, r=0, t=40, b=0),
        )
        fig_usa.update_layout(font=dict(size=16))
        fig_usa.add_trace(
            go.Scattergeo(
                locationmode="USA-states",
                locations=df_us_agg["StateCode"],
                text=df_us_agg["Amount per 1M people (MUSD)"].round(2).astype(str),
                mode="text",
                textfont=dict(color="black", size=12),
                showlegend=False,
            )
        )
        fig_usa.write_html("US_states_map_amount_norm.html")
        fig_usa.show()
