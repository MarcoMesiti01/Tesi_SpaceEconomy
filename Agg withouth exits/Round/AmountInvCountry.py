import pandas as pd
import Library as mylib
import plotly.express as px
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
        if len(states) == 1:
            mapping[key] = next(iter(states))
        else:
            ambiguous.add(key)
    return mapping, ambiguous, missing


# Load rounds and export; filter only space firms
df = mylib.openDB("rounds")
db_exp = pd.read_parquet("DB_Out/DB_export.parquet", columns=["company_id", "company_all_tags", "company_city", "company_country"])
db_exp = mylib.space(db_exp, "company_id", True)
db_ids = db_exp["company_id"]
df = df[df["Target firm ID"].isin(db_ids)]

# Keep needed fields and scale amount to millions
df = df[["Target firm ID", "Firm country", "AmountUSD"]].copy()
df["AmountUSD"] = df["AmountUSD"].apply(lambda x: x/1000000 if not pd.isna(x) else x)

# ---------------- World map (exclude USA) ----------------
df_world = df[df["Firm country"].notna()].copy()
df_world = df_world[df_world["Firm country"] != "United States"]
df_world_agg = df_world.groupby("Firm country")["AmountUSD"].agg(["sum", "mean", "count", "std"]).reset_index()
df_world_agg.rename(columns={
    "sum": "Total amount invested",
    "mean": "Average round size",
    "count": "Number of round",
    "std": "Variance measure (std)",
}, inplace=True)

fig_world = mylib.makeMap(df_world_agg, "Total amount invested")
fig_world.write_html("Countries_map_amount.html")

# ---------------- USA state-level map ----------------
df_us = df[df["Firm country"] == "United States"].copy()
if not df_us.empty:
    # Map company_id -> city, then resolve city -> state code
    df_us = df_us.merge(
        db_exp[["company_id", "company_city"]],
        left_on="Target firm ID",
        right_on="company_id",
        how="left",
    )
    cities = df_us["company_city"].dropna().astype(str).str.strip().drop_duplicates().tolist()
    try:
        city_state_map, _amb, _miss = _build_city_to_state_map(cities)
    except ImportError as e:
        print(str(e))
        city_state_map = {}

    df_us["StateCode"] = df_us["company_city"].map(city_state_map)
    df_us = df_us[df_us["StateCode"].notna()]

    if not df_us.empty:
        df_us_agg = df_us.groupby("StateCode")["AmountUSD"].sum().reset_index()
        df_us_agg.rename(columns={"AmountUSD": "Total amount invested (M)"}, inplace=True)

        fig_usa = go.Figure(
            data=go.Choropleth(
                locations=df_us_agg["StateCode"],
                z=df_us_agg["Total amount invested (M)"],
                locationmode="USA-states",
                colorscale="Reds",
                colorbar_title="Amount (M USD)",
            )
        )
        fig_usa.update_layout(
            title_text="Total Amount Invested by US State (M USD)",
            geo_scope="usa",
            margin=dict(l=0, r=0, t=40, b=0),
        )
        # Overlay numeric labels
        fig_usa.add_trace(
            go.Scattergeo(
                locationmode="USA-states",
                locations=df_us_agg["StateCode"],
                text=df_us_agg["Total amount invested (M)"].round().astype(int).astype(str),
                mode="text",
                textfont=dict(color="black", size=9),
                showlegend=False,
            )
        )
        fig_usa.write_html("US_states_map_amount.html")
