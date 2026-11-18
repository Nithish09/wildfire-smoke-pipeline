# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "plotly==6.3.0",
# ]
# ///
# MAGIC %md
# MAGIC # 10 · Fire-event graph dashboard
# MAGIC
# MAGIC This notebook visualizes the outputs of `09_fire_events.py` as a geographic
# MAGIC node-and-edge graph.
# MAGIC
# MAGIC - Orange star: one selected fire event.
# MAGIC - Blue squares: counties whose centres were within the configured event-impact radius.
# MAGIC - Light gray polygons: California county boundaries.
# MAGIC - Orange lines: event-to-county impact edges from `gold_fire_event_county_impact`.
# MAGIC - Thicker lines: the event was near the county on more days.
# MAGIC
# MAGIC The current `09_fire_events.py` implementation does not persist the individual
# MAGIC detection-to-detection edges used by the union-find clustering algorithm. This
# MAGIC notebook therefore visualizes the persisted event-to-county impact graph.
# MAGIC Run `09_fire_events.py` before running this notebook.

# COMMAND ----------

import json
from urllib.request import urlopen

from pyspark.sql import functions as F
import plotly.graph_objects as go

EVENTS_TABLE = "workspace.wildfire.gold_fire_events"
EVENT_DAYS_TABLE = "workspace.wildfire.gold_fire_event_day"
IMPACT_TABLE = "workspace.wildfire.gold_fire_event_county_impact"
COUNTIES_TABLE = "workspace.wildfire.gold_dim_county"

dbutils.widgets.text(
    "event_id",
    "",
    "Event ID; blank selects the largest event",
)
dbutils.widgets.text(
    "max_counties",
    "100",
    "Maximum county nodes to display",
)

SELECTED_EVENT_ID = dbutils.widgets.get("event_id").strip()
MAX_COUNTIES = int(dbutils.widgets.get("max_counties").strip())

if MAX_COUNTIES < 1:
    raise ValueError("max_counties must be at least 1")

events = spark.table(EVENTS_TABLE)

if not SELECTED_EVENT_ID:
    largest_event = (
        events
        .orderBy(F.desc("detections"))
        .select("event_id")
        .first()
    )
    if largest_event is None:
        raise ValueError("No fire events found. Run 09_fire_events.py first.")
    SELECTED_EVENT_ID = largest_event["event_id"]

event = (
    events
    .where(F.col("event_id") == SELECTED_EVENT_ID)
    .select(
        "event_id",
        "event_label",
        "detections",
        "active_days",
        "frp_total_mw",
        "frp_max_mw",
        "centroid_lat",
        "centroid_lon",
        "first_day",
        "last_day",
    )
    .first()
)

if event is None:
    raise ValueError(f"Event not found: {SELECTED_EVENT_ID}")
if event["centroid_lat"] is None or event["centroid_lon"] is None:
    raise ValueError(f"Event {SELECTED_EVENT_ID} has no centroid coordinates")

event_lat = float(event["centroid_lat"])
event_lon = float(event["centroid_lon"])

# Aggregate multiple event-day rows into one edge per event-county pair.
edge_df = (
    spark.table(IMPACT_TABLE)
    .where(F.col("event_id") == SELECTED_EVENT_ID)
    .groupBy("event_id", "county_fips", "county_name")
    .agg(
        F.countDistinct("date_local").alias("days_nearby"),
        F.round(F.min("distance_km"), 1).alias("closest_km"),
        F.round(F.avg("upwind_alignment"), 2).alias("avg_upwind_alignment"),
        F.round(F.max("event_day_frp_mw"), 1).alias("peak_day_frp_mw"),
        F.round(F.avg("pm25_mean_ugm3"), 1).alias("avg_pm25_same_day"),
        F.round(F.avg("pm25_next_day_ugm3"), 1).alias("avg_pm25_next_day"),
    )
    .join(
        spark.table(COUNTIES_TABLE).select(
            "county_fips",
            "c_lat",
            "c_lon",
        ),
        "county_fips",
        "left",
    )
    .where(F.col("c_lat").isNotNull() & F.col("c_lon").isNotNull())
    .orderBy(F.desc("days_nearby"), F.asc("closest_km"))
    .limit(MAX_COUNTIES)
)

edge_rows = edge_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selected event summary

# COMMAND ----------

event_summary = spark.createDataFrame(
    [(
        event["event_id"],
        event["event_label"],
        event["first_day"],
        event["last_day"],
        int(event["detections"]),
        int(event["active_days"]),
        float(event["frp_total_mw"] or 0.0),
        float(event["frp_max_mw"] or 0.0),
        len(edge_rows),
    )],
    """
    event_id string,
    event_label string,
    first_day date,
    last_day date,
    detections long,
    active_days long,
    frp_total_mw double,
    frp_max_mw double,
    counties_linked long
    """,
)

# display(event_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event-to-county node-and-edge map

# COMMAND ----------

# Plotly's county GeoJSON uses five-digit FIPS identifiers.  California's
# state FIPS prefix is 06, so only the 58 California county polygons are kept.
COUNTY_GEOJSON_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "geojson-counties-fips.json"
)

with urlopen(COUNTY_GEOJSON_URL, timeout=60) as response:
    us_county_geojson = json.load(response)

ca_features = []
for feature in us_county_geojson["features"]:
    county_fips = str(feature.get("id", "")).zfill(5)
    if county_fips.startswith("06"):
        feature["id"] = county_fips
        ca_features.append(feature)

if not ca_features:
    raise ValueError("No California county polygons were found in the GeoJSON")

ca_geojson = {
    "type": "FeatureCollection",
    "features": ca_features,
}

county_border_trace = go.Choropleth(
    geojson=ca_geojson,
    locations=[feature["id"] for feature in ca_features],
    z=[1] * len(ca_features),
    featureidkey="id",
    colorscale=[
        [0, "rgba(245, 245, 245, 0.20)"],
        [1, "rgba(245, 245, 245, 0.20)"],
    ],
    showscale=False,
    marker_line_color="rgba(55, 55, 55, 0.75)",
    marker_line_width=0.8,
    hoverinfo="skip",
    name="California county borders",
)

edge_traces = []

for row in edge_rows:
    county_lat = float(row["c_lat"])
    county_lon = float(row["c_lon"])
    days_nearby = int(row["days_nearby"] or 0)
    closest_km = row["closest_km"]
    peak_frp = row["peak_day_frp_mw"]
    avg_pm25 = row["avg_pm25_same_day"]

    edge_traces.append(
        go.Scattergeo(
            lon=[event_lon, county_lon],
            lat=[event_lat, county_lat],
            mode="lines",
            line={
                "width": min(6, 1 + days_nearby / 3),
                "color": "rgba(220, 90, 40, 0.45)",
            },
            text=(
                f"{event['event_id']} → {row['county_name']}<br>"
                f"Days near event: {days_nearby}<br>"
                f"Closest distance: {closest_km} km<br>"
                f"Peak event-day FRP: {peak_frp} MW<br>"
                f"Average same-day PM2.5: {avg_pm25} µg/m³"
            ),
            hoverinfo="text",
            showlegend=False,
        )
    )

county_trace = None
if edge_rows:
    county_lats = [float(row["c_lat"]) for row in edge_rows]
    county_lons = [float(row["c_lon"]) for row in edge_rows]
    county_sizes = [
        8 + min(20, int(row["days_nearby"] or 0) * 2)
        for row in edge_rows
    ]
    county_hover = [
        (
            f"County: {row['county_name']}<br>"
            f"Days near event: {row['days_nearby']}<br>"
            f"Closest distance: {row['closest_km']} km<br>"
            f"Average same-day PM2.5: {row['avg_pm25_same_day']} µg/m³<br>"
            f"Average next-day PM2.5: {row['avg_pm25_next_day']} µg/m³"
        )
        for row in edge_rows
    ]

    county_trace = go.Scattergeo(
        lon=county_lons,
        lat=county_lats,
        mode="markers",
        name="Impacted counties",
        text=county_hover,
        hoverinfo="text",
        marker={
            "size": county_sizes,
            "color": "royalblue",
            "symbol": "square",
            "line": {"width": 1, "color": "white"},
        },
    )

event_trace = go.Scattergeo(
    lon=[event_lon],
    lat=[event_lat],
    mode="markers",
    name="Fire event",
    text=[
        (
            f"{event['event_label']}<br>"
            f"Event ID: {event['event_id']}<br>"
            f"Detections: {event['detections']}<br>"
            f"Active days: {event['active_days']}<br>"
            f"Total FRP: {event['frp_total_mw']} MW"
        )
    ],
    hoverinfo="text",
    marker={
        "size": 25,
        "color": "orangered",
        "symbol": "star",
        "line": {"width": 2, "color": "black"},
    },
)

traces = [county_border_trace] + edge_traces + (
    [county_trace] if county_trace is not None else []
)
traces.append(event_trace)

fig = go.Figure(traces)
fig.update_layout(
    title=f"Fire Event Impact Graph:",
    height=750,
    margin={"l": 0, "r": 0, "t": 60, "b": 0},
    legend={
        "orientation": "h",
        "y": 0.02,
        "x": 0.5,
        "xanchor": "center",
    },
    hovermode="closest",
)
fig.update_geos(
    scope="usa",
    projection_type="albers usa",
    projection_scale=4,
    center={"lat": event_lat, "lon": event_lon},
    showland=True,
    landcolor="rgb(235, 235, 235)",
    showlakes=True,
    lakecolor="rgb(210, 230, 245)",
    showcountries=True,
)

# Use Databricks' Plotly renderer explicitly. The generic display(fig) call can
# create a blank output container in serverless notebooks.
fig.show(renderer="databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Edge detail table

# COMMAND ----------

# display(
#     edge_df.select(
#         "county_name",
#         "days_nearby",
#         "closest_km",
#         "peak_day_frp_mw",
#         "avg_pm25_same_day",
#         "avg_pm25_next_day",
#         "avg_upwind_alignment",
#     )
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selected event timeline
# MAGIC
# MAGIC Add a line visualization to the dashboard from this result if desired.

# COMMAND ----------

timeline_df = (
    spark.table(EVENT_DAYS_TABLE)
    .where(F.col("event_id") == SELECTED_EVENT_ID)
    .select(
        "date_local",
        "detections",
        "frp_total_mw",
        "frp_max_mw",
        "centroid_lat",
        "centroid_lon",
    )
    .orderBy("date_local")
)

# display(timeline_df)