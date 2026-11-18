# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Fire-and-smoke graph dashboard
# MAGIC
# MAGIC This notebook visualizes one fire event as a graph of individual FIRMS
# MAGIC detections and nearby counties that could be affected by smoke.
# MAGIC
# MAGIC For each detection, the graph chooses the closest later detection that is
# MAGIC within 2 km and 24 hours. The resulting edges move forward in time, so the
# MAGIC visualization forms a chronological nearest-successor graph rather than an
# MAGIC event-to-county star.
# MAGIC
# MAGIC Fire detections remain circles. Nearby counties are shown as squares, with
# MAGIC dashed paths from the event's daily centroid to the counties identified by
# MAGIC `gold_fire_event_county_impact`.
# MAGIC
# MAGIC This is a visualization graph. The event membership itself was created by
# MAGIC 09_fire_events.py using union-find over all qualifying detection pairs.
# MAGIC The individual union-find edges are not persisted in a table. Run
# MAGIC 09_fire_events.py after 04_kpi_gold.py and before running this notebook.

# COMMAND ----------

import math

from pyspark.sql import functions as F
import plotly.graph_objects as go

EVENTS_TABLE = "workspace.wildfire.gold_fire_events"
EVENT_DAY_TABLE = "workspace.wildfire.gold_fire_event_day"
IMPACT_TABLE = "workspace.wildfire.gold_fire_event_county_impact"
COUNTY_TABLE = "workspace.wildfire.gold_dim_county"
DETECTIONS_TABLE = "workspace.wildfire.silver_fire_detections"
MEMBERS_TABLE = "workspace.wildfire.silver_fire_event_members"

LINK_KM = 2.0
LINK_HOURS = 24.0

dbutils.widgets.text(
    "event_id",
    "",
    "Event ID; blank selects the largest event",
)
dbutils.widgets.text(
    "max_detections",
    "300",
    "Maximum detections to plot in chronological order",
)
dbutils.widgets.text(
    "display_group",
    "0",
    "Display group; 0 is the largest plotted group",
)
dbutils.widgets.text(
    "max_smoke_counties",
    "30",
    "Maximum nearby counties to plot",
)

SELECTED_EVENT_ID = dbutils.widgets.get("event_id").strip()
MAX_DETECTIONS = int(dbutils.widgets.get("max_detections").strip())
DISPLAY_GROUP = int(dbutils.widgets.get("display_group").strip())
MAX_SMOKE_COUNTIES = int(dbutils.widgets.get("max_smoke_counties").strip())

if MAX_DETECTIONS < 2:
    raise ValueError("max_detections must be at least 2")
if DISPLAY_GROUP < 0:
    raise ValueError("display_group must be zero or greater")
if MAX_SMOKE_COUNTIES < 1:
    raise ValueError("max_smoke_counties must be at least 1")

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
        "first_day",
        "last_day",
    )
    .first()
)

if event is None:
    raise ValueError(f"Event not found: {SELECTED_EVENT_ID}")

# 09_fire_events.py coalesces NULL satellite values to an empty string before
# writing silver_fire_event_members. Apply the same normalization before joining.
KEY = ["latitude", "longitude", "acq_ts_utc", "satellite"]
members = (
    spark.table(MEMBERS_TABLE)
    .where(F.col("event_id") == SELECTED_EVENT_ID)
    .select(*KEY, "event_id")
)

detections_df = (
    spark.table(DETECTIONS_TABLE)
    .withColumn("satellite", F.coalesce(F.col("satellite"), F.lit("")))
    .join(members, KEY, "inner")
    .select(
        "event_id",
        "latitude",
        "longitude",
        "acq_ts_utc",
        "date_local",
        "frp_mw",
        "satellite",
    )
    .where(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("acq_ts_utc").isNotNull()
    )
    .orderBy("acq_ts_utc", "latitude", "longitude", "satellite")
    .limit(MAX_DETECTIONS)
)

detection_rows = detections_df.collect()

if len(detection_rows) < 2:
    raise ValueError(
        f"Event {SELECTED_EVENT_ID} has fewer than two plottable detections"
    )

if len(detection_rows) == MAX_DETECTIONS:
    print(
        f"Showing the first {MAX_DETECTIONS} detections. "
        "Increase max_detections to plot more of this event."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build nearest-successor fire links
# MAGIC
# MAGIC Each detection has at most one outgoing link: the closest later detection
# MAGIC within LINK_KM and LINK_HOURS. Links are drawn as one combined line trace.

# COMMAND ----------

def haversine_km(lat1, lon1, lat2, lon2):
    radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * radius_km * math.asin(math.sqrt(a))


links = []

for current_index, current in enumerate(detection_rows[:-1]):
    best = None

    for next_index in range(current_index + 1, len(detection_rows)):
        candidate = detection_rows[next_index]
        elapsed_hours = (
            candidate["acq_ts_utc"] - current["acq_ts_utc"]
        ).total_seconds() / 3600.0

        if elapsed_hours > LINK_HOURS:
            break

        distance_km = haversine_km(
            float(current["latitude"]),
            float(current["longitude"]),
            float(candidate["latitude"]),
            float(candidate["longitude"]),
        )

        if distance_km <= LINK_KM:
            candidate_key = (distance_km, elapsed_hours, next_index)
            if best is None or candidate_key < best[0]:
                best = (candidate_key, next_index)

    if best is not None:
        distance_km, elapsed_hours, next_index = best[0]
        links.append(
            {
                "from_index": current_index,
                "to_index": next_index,
                "distance_km": distance_km,
                "elapsed_hours": elapsed_hours,
            }
        )

edge_lons = []
edge_lats = []

# The event_id is a union-find connected component. Because this notebook draws
# only one nearest-successor edge per detection, that visualization can contain
# several disconnected groups. Compute those display groups so one can be selected.
adjacency = [set() for _ in detection_rows]
for link in links:
    start_index = link["from_index"]
    end_index = link["to_index"]
    adjacency[start_index].add(end_index)
    adjacency[end_index].add(start_index)

component_ids = [-1] * len(detection_rows)
components = []
for start_index in range(len(detection_rows)):
    if component_ids[start_index] != -1:
        continue

    stack = [start_index]
    component = []
    component_ids[start_index] = 0
    while stack:
        current_index = stack.pop()
        component.append(current_index)
        for neighbour in adjacency[current_index]:
            if component_ids[neighbour] == -1:
                component_ids[neighbour] = 0
                stack.append(neighbour)
    components.append(sorted(component))

components.sort(key=lambda component: (-len(component), component[0]))
for group_id, component in enumerate(components):
    for index in component:
        component_ids[index] = group_id

if DISPLAY_GROUP >= len(components):
    raise ValueError(
        f"display_group {DISPLAY_GROUP} is unavailable; "
        f"choose a value from 0 to {len(components) - 1}"
    )

print(f"Available display groups for event {SELECTED_EVENT_ID}:")
for group_id, component in enumerate(components[:20]):
    first_row = detection_rows[component[0]]
    last_row = detection_rows[component[-1]]
    print(
        f"  group {group_id}: {len(component)} detections, "
        f"{first_row['acq_ts_utc']} to {last_row['acq_ts_utc']}"
    )

selected_indices = components[DISPLAY_GROUP]
selected_index_set = set(selected_indices)
selected_rows = [detection_rows[index] for index in selected_indices]
selected_links = [
    link
    for link in links
    if link["from_index"] in selected_index_set
    and link["to_index"] in selected_index_set
]

for link in selected_links:
    start = detection_rows[link["from_index"]]
    end = detection_rows[link["to_index"]]
    edge_lons.extend([float(start["longitude"]), float(end["longitude"]), None])
    edge_lats.extend([float(start["latitude"]), float(end["latitude"]), None])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load smoke-impact paths
# MAGIC
# MAGIC `09_fire_events.py` writes one row for each event-day and nearby county.
# MAGIC This notebook uses the dates in the selected fire display group, joins the
# MAGIC event's daily centroid and county coordinates, and draws a dashed path from
# MAGIC the fire centroid to each selected county.

# COMMAND ----------

for required_table in (IMPACT_TABLE, EVENT_DAY_TABLE, COUNTY_TABLE):
    if not spark.catalog.tableExists(required_table):
        raise ValueError(
            f"Required table {required_table} does not exist. "
            "Run 04_kpi_gold.py and 09_fire_events.py first."
        )

selected_dates = sorted({row["date_local"] for row in selected_rows if row["date_local"] is not None})

impact_df = (
    spark.table(IMPACT_TABLE)
    .where(
        (F.col("event_id") == SELECTED_EVENT_ID)
        & F.col("date_local").isin(selected_dates)
    )
    .join(
        spark.table(EVENT_DAY_TABLE)
        .select("event_id", "date_local", "centroid_lat", "centroid_lon")
        .alias("d"),
        ["event_id", "date_local"],
        "inner",
    )
    .join(
        spark.table(COUNTY_TABLE)
        .select("county_fips", "c_lat", "c_lon")
        .alias("c"),
        ["county_fips"],
        "left",
    )
    .select(
        "event_id",
        "date_local",
        "county_fips",
        "county_name",
        "distance_km",
        "event_day_frp_mw",
        "event_day_detections",
        "upwind_alignment",
        "event_upwind",
        "pm25_mean_ugm3",
        F.col("d.centroid_lat").alias("event_lat"),
        F.col("d.centroid_lon").alias("event_lon"),
        F.col("c.c_lat").alias("county_lat"),
        F.col("c.c_lon").alias("county_lon"),
    )
    .where(F.col("county_lat").isNotNull() & F.col("county_lon").isNotNull())
)

impact_rows = impact_df.collect()

from collections import defaultdict

smoke_by_county = defaultdict(list)
for row in impact_rows:
    smoke_by_county[str(row["county_fips"])].append(row)

ranked_smoke_groups = sorted(
    smoke_by_county.items(),
    key=lambda item: (
        -len(item[1]),
        min(float(row["distance_km"]) for row in item[1] if row["distance_km"] is not None),
        item[0],
    ),
)
selected_smoke_groups = ranked_smoke_groups[:MAX_SMOKE_COUNTIES]

smoke_nodes = []
smoke_upwind_edge_lons = []
smoke_upwind_edge_lats = []
smoke_other_edge_lons = []
smoke_other_edge_lats = []

for county_fips, rows in selected_smoke_groups:
    representative = rows[0]
    distances = [float(row["distance_km"]) for row in rows if row["distance_km"] is not None]
    alignments = [float(row["upwind_alignment"]) for row in rows if row["upwind_alignment"] is not None]
    pm25_values = [float(row["pm25_mean_ugm3"]) for row in rows if row["pm25_mean_ugm3"] is not None]
    upwind_days = sum(1 for row in rows if row["event_upwind"] is True)

    smoke_nodes.append(
        {
            "county_fips": county_fips,
            "county_name": representative["county_name"] or county_fips,
            "latitude": float(representative["county_lat"]),
            "longitude": float(representative["county_lon"]),
            "days_in_radius": len(rows),
            "upwind_days": upwind_days,
            "closest_km": min(distances) if distances else None,
            "avg_alignment": sum(alignments) / len(alignments) if alignments else None,
            "avg_pm25": sum(pm25_values) / len(pm25_values) if pm25_values else None,
        }
    )

    for row in rows:
        if row["event_lat"] is None or row["event_lon"] is None:
            continue
        target_lons = smoke_upwind_edge_lons if row["event_upwind"] is True else smoke_other_edge_lons
        target_lats = smoke_upwind_edge_lats if row["event_upwind"] is True else smoke_other_edge_lats
        target_lons.extend([float(row["event_lon"]), float(row["county_lon"]), None])
        target_lats.extend([float(row["event_lat"]), float(row["county_lat"]), None])

print(
    f"Smoke impact: {len(impact_rows)} event-day/county rows and "
    f"{len(smoke_nodes)} plotted counties."
)
if not smoke_nodes:
    print("No nearby county impact rows matched the selected display-group dates.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fire-and-smoke graph
# MAGIC
# MAGIC The `display_group` widget selects one connected group from the nearest-successor
# MAGIC visualization. Group 0 is the largest group.
# MAGIC
# MAGIC Circle size represents fire power (FRP). Detection color progresses from bright red
# MAGIC for the first detection to yellow for the last detection in the selected group.
# MAGIC Square nodes represent nearby counties identified by the smoke-impact table.
# MAGIC Dashed paths show the estimated fire-centroid-to-county smoke direction.

# COMMAND ----------

def point_size(frp_mw):
    if frp_mw is None:
        return 8
    return 7 + min(18, 2 * math.sqrt(max(float(frp_mw), 0.0)))


node_lons = [float(row["longitude"]) for row in selected_rows]
node_lats = [float(row["latitude"]) for row in selected_rows]
node_sizes = [point_size(row["frp_mw"]) for row in selected_rows]
node_colors = list(range(len(selected_rows)))
node_hover = [
    (
        f"Detection order: {original_index + 1}<br>"
        f"Display group: {DISPLAY_GROUP}<br>"
        f"Time: {row['acq_ts_utc']}<br>"
        f"FRP: {row['frp_mw']} MW<br>"
        f"Latitude: {row['latitude']}<br>"
        f"Longitude: {row['longitude']}"
    )
    for original_index, row in zip(selected_indices, selected_rows)
]

def smoke_node_size(days_in_radius):
    return 10 + min(14, 3 * math.sqrt(max(days_in_radius, 1)))


def smoke_hover(node):
    alignment = (
        f"{node['avg_alignment']:.2f}"
        if node["avg_alignment"] is not None
        else "no wind data"
    )
    pm25 = (
        f"{node['avg_pm25']:.1f} ug/m3"
        if node["avg_pm25"] is not None
        else "no PM2.5 data"
    )
    return (
        f"County: {node['county_name']} ({node['county_fips']})<br>"
        f"Event-days within impact radius: {node['days_in_radius']}<br>"
        f"Days event was upwind: {node['upwind_days']}<br>"
        f"Closest distance: {node['closest_km']:.1f} km<br>"
        f"Average upwind alignment: {alignment}<br>"
        f"Average PM2.5: {pm25}<br>"
        f"Latitude: {node['latitude']:.4f}<br>"
        f"Longitude: {node['longitude']:.4f}"
    )


smoke_upwind_nodes = [node for node in smoke_nodes if node["upwind_days"] > 0]
smoke_other_nodes = [node for node in smoke_nodes if node["upwind_days"] == 0]

all_lats = node_lats + [node["latitude"] for node in smoke_nodes]
all_lons = node_lons + [node["longitude"] for node in smoke_nodes]
center_lat = sum(all_lats) / len(all_lats)
center_lon = sum(all_lons) / len(all_lons)

traces = [
    go.Scattergeo(
        lon=edge_lons,
        lat=edge_lats,
        mode="lines",
        name="Nearest eligible fire link",
        line={"width": 1.5, "color": "rgba(220, 90, 40, 0.55)"},
        hoverinfo="skip",
    ),
]

if smoke_upwind_edge_lons:
    traces.append(
        go.Scattergeo(
            lon=smoke_upwind_edge_lons,
            lat=smoke_upwind_edge_lats,
            mode="lines",
            name="Estimated smoke path (event upwind)",
            line={"width": 1.8, "color": "rgba(126, 34, 206, 0.70)", "dash": "dash"},
            hoverinfo="skip",
        )
    )

if smoke_other_edge_lons:
    traces.append(
        go.Scattergeo(
            lon=smoke_other_edge_lons,
            lat=smoke_other_edge_lats,
            mode="lines",
            name="Nearby-county path (not upwind/unknown)",
            line={"width": 1.2, "color": "rgba(120, 120, 120, 0.45)", "dash": "dot"},
            hoverinfo="skip",
        )
    )

traces.append(
    go.Scattergeo(
        lon=node_lons,
        lat=node_lats,
        mode="markers",
        name="Fire detections (circles; size = fire power / FRP)",
        text=node_hover,
        hoverinfo="text",
        marker={
            "size": node_sizes,
            "color": node_colors,
            "colorscale": [
                [0.0, "#ff0000"],
                [1.0, "#ffff00"],
            ],
            "showscale": True,
            "colorbar": {"title": "Detection order<br>red → yellow"},
            "line": {"width": 0.7, "color": "black"},
        },
    )
)

if smoke_upwind_nodes:
    traces.append(
        go.Scattergeo(
            lon=[node["longitude"] for node in smoke_upwind_nodes],
            lat=[node["latitude"] for node in smoke_upwind_nodes],
            mode="markers",
            name="Smoke counties (squares; event upwind)",
            text=[smoke_hover(node) for node in smoke_upwind_nodes],
            hoverinfo="text",
            marker={
                "size": [smoke_node_size(node["days_in_radius"]) for node in smoke_upwind_nodes],
                "symbol": "square",
                "color": "#8a2be2",
                "line": {"width": 1.2, "color": "black"},
            },
        )
    )

if smoke_other_nodes:
    traces.append(
        go.Scattergeo(
            lon=[node["longitude"] for node in smoke_other_nodes],
            lat=[node["latitude"] for node in smoke_other_nodes],
            mode="markers",
            name="Smoke counties (squares; not upwind/unknown)",
            text=[smoke_hover(node) for node in smoke_other_nodes],
            hoverinfo="text",
            marker={
                "size": [smoke_node_size(node["days_in_radius"]) for node in smoke_other_nodes],
                "symbol": "square",
                "color": "#708090",
                "line": {"width": 1.2, "color": "black"},
            },
        )
    )

traces.extend(
    [
        go.Scattergeo(
            lon=[node_lons[0]],
            lat=[node_lats[0]],
            mode="markers",
            name="First detection",
            marker={
                "size": 17,
                "color": "limegreen",
                "symbol": "star",
                "line": {"width": 1.5, "color": "black"},
            },
            hoverinfo="skip",
        ),
        go.Scattergeo(
            lon=[node_lons[-1]],
            lat=[node_lats[-1]],
            mode="markers",
            name="Last plotted detection",
            marker={
                "size": 15,
                "color": "royalblue",
                "symbol": "diamond",
                "line": {"width": 1.5, "color": "black"},
            },
            hoverinfo="skip",
        ),
    ]
)

fig = go.Figure(traces)
fig.update_layout(
    title=(
        f"Fire and Smoke Graph: {SELECTED_EVENT_ID}"
        f" | display group {DISPLAY_GROUP}"
        f" | {len(selected_rows)} fire detections, {len(selected_links)} fire links"
        f" | {len(smoke_nodes)} smoke counties"
    ),
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
    projection_scale=5,
    center={"lat": center_lat, "lon": center_lon},
    showland=True,
    landcolor="rgb(235, 235, 235)",
    showlakes=True,
    lakecolor="rgb(210, 230, 245)",
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="rgb(100, 100, 100)",
)

fig.show(renderer="databricks")

