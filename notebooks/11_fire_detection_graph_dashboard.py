# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Fire-detection graph dashboard
# MAGIC
# MAGIC This notebook visualizes one fire event as a graph of individual FIRMS
# MAGIC detections. It does not create event-to-county edges.
# MAGIC
# MAGIC For each detection, the graph chooses the closest later detection that is
# MAGIC within 2 km and 24 hours. The resulting edges move forward in time, so the
# MAGIC visualization forms a chronological nearest-successor graph rather than an
# MAGIC event-to-county star.
# MAGIC
# MAGIC This is a visualization graph. The event membership itself was created by
# MAGIC 09_fire_events.py using union-find over all qualifying detection pairs.
# MAGIC The individual union-find edges are not persisted in a table.
# MAGIC Run 09_fire_events.py before running this notebook.

# COMMAND ----------

import math

from pyspark.sql import functions as F
import plotly.graph_objects as go

EVENTS_TABLE = "workspace.wildfire.gold_fire_events"
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

SELECTED_EVENT_ID = dbutils.widgets.get("event_id").strip()
MAX_DETECTIONS = int(dbutils.widgets.get("max_detections").strip())
DISPLAY_GROUP = int(dbutils.widgets.get("display_group").strip())

if MAX_DETECTIONS < 2:
    raise ValueError("max_detections must be at least 2")
if DISPLAY_GROUP < 0:
    raise ValueError("display_group must be zero or greater")

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
# MAGIC ## Fire-detection graph
# MAGIC
# MAGIC The `display_group` widget selects one connected group from the nearest-successor
# MAGIC visualization. Group 0 is the largest group.
# MAGIC
# MAGIC Circle size represents fire power (FRP). Detection color progresses from bright red
# MAGIC for the first detection to yellow for the last detection in the selected group.

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

center_lat = sum(node_lats) / len(node_lats)
center_lon = sum(node_lons) / len(node_lons)

traces = [
    go.Scattergeo(
        lon=edge_lons,
        lat=edge_lats,
        mode="lines",
        name="Nearest eligible fire link",
        line={"width": 1.5, "color": "rgba(220, 90, 40, 0.55)"},
        hoverinfo="skip",
    ),
    go.Scattergeo(
        lon=node_lons,
        lat=node_lats,
        mode="markers",
        name="Fire detections (circle size = fire power / FRP)",
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
    ),
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

fig = go.Figure(traces)
fig.update_layout(
    title=(
        f"Fire Detection Graph: {SELECTED_EVENT_ID}"
        f" | display group {DISPLAY_GROUP}"
        f" | {len(selected_rows)} detections, {len(selected_links)} links"
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
