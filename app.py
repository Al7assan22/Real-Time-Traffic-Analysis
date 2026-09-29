import json
import threading
from collections import deque
from datetime import datetime
from html import escape

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from azure.eventhub import EventHubConsumerClient


# --------------------------------------------------
# Page Configuration
# --------------------------------------------------

st.set_page_config(
    page_title="Real-Time Traffic Analytics",
    page_icon="🚦",
    layout="wide"
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

EVENTHUB_NAME = "traffic-data"
CONSUMER_GROUP = "$Default"

MAX_EVENTS = 500

SPEED_LIMIT = 100


# --------------------------------------------------
# Styling
# --------------------------------------------------

st.markdown(
    """
<style>
#MainMenu, footer, header[data-testid="stHeader"] {visibility: hidden; height: 0;}
.stApp {
    background-color: #090d16;
    background-image:
        radial-gradient(900px 520px at 6% -6%, rgba(56,189,248,.18), transparent 60%),
        radial-gradient(800px 520px at 96% 0%, rgba(129,140,248,.16), transparent 60%),
        radial-gradient(1000px 600px at 50% 115%, rgba(34,197,94,.08), transparent 60%),
        linear-gradient(rgba(255,255,255,.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,.03) 1px, transparent 1px);
    background-size: auto, auto, auto, 46px 46px, 46px 46px;
    background-attachment: fixed;
}
.stApp::before {
    content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 0;
    background: radial-gradient(ellipse at center, transparent 55%, rgba(0,0,0,.45) 100%);
}
.block-container {position: relative; z-index: 1;}
.block-container {padding-top: 1.2rem; padding-bottom: 1rem; max-width: 1400px;}

.topbar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 20px; margin-bottom: 14px;
    border-radius: 14px;
    background: linear-gradient(90deg, #172036 0%, #111827 100%);
    border: 1px solid #263048;
}
.topbar .title {font-size: 1.25rem; font-weight: 700; color: #f3f4f6; letter-spacing: .3px;}
.topbar .sub {font-size: .75rem; color: #94a3b8; margin-top: 2px;}
.live {
    display: flex; align-items: center; gap: 8px;
    font-size: .72rem; font-weight: 700; letter-spacing: 1.5px; color: #22c55e;
    background: rgba(34,197,94,.10); border: 1px solid rgba(34,197,94,.35);
    padding: 6px 12px; border-radius: 999px;
}
.live i {
    width: 8px; height: 8px; border-radius: 50%; background: #22c55e;
    animation: pulse 1.6s infinite;
}
@keyframes pulse {
    0% {box-shadow: 0 0 0 0 rgba(34,197,94,.6);}
    70% {box-shadow: 0 0 0 8px rgba(34,197,94,0);}
    100% {box-shadow: 0 0 0 0 rgba(34,197,94,0);}
}

.section {
    font-size: .78rem; font-weight: 700; color: #94a3b8;
    text-transform: uppercase; letter-spacing: 2px;
    margin: 18px 0 8px 2px;
}

.status-card {
    border-radius: 16px; padding: 16px 20px 18px 20px;
    border: 1.5px solid var(--c);
    background: linear-gradient(135deg, var(--bg) 0%, rgba(17,24,39,.6) 100%);
    box-shadow: 0 0 24px var(--glow);
}
.status-head {display: flex; align-items: center; gap: 10px; margin-bottom: 14px;}
.status-dot {width: 10px; height: 10px; border-radius: 50%; background: var(--c); box-shadow: 0 0 10px var(--c);}
.status-label {font-size: .8rem; font-weight: 800; letter-spacing: 1.8px; color: var(--c);}
.status-sub {font-size: .72rem; color: #9ca3af; margin-left: auto;}

.grid {display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px;}
.cell {
    background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px; padding: 10px 14px;
}
.cell .k {font-size: .66rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1.2px;}
.cell .v {font-size: 1.05rem; font-weight: 600; color: #f3f4f6; margin-top: 3px;
          white-space: nowrap; overflow: hidden; text-overflow: ellipsis;}

.kpi {
    background: linear-gradient(160deg, #1a2133 0%, #131a29 100%);
    border: 1px solid #263048; border-left: 3px solid var(--a);
    border-radius: 12px; padding: 12px 16px;
}
.kpi .k {font-size: .66rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1.2px;}
.kpi .v {font-size: 1.3rem; font-weight: 700; color: #f3f4f6; margin-top: 3px;}

div[data-testid="stVerticalBlockBorderWrapper"] {
    border-radius: 14px; border-color: #263048 !important;
    background: rgba(17,24,39,.55);
}
.foot {text-align: center; font-size: .72rem; color: #64748b; margin-top: 14px;}

@media (max-width: 900px) {.grid {grid-template-columns: repeat(2, 1fr);}}
</style>
""",
    unsafe_allow_html=True
)


def cell(label, value):
    return (
        f'<div class="cell"><div class="k">{escape(str(label))}</div>'
        f'<div class="v">{escape(str(value))}</div></div>'
    )


def kpi(label, value, accent="#38bdf8"):
    return (
        f'<div class="kpi" style="--a:{accent}"><div class="k">{escape(str(label))}</div>'
        f'<div class="v">{escape(str(value))}</div></div>'
    )


def section(text):
    st.markdown(f'<div class="section">{text}</div>', unsafe_allow_html=True)


# --------------------------------------------------
# Shared Event Storage
# --------------------------------------------------

@st.cache_resource
def get_storage():

    events = deque(maxlen=MAX_EVENTS)

    lock = threading.Lock()

    return events, lock


events, lock = get_storage()


# --------------------------------------------------
# Receive Event from Azure Event Hub
# --------------------------------------------------

def receive_event(partition_context, event):

    if event is None:
        return

    try:

        data = json.loads(
            event.body_as_str()
        )

        with lock:
            events.append(data)

        print("Received:", data)

    except Exception as error:

        print(
            f"Error reading event: {error}"
        )


# --------------------------------------------------
# Start Azure Event Hub Consumer
# --------------------------------------------------

@st.cache_resource
def start_consumer():

    connection_string = st.secrets[
        "EVENT_HUB_CONNECTION_STR"
    ]

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME
    )

    def run_consumer():

        try:

            print("Consumer is running...")

            consumer.receive(
                on_event=receive_event,
                starting_position="-1"
            )

        except Exception as error:

            print(
                "CONSUMER ERROR:",
                error
            )

    thread = threading.Thread(
        target=run_consumer,
        daemon=True
    )

    thread.start()

    return consumer


start_consumer()


# --------------------------------------------------
# Traffic Dashboard
# --------------------------------------------------

@st.fragment(run_every=3)
def traffic_dashboard():

    # --------------------------------------------------
    # Header
    # --------------------------------------------------

    st.markdown(
        '<div class="topbar"><div>'
        '<div class="title">🚦 Real-Time Traffic Analytics</div>'
        '<div class="sub">Live traffic monitoring and vehicle activity</div>'
        '</div><div class="live"><i></i>LIVE</div></div>',
        unsafe_allow_html=True
    )

    # --------------------------------------------------
    # Get Events
    # --------------------------------------------------

    with lock:
        current_events = list(events)

    if len(current_events) == 0:

        st.info(
            "Waiting for traffic data from Azure Event Hub..."
        )

        return


    # --------------------------------------------------
    # Create DataFrame
    # --------------------------------------------------

    df = pd.DataFrame(
        current_events
    )

    df["Time"] = pd.to_datetime(
        df["Time"]
    )

    df["Speed"] = pd.to_numeric(
        df["Speed"]
    )

    df = df.sort_values(
        "Time"
    )


    # --------------------------------------------------
    # Latest Vehicle
    # --------------------------------------------------

    latest_vehicle = df.iloc[-1]

    latest_car = latest_vehicle["Car"]

    latest_speed = latest_vehicle["Speed"]

    latest_location = latest_vehicle["Location"]

    latest_time = latest_vehicle["Time"]


    # --------------------------------------------------
    # Current Vehicle Card (green = normal, red = warning)
    # --------------------------------------------------

    section("Current Vehicle")

    if latest_speed > SPEED_LIMIT:
        color = "#ef4444"
        bg = "rgba(239,68,68,0.14)"
        glow = "rgba(239,68,68,0.28)"
        label = "WARNING • OVER SPEED LIMIT"
    else:
        color = "#22c55e"
        bg = "rgba(34,197,94,0.12)"
        glow = "rgba(34,197,94,0.20)"
        label = "NORMAL"

    st.markdown(
        f'<div class="status-card" style="--c:{color};--bg:{bg};--glow:{glow};">'
        f'<div class="status-head"><span class="status-dot"></span>'
        f'<span class="status-label">{label}</span>'
        f'<span class="status-sub">Speed limit: {SPEED_LIMIT} km/h</span></div>'
        f'<div class="grid">'
        + cell("Vehicle", latest_car)
        + cell("Speed", f"{latest_speed} km/h")
        + cell("Road / Location", latest_location)
        + cell("Date", latest_time.strftime("%Y-%m-%d"))
        + cell("Time", latest_time.strftime("%H:%M:%S"))
        + '</div></div>',
        unsafe_allow_html=True
    )


    # --------------------------------------------------
    # Traffic Statistics
    # --------------------------------------------------

    total_vehicles = df["Car"].nunique()

    average_speed = df["Speed"].mean()

    minimum_speed = df["Speed"].min()

    maximum_speed = df["Speed"].max()

    warning_count = (
        df["Speed"] > SPEED_LIMIT
    ).sum()


    section("Traffic Overview")

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.markdown(kpi("Vehicles", total_vehicles, "#38bdf8"), unsafe_allow_html=True)
    col2.markdown(kpi("Average Speed", f"{average_speed:.1f} km/h", "#818cf8"), unsafe_allow_html=True)
    col3.markdown(kpi("Minimum Speed", f"{minimum_speed} km/h", "#22c55e"), unsafe_allow_html=True)
    col4.markdown(kpi("Maximum Speed", f"{maximum_speed} km/h", "#f59e0b"), unsafe_allow_html=True)
    col5.markdown(
        kpi("Warnings", warning_count, "#ef4444" if warning_count > 0 else "#22c55e"),
        unsafe_allow_html=True
    )


    # --------------------------------------------------
    # Charts
    # --------------------------------------------------

    section("Analytics")

    chart_col1, chart_col2 = st.columns(2)

    chart_layout = dict(
        template="plotly_dark",
        height=320,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=11, color="#cbd5e1"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
    )

    base_layout = {
        k: v for k, v in chart_layout.items()
        if k not in ("xaxis", "yaxis")
    }


    # --------------------------------------------------
    # Speed Trend
    # --------------------------------------------------

    with chart_col1:

        with st.container(border=True):

            st.markdown("**Speed Trend**")

            speed_chart = px.line(
                df,
                x="Time",
                y="Speed",
                markers=True
            )

            speed_chart.update_traces(
                line=dict(color="#38bdf8", width=2),
                marker=dict(size=5)
            )

            speed_chart.add_hline(
                y=SPEED_LIMIT,
                line_dash="dash",
                line_color="#ef4444",
                annotation_text="Limit",
                annotation_font_size=10
            )

            speed_chart.update_layout(
                **chart_layout,
                xaxis_title="Time",
                yaxis_title="Speed (km/h)"
            )

            st.plotly_chart(
                speed_chart,
                use_container_width=True
            )


    # --------------------------------------------------
    # Traffic by Road
    # --------------------------------------------------

    with chart_col2:

        with st.container(border=True):

            st.markdown("**Traffic by Road**")

            location_data = (
                df.groupby("Location")
                .size()
                .reset_index(
                    name="Vehicles"
                )
                .sort_values(
                    "Vehicles",
                    ascending=False
                )
            )

            location_chart = px.bar(
                location_data,
                x="Location",
                y="Vehicles"
            )

            location_chart.update_traces(
                marker_color="#818cf8"
            )

            location_chart.update_layout(
                **chart_layout,
                xaxis_title="Road",
                yaxis_title="Vehicles"
            )

            st.plotly_chart(
                location_chart,
                use_container_width=True
            )


    # --------------------------------------------------
    # Monitoring Charts
    # --------------------------------------------------

    section("Monitoring")

    gauge_max = max(SPEED_LIMIT * 1.5, float(maximum_speed))

    g1, g2, g3 = st.columns(3)


    # Live speed gauge
    with g1:

        with st.container(border=True):

            st.markdown("**Live Speed Gauge**")

            gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=float(latest_speed),
                    number={"suffix": " km/h", "font": {"size": 32}},
                    gauge={
                        "axis": {"range": [0, gauge_max]},
                        "bar": {"color": color},
                        "steps": [
                            {"range": [0, SPEED_LIMIT], "color": "rgba(34,197,94,0.20)"},
                            {"range": [SPEED_LIMIT, gauge_max], "color": "rgba(239,68,68,0.20)"},
                        ],
                        "threshold": {
                            "line": {"color": "#ef4444", "width": 3},
                            "value": SPEED_LIMIT,
                        },
                    },
                )
            )

            gauge.update_layout(
                **{**base_layout, "margin": dict(l=25, r=25, t=30, b=10)}
            )

            st.plotly_chart(gauge, use_container_width=True)


    # Normal vs warning share
    with g2:

        with st.container(border=True):

            st.markdown("**Normal vs Warnings**")

            normal_count = len(df) - int(warning_count)

            donut = go.Figure(
                go.Pie(
                    labels=["Normal", "Warnings"],
                    values=[normal_count, int(warning_count)],
                    hole=0.62,
                    marker=dict(colors=["#22c55e", "#ef4444"]),
                    textinfo="percent",
                    sort=False,
                )
            )

            donut.update_layout(
                **base_layout,
                legend=dict(orientation="h", y=-0.05, x=0.25)
            )

            st.plotly_chart(donut, use_container_width=True)


    # Speed distribution
    with g3:

        with st.container(border=True):

            st.markdown("**Speed Distribution**")

            hist = px.histogram(df, x="Speed", nbins=20)

            hist.update_traces(marker_color="#38bdf8")

            hist.add_vline(
                x=SPEED_LIMIT,
                line_dash="dash",
                line_color="#ef4444"
            )

            hist.update_layout(
                **chart_layout,
                xaxis_title="Speed (km/h)",
                yaxis_title="Vehicles"
            )

            st.plotly_chart(hist, use_container_width=True)


    h1, h2, h3 = st.columns(3)


    # Average speed by road
    with h1:

        with st.container(border=True):

            st.markdown("**Average Speed by Road**")

            road_speed = (
                df.groupby("Location")["Speed"]
                .mean()
                .reset_index()
                .sort_values("Speed", ascending=False)
            )

            road_chart = px.bar(road_speed, x="Location", y="Speed")

            road_chart.update_traces(
                marker_color=[
                    "#ef4444" if v > SPEED_LIMIT else "#22c55e"
                    for v in road_speed["Speed"]
                ]
            )

            road_chart.add_hline(
                y=SPEED_LIMIT,
                line_dash="dash",
                line_color="#ef4444"
            )

            road_chart.update_layout(
                **chart_layout,
                xaxis_title="Road",
                yaxis_title="Avg Speed (km/h)"
            )

            st.plotly_chart(road_chart, use_container_width=True)


    # Top fastest vehicles
    with h2:

        with st.container(border=True):

            st.markdown("**Top 5 Fastest Vehicles**")

            top_cars = (
                df.sort_values("Speed", ascending=False)
                .drop_duplicates("Car")
                .head(5)
                .sort_values("Speed")
            )

            top_chart = px.bar(
                top_cars,
                x="Speed",
                y="Car",
                orientation="h"
            )

            top_chart.update_traces(
                marker_color=[
                    "#ef4444" if v > SPEED_LIMIT else "#818cf8"
                    for v in top_cars["Speed"]
                ]
            )

            top_chart.update_layout(
                **chart_layout,
                xaxis_title="Speed (km/h)",
                yaxis_title=""
            )

            st.plotly_chart(top_chart, use_container_width=True)


    # Recent violations
    with h3:

        with st.container(border=True):

            st.markdown("**Recent Violations**")

            violations = df[df["Speed"] > SPEED_LIMIT].sort_values(
                "Time",
                ascending=False
            ).head(6)

            if violations.empty:

                st.success("No violations recorded")

            else:

                violations = violations[
                    ["Time", "Car", "Speed", "Location"]
                ].copy()

                violations["Time"] = violations["Time"].dt.strftime("%H:%M:%S")

                st.dataframe(
                    violations,
                    use_container_width=True,
                    hide_index=True,
                    height=270
                )


    # --------------------------------------------------
    # Recent Vehicles
    # --------------------------------------------------

    section("Recent Vehicles")

    recent_vehicles = (
        df.sort_values(
            "Time",
            ascending=False
        )
        .head(10)
    )

    recent_vehicles = recent_vehicles[
        [
            "Time",
            "Car",
            "Speed",
            "Location"
        ]
    ].reset_index(drop=True)

    def highlight_warning(row):
        if row["Speed"] > SPEED_LIMIT:
            return ["background-color: rgba(239,68,68,0.20)"] * len(row)
        return [""] * len(row)

    styled = (
        recent_vehicles.style
        .apply(highlight_warning, axis=1)
        .format({"Time": lambda t: t.strftime("%Y-%m-%d %H:%M:%S")})
    )

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True
    )


    # --------------------------------------------------
    # Footer
    # --------------------------------------------------

    st.markdown(
        f'<div class="foot">Live data from Azure Event Hub • '
        f'Updates every 3 seconds • Last refresh {datetime.now().strftime("%H:%M:%S")}</div>',
        unsafe_allow_html=True
    )


# --------------------------------------------------
# Run Dashboard
# --------------------------------------------------

traffic_dashboard()