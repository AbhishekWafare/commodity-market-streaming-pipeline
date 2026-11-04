import math
import os
from contextlib import closing

import pandas as pd
import psycopg2
import streamlit as st


# -----------------------------------------------------------------------------
# PAGE CONFIGURATION
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Commodity Market Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Performance guardrails. The dashboard intentionally loads a bounded history
# and paginates the raw table so every Streamlit rerun stays responsive.
MAX_LOAD_ROWS = 20_000
MAX_CHART_POINTS = 2_000
RAW_PAGE_SIZE = 100
CACHE_TTL_SECONDS = 45

COMMODITY_LABELS = {
    "gold": "Gold",
    "silver": "Silver",
    "oil": "Crude Oil",
}


# -----------------------------------------------------------------------------
# DATABASE CONNECTION
# Uses Kafka/.env POSTGRES_* values when launched by start-project.sh.
# KAFKA_* names remain as fallbacks for compatibility with the old dashboard.
# -----------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST") or os.getenv("KAFKA_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT") or os.getenv("KAFKA_PORT", "5434")),
        dbname=os.getenv("POSTGRES_DB") or os.getenv("KAFKA_DB", "kafka_db"),
        user=os.getenv("POSTGRES_USER") or os.getenv("KAFKA_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD") or os.getenv("KAFKA_PASSWORD", "postgres"),
        connect_timeout=5,
        options="-c statement_timeout=10000",
    )


# -----------------------------------------------------------------------------
# DATA LOADING
# Streamlit reruns the script when widgets change. Cache DB results, cap the
# amount loaded, and keep only one cached result to control memory usage.
# -----------------------------------------------------------------------------
@st.cache_data(
    ttl=CACHE_TTL_SECONDS,
    max_entries=1,
    show_spinner="Loading commodity records from PostgreSQL...",
)
def load_data(row_limit: int = MAX_LOAD_ROWS) -> pd.DataFrame:
    query = """
        SELECT id, commodity, price, currency, timestamp
        FROM commodity_prices
        ORDER BY timestamp DESC, id DESC
        LIMIT %s;
    """

    with closing(get_connection()) as conn:
        dataframe = pd.read_sql_query(query, conn, params=(row_limit,))

    if dataframe.empty:
        return dataframe

    dataframe["commodity"] = dataframe["commodity"].astype(str).str.lower()
    dataframe["price"] = pd.to_numeric(dataframe["price"], errors="coerce")
    dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"], errors="coerce")
    dataframe["currency"] = dataframe["currency"].fillna("Unknown").astype(str)
    dataframe = dataframe.dropna(subset=["commodity", "price", "timestamp"])

    # Restore ascending order for time-series charts.
    return dataframe.sort_values(["timestamp", "id"]).reset_index(drop=True)


def format_price(value, currency):
    if pd.isna(value):
        return "—"
    return f"{value:,.4f} {currency}"


def display_connection_help(error):
    st.error("Could not connect to the commodity PostgreSQL database.")
    st.code(str(error))
    st.markdown(
        """
        Check that:
        - `commodity-kafka-postgres-1` is running.
        - The commodity PostgreSQL host port is `5434`.
        - The dashboard was launched with variables from `Kafka/.env` loaded.
        - The `commodity_prices` table exists in `kafka_db`.
        """
    )


def sample_chart_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Reduce chart points on large histories without changing the raw data."""
    if len(dataframe) <= MAX_CHART_POINTS:
        return dataframe

    step = math.ceil(len(dataframe) / MAX_CHART_POINTS)
    sampled = dataframe.iloc[::step]

    # Preserve the newest point even when it does not fall on the sampling step.
    if not sampled.index.equals(dataframe.tail(1).index):
        sampled = pd.concat([sampled, dataframe.tail(1)])

    return sampled.drop_duplicates(subset=["id"]).sort_values(["timestamp", "id"])


# -----------------------------------------------------------------------------
# SIDEBAR: REFRESH AND FILTERS
# A form batches changes so every dropdown interaction does not rerun the full
# dashboard. The user applies the filter set once with the button.
# -----------------------------------------------------------------------------
st.sidebar.title("Dashboard Controls")

if st.sidebar.button("↻ Refresh data", use_container_width=True):
    load_data.clear()
    st.rerun()

try:
    df = load_data()
except Exception as error:
    st.title("📈 Commodity Market Dashboard")
    display_connection_help(error)
    st.stop()

st.title("📈 Commodity Market Dashboard")
st.caption(
    "Gold, Silver and Crude Oil events ingested through Kafka and stored in PostgreSQL. "
    f"Data refreshes from the database every {CACHE_TTL_SECONDS} seconds when the app reruns."
)

if df.empty:
    st.info(
        "No commodity records are available yet. Start the Kafka producer and consumer, "
        "then refresh this dashboard."
    )
    st.stop()

if len(df) == MAX_LOAD_ROWS:
    st.info(
        f"For responsiveness, this dashboard loads at most the latest {MAX_LOAD_ROWS:,} "
        "records. Statistics and charts below use this loaded history."
    )

available_commodities = sorted(df["commodity"].dropna().unique().tolist())
min_date = df["timestamp"].min().date()
max_date = df["timestamp"].max().date()

with st.sidebar.form("dashboard_filters"):
    st.subheader("Filters")
    selected_commodities = st.multiselect(
        "Commodities",
        options=available_commodities,
        default=available_commodities,
        format_func=lambda value: COMMODITY_LABELS.get(value, value.title()),
    )

    period = st.selectbox(
        "Date range",
        options=[
            "All loaded data",
            "Last 24 hours",
            "Last 7 days",
            "Last 30 days",
            "Custom range",
        ],
        index=0,
    )

    selected_dates = None
    if period == "Custom range":
        selected_dates = st.date_input(
            "Select date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    apply_filters = st.form_submit_button("Apply filters", use_container_width=True)

# Read the selected widget values after the user submits the form.
filtered_df = df[df["commodity"].isin(selected_commodities)].copy()

if period == "Last 24 hours":
    cutoff = pd.Timestamp.now() - pd.Timedelta(hours=24)
    filtered_df = filtered_df[filtered_df["timestamp"] >= cutoff]
elif period == "Last 7 days":
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
    filtered_df = filtered_df[filtered_df["timestamp"] >= cutoff]
elif period == "Last 30 days":
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
    filtered_df = filtered_df[filtered_df["timestamp"] >= cutoff]
elif period == "Custom range" and selected_dates is not None:
    if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
        filtered_df = filtered_df[
            filtered_df["timestamp"].dt.date.between(start_date, end_date)
        ]
    else:
        st.sidebar.caption("Select both a start date and an end date.")

st.sidebar.caption(f"Showing {len(filtered_df):,} of {len(df):,} loaded records")

if filtered_df.empty:
    st.warning("No records match the selected filters. Adjust the filters or refresh the data.")
    st.stop()


# -----------------------------------------------------------------------------
# MARKET OVERVIEW
# -----------------------------------------------------------------------------
st.subheader("Market Overview")
latest_timestamp = filtered_df["timestamp"].max()
active_count = filtered_df["commodity"].nunique()

metric1, metric2, metric3 = st.columns(3)
metric1.metric("Total Events", f"{len(filtered_df):,}")
metric2.metric("Commodities", f"{active_count}")
metric3.metric("Latest Event", latest_timestamp.strftime("%d %b %Y, %H:%M:%S"))


# -----------------------------------------------------------------------------
# LATEST PRICE CARDS
# -----------------------------------------------------------------------------
st.subheader("Latest Commodity Prices")
latest_per_commodity = (
    filtered_df.sort_values(["timestamp", "id"])
    .groupby("commodity", as_index=False)
    .tail(1)
)

card_columns = st.columns(max(1, len(latest_per_commodity)))
for column, (_, record) in zip(card_columns, latest_per_commodity.iterrows()):
    commodity = record["commodity"]
    with column:
        st.metric(
            label=COMMODITY_LABELS.get(commodity, commodity.title()),
            value=format_price(record["price"], record["currency"]),
            help=f"Latest event: {record['timestamp'].strftime('%d %b %Y, %H:%M:%S')}",
        )
        st.caption(f"Updated {record['timestamp'].strftime('%d %b %Y at %H:%M:%S')}")


# -----------------------------------------------------------------------------
# SUMMARY STATISTICS
# -----------------------------------------------------------------------------
st.subheader("Commodity Statistics")
stats_df = (
    filtered_df.groupby(["commodity", "currency"], as_index=False)
    .agg(
        events=("id", "count"),
        average_price=("price", "mean"),
        highest_price=("price", "max"),
        lowest_price=("price", "min"),
    )
)

latest_stats = (
    filtered_df.sort_values(["timestamp", "id"])
    .groupby(["commodity", "currency"], as_index=False)
    .tail(1)[["commodity", "currency", "price", "timestamp"]]
    .rename(columns={"price": "latest_price", "timestamp": "latest_timestamp"})
)
stats_df = stats_df.merge(latest_stats, on=["commodity", "currency"], how="left")
stats_df["Commodity"] = stats_df["commodity"].map(
    lambda value: COMMODITY_LABELS.get(value, value.title())
)
stats_df = stats_df.rename(
    columns={
        "currency": "Currency",
        "events": "Events",
        "average_price": "Average Price",
        "highest_price": "Highest Price",
        "lowest_price": "Lowest Price",
        "latest_price": "Latest Price",
        "latest_timestamp": "Latest Timestamp",
    }
)[
    [
        "Commodity",
        "Currency",
        "Events",
        "Average Price",
        "Highest Price",
        "Lowest Price",
        "Latest Price",
        "Latest Timestamp",
    ]
]

st.dataframe(
    stats_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Average Price": st.column_config.NumberColumn(format="%.4f"),
        "Highest Price": st.column_config.NumberColumn(format="%.4f"),
        "Lowest Price": st.column_config.NumberColumn(format="%.4f"),
        "Latest Price": st.column_config.NumberColumn(format="%.4f"),
        "Latest Timestamp": st.column_config.DatetimeColumn(format="DD MMM YYYY, HH:mm:ss"),
    },
)


# -----------------------------------------------------------------------------
# PRICE TRENDS
# -----------------------------------------------------------------------------
st.subheader("Price Trends")
trend_commodities = [
    commodity for commodity in ["gold", "silver", "oil"]
    if commodity in selected_commodities
]
trend_commodities += [
    commodity for commodity in selected_commodities
    if commodity not in {"gold", "silver", "oil"}
]

if trend_commodities:
    trend_tabs = st.tabs(
        [COMMODITY_LABELS.get(value, value.title()) for value in trend_commodities]
    )

    for tab, commodity in zip(trend_tabs, trend_commodities):
        with tab:
            commodity_df = filtered_df[filtered_df["commodity"] == commodity].copy()
            commodity_df = commodity_df.sort_values(["timestamp", "id"])

            if commodity_df.empty:
                st.info("No records for this commodity in the selected period.")
            else:
                currencies = sorted(commodity_df["currency"].dropna().unique().tolist())
                if len(currencies) > 1:
                    selected_currency = st.selectbox(
                        "Currency",
                        currencies,
                        key=f"currency_{commodity}",
                    )
                    commodity_df = commodity_df[
                        commodity_df["currency"] == selected_currency
                    ]

                chart_df = sample_chart_data(commodity_df).set_index("timestamp")[["price"]]
                st.line_chart(chart_df, y="price", use_container_width=True)
                st.caption(
                    f"{len(commodity_df):,} events · "
                    f"Currency: {commodity_df['currency'].iloc[-1]} · "
                    f"Latest: {commodity_df['timestamp'].max().strftime('%d %b %Y, %H:%M:%S')}"
                )


# -----------------------------------------------------------------------------
# RAW RECORDS - PAGINATED
# -----------------------------------------------------------------------------
st.subheader("Raw Commodity Records")
raw_df = filtered_df.sort_values(
    ["timestamp", "id"], ascending=[False, False]
).reset_index(drop=True)

page_count = max(1, math.ceil(len(raw_df) / RAW_PAGE_SIZE))
page_number = st.selectbox(
    "Records page",
    options=list(range(1, page_count + 1)),
    format_func=lambda value: f"Page {value} of {page_count}",
)
start = (page_number - 1) * RAW_PAGE_SIZE
end = start + RAW_PAGE_SIZE
st.dataframe(
    raw_df.iloc[start:end],
    use_container_width=True,
    hide_index=True,
)
st.caption(
    f"Showing records {start + 1:,}–{min(end, len(raw_df)):,} of {len(raw_df):,} filtered records."
)

st.caption(
    f"Source: PostgreSQL · Table: commodity_prices · Cache TTL: {CACHE_TTL_SECONDS}s · "
    f"Maximum loaded history: {MAX_LOAD_ROWS:,} records"
)
