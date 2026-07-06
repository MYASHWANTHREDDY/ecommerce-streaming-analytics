import os
from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

PG_HOST = os.environ.get("POSTGRES_HOST_EXTERNAL", "localhost")
PG_PORT = os.environ.get("POSTGRES_PORT_EXTERNAL", "5433")
PG_DB = os.environ.get("POSTGRES_DB", "analytics")
PG_USER = os.environ.get("POSTGRES_USER", "pipeline")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "pipeline")

LIVE_LOOKBACK_MINUTES = 30

st.set_page_config(page_title="Real-Time E-Commerce Analytics", layout="wide")


def run_query(sql: str) -> pd.DataFrame:
    # A fresh connection per call, deliberately not cached: a cached connection would go
    # stale across Postgres/container restarts (e.g. during chaos testing), and every
    # subsequent query would then raise until the whole Streamlit process restarted. At a
    # 5s poll interval, for a local dev dashboard, connection-per-query costs nothing.
    try:
        with psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
        ) as conn:
            return pd.read_sql(sql, conn)
    except psycopg2.OperationalError as e:
        st.error(f"Database unavailable — is Postgres running? (`make up`)\n\n{e}")
        return pd.DataFrame()


st.title("Streaming E-Commerce Analytics")
tab_live, tab_analytics = st.tabs(["Live", "Analytics"])

with tab_live:
    st.subheader("Live Order Metrics (speed layer)")
    st.caption(
        f"Auto-refreshes every 5s · last {LIVE_LOOKBACK_MINUTES} minutes of `live_order_metrics`"
    )

    @st.fragment(run_every="5s")
    def live_fragment():
        df = run_query(
            f"""
            SELECT window_start, window_end, region, sales_channel, order_count, revenue
            FROM live_order_metrics
            WHERE window_start >= NOW() - INTERVAL '{LIVE_LOOKBACK_MINUTES} minutes'
            ORDER BY window_start DESC
            """
        )
        if df.empty:
            st.info("No recent windows — is the producer/Spark job running? (`make produce`)")
            return

        c1, c2, c3 = st.columns(3)
        c1.metric("Orders", int(df["order_count"].sum()))
        c2.metric("Revenue", f"${df['revenue'].sum():,.2f}")
        c3.metric("Windows shown", df["window_start"].nunique())

        st.dataframe(df, use_container_width=True, hide_index=True)

        st.caption("Revenue by region over time")
        pivot = df.pivot_table(
            index="window_start", columns="region", values="revenue", aggfunc="sum", fill_value=0
        )
        st.line_chart(pivot)

        st.caption("Orders by sales channel")
        st.bar_chart(df.groupby("sales_channel")["order_count"].sum())

    live_fragment()

with tab_analytics:
    st.subheader("Gold Marts (batch layer)")
    if st.button("Refresh analytics"):
        st.rerun()

    def bar_from(df: pd.DataFrame, group_col: str, value_col: str, agg: str = "sum", top_n: int | None = None):
        series = df.groupby(group_col)[value_col].agg(agg).sort_values(ascending=False)
        return series.head(top_n) if top_n else series

    def mart_section(
        title: str,
        table: str,
        chart_group_col: str,
        chart_value_col: str,
        agg: str = "sum",
        top_n: int | None = None,
    ):
        st.markdown(f"#### {title}")
        df = run_query(f"SELECT * FROM marts.{table} ORDER BY event_date DESC")
        if df.empty:
            st.info("No data yet — has the Airflow DAG run? (`batch_quality_marts`, hourly)")
            return
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.bar_chart(bar_from(df, chart_group_col, chart_value_col, agg, top_n))

    mart_section("Regional Sales", "regional_sales", "region", "total_revenue")
    mart_section("Top Items", "top_items", "item_type", "total_revenue", top_n=10)
    mart_section("Fulfillment Time", "fulfillment_time", "region", "avg_fulfillment_days", agg="mean")
    mart_section("Profit Margin", "profit_margin", "item_type", "profit_margin_pct", agg="mean")
    mart_section("Channel Performance", "channel_performance", "sales_channel", "total_revenue")
