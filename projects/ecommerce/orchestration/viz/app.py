import json
import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="E-Commerce Real-Time Intelligence", layout="wide")
st.title("🛒 E-Commerce Lambda Architecture Intelligence")

DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH", "projects/ecommerce/batch_layer/data/ecommerce.duckdb"
)
OUTPUT_PREFIX = os.environ.get(
    "CLICK_STREAM_OUTPUT_PREFIX", "projects/ecommerce/output_data"
)

if not os.path.exists(DUCKDB_PATH):
    st.warning(
        f"DuckDB database not found at `{DUCKDB_PATH}`. Please run streaming and batch pipelines first."
    )
    st.stop()


@st.cache_data(ttl=15)
def run_query(query: str) -> pd.DataFrame:
    """Executes analytical queries against DuckDB with short caching."""
    with duckdb.connect(DUCKDB_PATH, read_only=True) as conn:
        return conn.execute(query).fetchdf()


def _load_json_report(path: str) -> dict | None:
    """Loads a JSON quality report if present on disk."""
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Metrics Summary Row
col1, col2, col3, col4 = st.columns(4)

try:
    df_metrics = run_query("""
        SELECT
          (SELECT coalesce(sum(daily_revenue), 0) FROM daily_category_sales) AS total_rev,
          (SELECT coalesce(sum(daily_units), 0) FROM daily_category_sales) AS total_units,
          (SELECT coalesce(max(cumulative_users), 0) FROM cumulative_users) AS active_users,
          (SELECT coalesce(avg(avg_conversion_rate), 0) * 100 FROM daily_url_conversion) AS avg_conv
    """)

    total_rev = df_metrics["total_rev"].iloc[0]
    total_units = df_metrics["total_units"].iloc[0]
    active_users = df_metrics["active_users"].iloc[0]
    avg_conv = df_metrics["avg_conv"].iloc[0]

    col1.metric("Total Revenue", f"${total_rev:,.2f}")
    col2.metric("Units Sold", f"{total_units:,}")
    col3.metric("Cumulative Users", f"{active_users:,}")
    col4.metric("Avg Conversion Rate", f"{avg_conv:.2f}%")
except Exception as e:
    st.error(f"Error loading summary metrics: {e}")

st.divider()

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "📊 Category Sales",
        "🎯 URL Conversion & Funnels",
        "👥 User Growth",
        "🏢 Customer & Brand Intelligence",
        "🛡️ Data Quality & Pipeline Health",
    ]
)

with tab1:
    try:
        df_cat = run_query(
            "SELECT sales_date, category, daily_revenue, daily_units FROM daily_category_sales ORDER BY sales_date"
        )
        if not df_cat.empty:
            fig1 = px.bar(
                df_cat,
                x="category",
                y="daily_revenue",
                color="category",
                title="Revenue by Product Category",
            )
            st.plotly_chart(fig1, use_container_width=True)
            st.dataframe(df_cat, use_container_width=True)
        else:
            st.info("No category sales records found yet.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab2:
    try:
        df_conv = run_query(
            "SELECT url, total_views, total_purchases, avg_conversion_rate FROM daily_url_conversion ORDER BY total_purchases DESC"
        )
        if not df_conv.empty:
            fig2 = px.bar(
                df_conv,
                x="url",
                y="avg_conversion_rate",
                title="Conversion Rate by Page URL",
            )
            st.plotly_chart(fig2, use_container_width=True)
            st.dataframe(df_conv, use_container_width=True)
        else:
            st.info("No URL conversion records found yet.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab3:
    try:
        df_users = run_query(
            "SELECT cohort_date, new_users, cumulative_users FROM cumulative_users ORDER BY cohort_date"
        )
        if not df_users.empty:
            fig3 = px.line(
                df_users,
                x="cohort_date",
                y="cumulative_users",
                markers=True,
                title="Cumulative User Growth",
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No user cohort records found yet.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab4:
    c_left, c_right = st.columns(2)
    with c_left:
        st.subheader("Revenue by Customer Segment & Region")
        try:
            df_seg = run_query(
                "SELECT user_id, segment, region, total_revenue, total_units FROM customer_segment_spending ORDER BY total_revenue DESC"
            )
            if not df_seg.empty:
                fig_seg = px.bar(
                    df_seg,
                    x="segment",
                    y="total_revenue",
                    color="region",
                    barmode="group",
                    title="OLTP-Enriched Segment Spending",
                )
                st.plotly_chart(fig_seg, use_container_width=True)
                st.dataframe(df_seg, use_container_width=True)
            else:
                st.info("No segment spending records found yet.")
        except Exception as e:
            st.info(f"Segment spending mart not available yet: {e}")

    with c_right:
        st.subheader("Revenue by Product Brand")
        try:
            df_brand = run_query(
                "SELECT brand, category, total_revenue, total_units, avg_unit_price FROM brand_category_performance ORDER BY total_revenue DESC"
            )
            if not df_brand.empty:
                fig_brand = px.bar(
                    df_brand,
                    x="brand",
                    y="total_revenue",
                    color="category",
                    title="OLTP-Enriched Brand Performance",
                )
                st.plotly_chart(fig_brand, use_container_width=True)
                st.dataframe(df_brand, use_container_width=True)
            else:
                st.info("No brand performance records found yet.")
        except Exception as e:
            st.info(f"Brand performance mart not available yet: {e}")

with tab5:
    st.subheader("Pipeline Freshness Monitor")
    try:
        df_fresh = run_query(
            "SELECT latest_stream_window, batch_processed_at, cast(pipeline_latency as varchar) as pipeline_latency FROM last_ingest"
        )
        st.dataframe(df_fresh, use_container_width=True)
    except Exception as e:
        st.info(f"Freshness monitor not available yet: {e}")

    q_left, q_right = st.columns(2)
    with q_left:
        st.subheader("Great Expectations (Batch & Gold Marts)")
        gx_path = os.path.join(
            OUTPUT_PREFIX, "quality_reports/gx_validation_results.json"
        )
        gx_report = _load_json_report(gx_path)
        if gx_report:
            status_label = "PASSED" if gx_report.get("overall_success") else "FAILED"
            st.metric("GX Overall Status", status_label)
            for suite in gx_report.get("suites", []):
                st.write(
                    f"**{suite['suite_name']}**: {suite['passed_count']}/{suite['total_count']} checks passed"
                )
                st.dataframe(pd.DataFrame(suite["checks"]), use_container_width=True)
        else:
            st.info("No Great Expectations report generated yet.")

    with q_right:
        st.subheader("PyDeequ (PySpark Stream Verification)")
        deequ_path = os.path.join(OUTPUT_PREFIX, "pydeequ_metrics/latest_report.json")
        deequ_report = _load_json_report(deequ_path)
        if deequ_report:
            st.metric(
                "PyDeequ Overall Status",
                deequ_report.get("overall_status", "Unknown"),
                delta=f"{deequ_report.get('row_count', 0)} rows checked",
            )
            checks = deequ_report.get("checks", [])
            if checks:
                st.dataframe(pd.DataFrame(checks), use_container_width=True)
        else:
            st.info("No PyDeequ metrics report generated yet.")
