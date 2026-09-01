import os
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="E-Commerce Real-Time Intelligence", layout="wide")
st.title("🛒 E-Commerce Lambda Architecture Intelligence")

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "projects/realtime/ecommerce/batch_layer/data/ecommerce.duckdb")

if not os.path.exists(DUCKDB_PATH):
    st.warning(f"DuckDB database not found at `{DUCKDB_PATH}`. Please run streaming and batch pipelines first.")
    st.stop()

@st.cache_data(ttl=15)
def run_query(query: str) -> pd.DataFrame:
    """Executes analytical queries against DuckDB with short caching."""
    with duckdb.connect(DUCKDB_PATH, read_only=True) as conn:
        return conn.execute(query).fetchdf()


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

tab1, tab2, tab3 = st.tabs(["📊 Category Sales", "🎯 URL Conversion & Funnels", "👥 User Growth"])

with tab1:
    try:
        df_cat = run_query("SELECT sales_date, category, daily_revenue, daily_units FROM daily_category_sales ORDER BY sales_date")
        if not df_cat.empty:
            fig1 = px.bar(df_cat, x="category", y="daily_revenue", color="category", title="Revenue by Product Category")
            st.plotly_chart(fig1, use_container_width=True)
            st.dataframe(df_cat, use_container_width=True)
        else:
            st.info("No category sales records found yet.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab2:
    try:
        df_conv = run_query("SELECT url, total_views, total_purchases, avg_conversion_rate FROM daily_url_conversion ORDER BY total_purchases DESC")
        if not df_conv.empty:
            fig2 = px.bar(df_conv, x="url", y="avg_conversion_rate", title="Conversion Rate by Page URL")
            st.plotly_chart(fig2, use_container_width=True)
            st.dataframe(df_conv, use_container_width=True)
        else:
            st.info("No URL conversion records found yet.")
    except Exception as e:
        st.error(f"Error: {e}")

with tab3:
    try:
        df_users = run_query("SELECT cohort_date, new_users, cumulative_users FROM cumulative_users ORDER BY cohort_date")
        if not df_users.empty:
            fig3 = px.line(df_users, x="cohort_date", y="cumulative_users", markers=True, title="Cumulative User Growth")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No user cohort records found yet.")
    except Exception as e:
        st.error(f"Error: {e}")
