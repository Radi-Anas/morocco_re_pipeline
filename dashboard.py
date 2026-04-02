"""
dashboard.py
Streamlit dashboard for Moroccan Real Estate data visualization.

Run:
    streamlit run dashboard.py

Pages:
    - Overview: Key metrics and summary
    - Price Analysis: Price distributions and trends
    - Map View: Listings by city
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os

from config.settings import DATABASE_URL

st.set_page_config(
    page_title="Morocco RE Dashboard",
    page_icon="🏠",
    layout="wide",
)


@st.cache_data(ttl=300)
def load_data():
    """Load data from PostgreSQL with caching."""
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query(text("SELECT * FROM listings"), engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_stats():
    """Load statistics from database."""
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query(text("""
            SELECT 
                city,
                COUNT(*) as listing_count,
                ROUND(AVG(price), 0) as avg_price,
                ROUND(MIN(price), 0) as min_price,
                ROUND(MAX(price), 0) as max_price,
                ROUND(AVG(price_per_m2), 0) as avg_price_per_m2
            FROM listings
            GROUP BY city
            ORDER BY avg_price DESC
        """), engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Error loading stats: {e}")
        return pd.DataFrame()


def main():
    st.title("🏠 Morocco Real Estate Dashboard")
    st.markdown("Data from Avito.ma listings")
    
    df = load_data()
    stats = load_stats()
    
    if df.empty:
        st.warning("No data available. Run the pipeline first: `python prefect_flow.py`")
        return
    
    tab1, tab2, tab3 = st.tabs(["Overview", "Price Analysis", "Listings"])
    
    with tab1:
        st.header("Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Listings", len(df))
        
        with col2:
            avg_price = df["price"].mean()
            st.metric("Avg Price", f"{avg_price:,.0f} DH")
        
        with col3:
            cities = df["city"].nunique()
            st.metric("Cities", cities)
        
        with col4:
            avg_m2 = df["price_per_m2"].mean()
            st.metric("Avg Price/m²", f"{avg_m2:,.0f} DH")
        
        st.divider()
        
        st.subheader("Price by City")
        fig = px.bar(
            stats,
            x="city",
            y="avg_price",
            color="listing_count",
            title="Average Price by City",
            labels={"avg_price": "Average Price (DH)", "city": "City"},
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Listings by City")
            city_counts = df["city"].value_counts()
            fig = px.pie(
                values=city_counts.values,
                names=city_counts.index,
                title="Distribution of Listings",
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Price Ranges")
            price_ranges = df["price_range"].value_counts()
            fig = px.bar(
                x=price_ranges.index,
                y=price_ranges.values,
                color=price_ranges.index,
                title="Listings by Price Range",
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("Price Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Price Distribution")
            fig = px.histogram(
                df,
                x="price",
                nbins=50,
                title="Price Distribution (All Listings)",
            )
            fig.update_layout(xaxis_title="Price (DH)", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Price per m² Distribution")
            if "price_per_m2" in df.columns:
                fig = px.histogram(
                    df,
                    x="price_per_m2",
                    nbins=50,
                    title="Price per m² Distribution",
                )
                fig.update_layout(xaxis_title="Price/m² (DH)", yaxis_title="Count")
                st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        st.subheader("Price vs Surface Area")
        if "surface_m2" in df.columns:
            fig = px.scatter(
                df,
                x="surface_m2",
                y="price",
                color="city",
                size="price_per_m2" if "price_per_m2" in df.columns else None,
                hover_name="title",
                title="Price vs Surface Area by City",
            )
            fig.update_layout(xaxis_title="Surface (m²)", yaxis_title="Price (DH)")
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        st.subheader("Statistics by City")
        st.dataframe(
            stats,
            use_container_width=True,
            hide_index=True,
        )
    
    with tab3:
        st.header("Listings Browser")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            city_filter = st.selectbox("City", ["All"] + sorted(df["city"].unique().tolist()))
        
        with col2:
            price_range_filter = st.selectbox(
                "Price Range",
                ["All"] + sorted(df["price_range"].dropna().unique().tolist())
            )
        
        with col3:
            sort_by = st.selectbox("Sort By", ["Price (High to Low)", "Price (Low to High)", "Surface"])
        
        filtered_df = df.copy()
        
        if city_filter != "All":
            filtered_df = filtered_df[filtered_df["city"] == city_filter]
        
        if price_range_filter != "All":
            filtered_df = filtered_df[filtered_df["price_range"] == price_range_filter]
        
        if sort_by == "Price (High to Low)":
            filtered_df = filtered_df.sort_values("price", ascending=False)
        elif sort_by == "Price (Low to High)":
            filtered_df = filtered_df.sort_values("price", ascending=True)
        else:
            filtered_df = filtered_df.sort_values("surface_m2", ascending=False)
        
        st.write(f"Showing {len(filtered_df)} of {len(df)} listings")
        
        display_cols = ["title", "city", "price", "surface_m2", "price_per_m2", "price_range"]
        display_cols = [c for c in display_cols if c in filtered_df.columns]
        
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
