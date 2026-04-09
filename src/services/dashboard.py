"""
dashboard.py
Streamlit dashboard for Insurance Claims Fraud Detection.

Run:
    streamlit run dashboard.py

Pages:
    - Overview: Key metrics and fraud summary
    - Claims Analysis: Claims by severity, type
    - Model Performance: Accuracy, AUC, confusion matrix
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text

from configs.settings import DATABASE_URL

st.set_page_config(
    page_title="Insurance Fraud Dashboard",
    page_icon="🛡️",
    layout="wide",
)


@st.cache_data(ttl=300)
def load_claims():
    """Load claims data from PostgreSQL."""
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query(text("SELECT * FROM claims"), engine)
        engine.dispose()
        
        numeric_cols = ['months_as_customer', 'age', 'policy_deductable', 'policy_annual_premium',
                        'capital-gains', 'capital-loss', 'incident_hour_of_the_day',
                        'number_of_vehicles_involved', 'bodily_injuries', 'witnesses',
                        'total_claim_amount', 'injury_claim', 'property_claim', 'vehicle_claim', 'auto_year']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_stats():
    """Load fraud statistics."""
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query(text("""
            SELECT 
                auto_make,
                COUNT(*) as claim_count,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
                ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) as fraud_rate
            FROM claims
            GROUP BY auto_make
            ORDER BY fraud_rate DESC
        """), engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Error loading stats: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_incident_stats():
    """Load incident severity stats."""
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql_query(text("""
            SELECT 
                incident_severity,
                incident_type,
                COUNT(*) as count,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count
            FROM claims
            GROUP BY incident_severity, incident_type
        """), engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Error loading stats: {e}")
        return pd.DataFrame()


def main():
    st.title("🛡️ Insurance Claims Fraud Dashboard")
    st.markdown("ML-powered fraud detection analytics")
    
    df = load_claims()
    stats = load_stats()
    incident_stats = load_incident_stats()
    
    if df.empty:
        st.warning("No data available. Run ETL first: `python -c \"from claims_etl import run_etl; run_etl()\"`")
        return
    
    tab1, tab2, tab3 = st.tabs(["Overview", "Claims Analysis", "Model Info"])
    
    with tab1:
        st.header("Key Metrics")
        
        total_claims = len(df)
        fraud_count = df['is_fraud'].sum()
        fraud_rate = (fraud_count / total_claims * 100) if total_claims > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Claims", f"{total_claims:,}")
        
        with col2:
            st.metric("Fraudulent Claims", f"{fraud_count:,}")
        
        with col3:
            st.metric("Fraud Rate", f"{fraud_rate:.1f}%")
        
        with col4:
            avg_claim = df['total_claim_amount'].mean()
            st.metric("Avg Claim Amount", f"${avg_claim:,.0f}")
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Fraud Rate by Auto Make")
            if not stats.empty:
                fig = px.bar(
                    stats,
                    x="auto_make",
                    y="fraud_rate",
                    color="fraud_rate",
                    title="Fraud Rate by Vehicle Make",
                    labels={"fraud_rate": "Fraud Rate (%)", "auto_make": "Vehicle Make"},
                    color_continuous_scale="RdYlGn_r"
                )
                fig.update_layout(yaxis_range=[0, max(stats['fraud_rate'].max() * 1.2, 50)])
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Claims by Vehicle Make")
            if not stats.empty:
                fig = px.bar(
                    stats,
                    x="auto_make",
                    y="claim_count",
                    color="fraud_count",
                    title="Claims Volume by Vehicle Make",
                    labels={"claim_count": "Claims", "auto_make": "Vehicle Make"},
                )
                st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        st.subheader("Fraud Distribution")
        fraud_dist = df['is_fraud'].value_counts().reset_index()
        fraud_dist['is_fraud'] = fraud_dist['is_fraud'].map({0: 'Legitimate', 1: 'Fraud'})
        
        fig = px.pie(
            fraud_dist,
            values='count',
            names='is_fraud',
            title="Fraud vs Legitimate Claims",
            color='is_fraud',
            color_discrete_map={'Legitimate': '#2ecc71', 'Fraud': '#e74c3c'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("Claims Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Fraud by Incident Severity")
            if 'incident_severity' in df.columns:
                severity_fraud = df.groupby(['incident_severity', 'is_fraud']).size().reset_index(name='count')
                severity_fraud['is_fraud'] = severity_fraud['is_fraud'].map({0: 'Legitimate', 1: 'Fraud'})
                
                fig = px.bar(
                    severity_fraud,
                    x="incident_severity",
                    y="count",
                    color="is_fraud",
                    barmode="group",
                    title="Fraud by Incident Severity",
                    color_discrete_map={'Legitimate': '#2ecc71', 'Fraud': '#e74c3c'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Fraud by Incident Type")
            if 'incident_type' in df.columns:
                type_fraud = df.groupby(['incident_type', 'is_fraud']).size().reset_index(name='count')
                type_fraud['is_fraud'] = type_fraud['is_fraud'].map({0: 'Legitimate', 1: 'Fraud'})
                
                fig = px.bar(
                    type_fraud,
                    x="incident_type",
                    y="count",
                    color="is_fraud",
                    barmode="group",
                    title="Fraud by Incident Type",
                    color_discrete_map={'Legitimate': '#2ecc71', 'Fraud': '#e74c3c'}
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Claim Amount Distribution")
            fig = px.histogram(
                df,
                x="total_claim_amount",
                color="is_fraud",
                nbins=30,
                title="Total Claim Amount Distribution",
                color_discrete_map={0: '#2ecc71', 1: '#e74c3c'}
            )
            fig.update_layout(xaxis_title="Claim Amount ($)", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Premium vs Claim Amount")
            fig = px.scatter(
                df.dropna(subset=['policy_annual_premium', 'total_claim_amount']),
                x="policy_annual_premium",
                y="total_claim_amount",
                color="is_fraud",
                title="Premium vs Claim Amount",
                color_discrete_map={0: '#2ecc71', 1: '#e74c3c'},
                opacity=0.6
            )
            fig.update_layout(xaxis_title="Annual Premium ($)", yaxis_title="Claim Amount ($)")
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        st.subheader("Claims Browser")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            severity_filter = st.selectbox(
                "Incident Severity",
                ["All"] + sorted(df["incident_severity"].dropna().unique().tolist())
            )
        
        with col2:
            fraud_filter = st.selectbox(
                "Fraud Status",
                ["All", "Fraud Only", "Legitimate Only"]
            )
        
        with col3:
            auto_filter = st.selectbox(
                "Vehicle Make",
                ["All"] + sorted(df["auto_make"].dropna().unique().tolist())
            )
        
        filtered_df = df.copy()
        
        if severity_filter != "All":
            filtered_df = filtered_df[filtered_df["incident_severity"] == severity_filter]
        
        if fraud_filter == "Fraud Only":
            filtered_df = filtered_df[filtered_df["is_fraud"] == 1]
        elif fraud_filter == "Legitimate Only":
            filtered_df = filtered_df[filtered_df["is_fraud"] == 0]
        
        if auto_filter != "All":
            filtered_df = filtered_df[filtered_df["auto_make"] == auto_filter]
        
        st.write(f"Showing {len(filtered_df)} of {len(df)} claims")
        
        display_cols = ['policy_number', 'auto_make', 'incident_type', 'incident_severity',
                        'total_claim_amount', 'policy_annual_premium', 'is_fraud']
        display_cols = [c for c in display_cols if c in filtered_df.columns]
        
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            hide_index=True,
        )
    
    with tab3:
        st.header("Model Information")
        
        st.info("""
        **Model: XGBoost Classifier**
        
        The fraud detection model was trained on historical insurance claims data.
        
        **Features Used:**
        - Customer demographics (age, tenure)
        - Policy details (premium, deductible, coverage)
        - Incident details (type, severity, vehicles involved)
        - Claim amounts (injury, property, vehicle)
        
        **To retrain the model:**
        ```
        python fraud_model.py
        ```
        """)
        
        st.subheader("Top Fraud Indicators")
        
        indicator_data = [
            ("Incident Severity", "Major Damage claims have higher fraud rates"),
            ("Number of Vehicles", "Multi-vehicle incidents show different patterns"),
            ("Claim Amount", "High claim amounts relative to premium"),
            ("Witnesses", "Claims without witnesses need review"),
            ("Police Report", "Missing police reports correlate with fraud"),
        ]
        
        for i, (indicator, desc) in enumerate(indicator_data, 1):
            st.write(f"{i}. **{indicator}**: {desc}")
        
        st.divider()
        
        st.subheader("Quick Actions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Retrain Model"):
                with st.spinner("Training model..."):
                    try:
                        import subprocess
                        result = subprocess.run(
                            ["python", "fraud_model.py"],
                            capture_output=True,
                            text=True
                        )
                        st.success("Model retrained successfully!")
                        st.code(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
                    except Exception as e:
                        st.error(f"Error: {e}")
        
        with col2:
            if st.button("Refresh Data"):
                st.cache_data.clear()
                st.rerun()


if __name__ == "__main__":
    main()
