import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
import plotly.express as px
from datetime import date

# Initialize database
def init_db():
    if not Path("contracts.db").exists():
        conn = sqlite3.connect('contracts.db')
        # Read from parquet instead of CSV
        df = pd.read_parquet('contracts_sample.parquet')
        df.to_sql('contracts', conn, if_exists='replace', index=False)
        conn.close()

init_db()

# Page config
st.set_page_config(page_title="Government Contracts Explorer", layout="wide")

# Create sidebar filters
st.sidebar.header("Filters")

# Date range filter for contract end dates
st.sidebar.subheader("Contract End Date Range")
default_start = date(2025, 2, 1)  # February 1st, 2025
end_date_min = st.sidebar.date_input(
    "From End Date",
    value=default_start,
    min_value=default_start,
    help="Filter contracts ending after this date"
)
end_date_max = st.sidebar.date_input(
    "To End Date",
    value=date(2025, 12, 31),
    min_value=default_start,
    help="Filter contracts ending before this date"
)

# Text search filters
st.sidebar.subheader("Search Filters")
recipient_search = st.sidebar.text_input("Search Recipient Name")
agency_search = st.sidebar.text_input("Search Awarding Agency")
description_search = st.sidebar.text_input("Search Contract Description")

# Value range filter
st.sidebar.subheader("Contract Value Range")
value_range = st.sidebar.slider(
    "Current Total Value ($)",
    min_value=0,
    max_value=10000000,
    value=(0, 10000000)
)

# Build query based on filters
query = "SELECT * FROM contracts WHERE period_of_performance_current_end_date >= ?"
params = [default_start.strftime('%Y-%m-%d')]

if end_date_max:
    query += " AND period_of_performance_current_end_date <= ?"
    params.append(end_date_max.strftime('%Y-%m-%d'))
    
if recipient_search:
    query += " AND recipient_name LIKE ?"
    params.append(f"%{recipient_search}%")
    
if agency_search:
    query += " AND awarding_agency_name LIKE ?"
    params.append(f"%{agency_search}%")

if description_search:
    query += " AND (transaction_description LIKE ? OR prime_award_base_transaction_description LIKE ?)"
    params.extend([f"%{description_search}%", f"%{description_search}%"])

query += " AND current_total_value_of_award BETWEEN ? AND ?"
params.extend([value_range[0], value_range[1]])

# Display results
conn = sqlite3.connect('contracts.db')
try:
    df = pd.read_sql(query, conn, params=params)
    
    st.title("Government Contracts Explorer")
    st.caption(f"Showing contracts ending between {end_date_min.strftime('%B %d, %Y')} and {end_date_max.strftime('%B %d, %Y')}")
    
    # Show summary metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Contracts", len(df))
    col2.metric("Total Current Value", f"${df['current_total_value_of_award'].sum():,.2f}")
    col3.metric("Total Potential Value", f"${df['potential_total_value_of_award'].sum():,.2f}")
    
    # Add visualizations
    st.subheader("Contract Value Distribution")
    fig = px.histogram(df, x="current_total_value_of_award", 
                      nbins=50, 
                      title="Distribution of Contract Values",
                      labels={"current_total_value_of_award": "Current Total Value ($)"})
    st.plotly_chart(fig, use_container_width=True)
    
    # Top contractors
    st.subheader("Top Recipients by Total Value")
    top_contractors = df.groupby("recipient_name")["current_total_value_of_award"].sum().sort_values(ascending=False).head(10)
    fig2 = px.bar(top_contractors, 
                  title="Top 10 Recipients by Total Contract Value",
                  labels={"value": "Current Total Value ($)", "recipient_name": "Recipient Name"})
    st.plotly_chart(fig2, use_container_width=True)
    
    # Show data table with key columns
    st.subheader("Contracts Data")
    display_columns = [
        "contract_transaction_unique_key",
        "recipient_name",
        "awarding_agency_name",
        "current_total_value_of_award",
        "potential_total_value_of_award",
        "action_date",
        "period_of_performance_current_end_date",
        "transaction_description",
        "prime_award_base_transaction_description"
    ]
    
    # Convert date columns to datetime for better display
    if len(df) > 0:
        df['action_date'] = pd.to_datetime(df['action_date'])
        df['period_of_performance_current_end_date'] = pd.to_datetime(df['period_of_performance_current_end_date'])
        
        # Format currency columns
        df['current_total_value_of_award'] = df['current_total_value_of_award'].apply(lambda x: f"${x:,.2f}")
        df['potential_total_value_of_award'] = df['potential_total_value_of_award'].apply(lambda x: f"${x:,.2f}")
    
    st.dataframe(df[display_columns], use_container_width=True)
    
except sqlite3.Error as e:
    st.error(f"Database error: {e}")
finally:
    conn.close() 