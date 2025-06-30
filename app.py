import streamlit as st
import pandas as pd
import streamlit.components.v1 as components
from backend.graph_visualizer import create_pyvis_graph
import requests
import json
from backend.graphrag_reasoner import explain_transaction_ids
import matplotlib.pyplot as plt
import altair as alt
import plotly.express as px
import os

st.set_page_config(page_title="Anomaly Detection Dashboard", page_icon="🕵️‍♂️", layout="wide")

# Sidebar for navigation
st.sidebar.title("🔍 Navigation")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["Transaction Analysis", "Anomaly Detection", "User Analytics", "System Statistics"]
)

# Load transaction IDs from CSV
@st.cache_data
def load_transaction_data():
    try:
        df = pd.read_csv("data/transactions_50k.csv")
        return df
    except Exception as e:
        st.error(f"❌ Could not load transactions: {e}")
        return None

# ---
# Helper functions for anomaly detection using SQL expressions (simulate what was in clickhouse_udfs.py)
from clickhouse_connect import get_client

client = get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
)

def get_anomalous_transactions():
    # Fetch a sample of recent transactions (e.g., last 10,000)
    try:
        query = """
        SELECT transaction_id, user_id, timestamp, amount, location, transaction_type, channel
        FROM transactions
        ORDER BY timestamp DESC
        LIMIT 10000
        """
        result = client.query(query)
        rows = result.result_rows
        if not rows:
            return []
        df = pd.DataFrame(rows, columns=[
            "transaction_id", "user_id", "timestamp", "amount", "location", "txn_type", "channel"
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Large transaction flag
        df['is_large_transaction'] = df['amount'] > 50000
        # Outlier flag (z-score per user)
        df['is_amount_outlier'] = False
        for uid, group in df.groupby('user_id'):
            mean = group['amount'].mean()
            std = group['amount'].std()
            idx = group.index
            df.loc[idx, 'is_amount_outlier'] = (group['amount'] > mean + 3 * std) | (group['amount'] < mean - 3 * std)
        # Frequency anomaly: more than 10 txns in 1 hour window
        df = df.sort_values(['user_id', 'timestamp'])
        df['txn_count_1h'] = 0
        for uid, group in df.groupby('user_id'):
            counts = []
            times = group['timestamp']
            for t in times:
                count = times[(times >= t - pd.Timedelta(hours=1)) & (times <= t)].count()
                counts.append(count)
            df.loc[group.index, 'txn_count_1h'] = counts
        df['is_frequency_anomaly'] = df['txn_count_1h'] > 10
        # Geographic anomaly: new location per user
        df['is_geographic_anomaly'] = False
        for uid, group in df.groupby('user_id'):
            known_locations = set()
            geo_flags = []
            for loc in group['location']:
                geo_flags.append(loc not in known_locations)
                known_locations.add(loc)
            df.loc[group.index, 'is_geographic_anomaly'] = geo_flags
        # Time anomaly: 2-5 AM
        df['is_time_anomaly'] = df['timestamp'].dt.hour.between(2, 5)
        # Channel anomaly: new channel per user
        df['is_channel_anomaly'] = False
        for uid, group in df.groupby('user_id'):
            known_channels = set()
            chan_flags = []
            for chan in group['channel']:
                chan_flags.append(chan not in known_channels)
                known_channels.add(chan)
            df.loc[group.index, 'is_channel_anomaly'] = chan_flags
        # Composite anomaly score
        df['anomaly_score'] = (
            df['is_large_transaction'] * 3 +
            df['is_amount_outlier'] * 2 +
            df['is_frequency_anomaly'] * 2 +
            df['is_geographic_anomaly'] * 1 +
            df['is_time_anomaly'] * 1 +
            df['is_channel_anomaly'] * 1
        )
        # Only return anomalous transactions
        anomalies = df[df['anomaly_score'] > 0].sort_values('anomaly_score', ascending=False).head(100)
        return anomalies[[
            "transaction_id", "user_id", "timestamp", "amount", "location", "txn_type", "channel",
            "is_large_transaction", "is_amount_outlier", "is_frequency_anomaly", "is_geographic_anomaly",
            "is_time_anomaly", "is_channel_anomaly", "anomaly_score"
        ]].values.tolist()
    except Exception as e:
        st.error(f"Error fetching anomalous transactions: {e}")
        return []

def get_user_anomaly_summary(user_id):
    # Fetch all transactions for the user
    try:
        query = f"""
        SELECT transaction_id, user_id, timestamp, amount, location, transaction_type, channel
        FROM transactions
        WHERE user_id = {user_id}
        ORDER BY timestamp
        """
        result = client.query(query)
        rows = result.result_rows
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=[
            "transaction_id", "user_id", "timestamp", "amount", "location", "txn_type", "channel"
        ])
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Large transaction flag
        df['is_large_transaction'] = df['amount'] > 50000
        # Outlier flag (z-score)
        mean = df['amount'].mean()
        std = df['amount'].std()
        df['is_amount_outlier'] = (df['amount'] > mean + 3 * std) | (df['amount'] < mean - 3 * std)
        # Frequency anomaly: more than 10 txns in 1 hour window
        df = df.sort_values('timestamp')
        txn_counts = []
        for i, row in df.iterrows():
            t = row['timestamp']
            count = df[(df['timestamp'] >= t - pd.Timedelta(hours=1)) & (df['timestamp'] <= t)].shape[0]
            txn_counts.append(count)
        df['txn_count_1h'] = txn_counts
        df['is_frequency_anomaly'] = df['txn_count_1h'] > 10
        # Geographic anomaly: new location
        known_locations = set()
        geo_flags = []
        for loc in df['location']:
            geo_flags.append(loc not in known_locations)
            known_locations.add(loc)
        df['is_geographic_anomaly'] = geo_flags
        # Time anomaly: 2-5 AM
        df['is_time_anomaly'] = df['timestamp'].dt.hour.between(2, 5)
        # Channel anomaly: new channel
        known_channels = set()
        chan_flags = []
        for chan in df['channel']:
            chan_flags.append(chan not in known_channels)
            known_channels.add(chan)
        df['is_channel_anomaly'] = chan_flags
        # Composite anomaly score
        df['anomaly_score'] = (
            df['is_large_transaction'] * 3 +
            df['is_amount_outlier'] * 2 +
            df['is_frequency_anomaly'] * 2 +
            df['is_geographic_anomaly'] * 1 +
            df['is_time_anomaly'] * 1 +
            df['is_channel_anomaly'] * 1
        )
        # Aggregate
        summary = [
            user_id,
            len(df),
            int(df['is_large_transaction'].sum()),
            int(df['is_amount_outlier'].sum()),
            int(df['is_frequency_anomaly'].sum()),
            int(df['is_geographic_anomaly'].sum()),
            int(df['is_time_anomaly'].sum()),
            int(df['is_channel_anomaly'].sum()),
            float(df['anomaly_score'].mean())
        ]
        return summary
    except Exception as e:
        st.error(f"Error fetching user anomaly summary: {e}")
        return None

def get_anomaly_statistics():
    # Use the same sample as get_anomalous_transactions
    try:
        query = """
        SELECT transaction_id, user_id, timestamp, amount, location, transaction_type, channel
        FROM transactions
        ORDER BY timestamp DESC
        LIMIT 10000
        """
        result = client.query(query)
        rows = result.result_rows
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=[
            "transaction_id", "user_id", "timestamp", "amount", "location", "txn_type", "channel"
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Reuse flag logic from get_anomalous_transactions
        df['is_large_transaction'] = df['amount'] > 50000
        df['is_amount_outlier'] = False
        for uid, group in df.groupby('user_id'):
            mean = group['amount'].mean()
            std = group['amount'].std()
            idx = group.index
            df.loc[idx, 'is_amount_outlier'] = (group['amount'] > mean + 3 * std) | (group['amount'] < mean - 3 * std)
        df = df.sort_values(['user_id', 'timestamp'])
        df['txn_count_1h'] = 0
        for uid, group in df.groupby('user_id'):
            counts = []
            times = group['timestamp']
            for t in times:
                count = times[(times >= t - pd.Timedelta(hours=1)) & (times <= t)].count()
                counts.append(count)
            df.loc[group.index, 'txn_count_1h'] = counts
        df['is_frequency_anomaly'] = df['txn_count_1h'] > 10
        df['is_geographic_anomaly'] = False
        for uid, group in df.groupby('user_id'):
            known_locations = set()
            geo_flags = []
            for loc in group['location']:
                geo_flags.append(loc not in known_locations)
                known_locations.add(loc)
            df.loc[group.index, 'is_geographic_anomaly'] = geo_flags
        df['is_time_anomaly'] = df['timestamp'].dt.hour.between(2, 5)
        df['is_channel_anomaly'] = False
        for uid, group in df.groupby('user_id'):
            known_channels = set()
            chan_flags = []
            for chan in group['channel']:
                chan_flags.append(chan not in known_channels)
                known_channels.add(chan)
            df.loc[group.index, 'is_channel_anomaly'] = chan_flags
        df['anomaly_score'] = (
            df['is_large_transaction'] * 3 +
            df['is_amount_outlier'] * 2 +
            df['is_frequency_anomaly'] * 2 +
            df['is_geographic_anomaly'] * 1 +
            df['is_time_anomaly'] * 1 +
            df['is_channel_anomaly'] * 1
        )
        stats = [
            len(df),
            int(df['is_large_transaction'].sum()),
            int(df['is_amount_outlier'].sum()),
            int(df['is_frequency_anomaly'].sum()),
            int(df['is_geographic_anomaly'].sum()),
            int(df['is_time_anomaly'].sum()),
            int(df['is_channel_anomaly'].sum()),
            float(df['anomaly_score'].mean())
        ]
        return stats
    except Exception as e:
        st.error(f"Error fetching anomaly statistics: {e}")
        return None

def get_top_anomalous_users(limit=10):
    # Use the same sample as get_anomalous_transactions
    try:
        query = """
        SELECT transaction_id, user_id, timestamp, amount, location, transaction_type, channel
        FROM transactions
        ORDER BY timestamp DESC
        LIMIT 10000
        """
        result = client.query(query)
        rows = result.result_rows
        if not rows:
            return []
        df = pd.DataFrame(rows, columns=[
            "transaction_id", "user_id", "timestamp", "amount", "location", "txn_type", "channel"
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Reuse flag logic from get_anomalous_transactions
        df['is_large_transaction'] = df['amount'] > 50000
        df['is_amount_outlier'] = False
        for uid, group in df.groupby('user_id'):
            mean = group['amount'].mean()
            std = group['amount'].std()
            idx = group.index
            df.loc[idx, 'is_amount_outlier'] = (group['amount'] > mean + 3 * std) | (group['amount'] < mean - 3 * std)
        df = df.sort_values(['user_id', 'timestamp'])
        df['txn_count_1h'] = 0
        for uid, group in df.groupby('user_id'):
            counts = []
            times = group['timestamp']
            for t in times:
                count = times[(times >= t - pd.Timedelta(hours=1)) & (times <= t)].count()
                counts.append(count)
            df.loc[group.index, 'txn_count_1h'] = counts
        df['is_frequency_anomaly'] = df['txn_count_1h'] > 10
        df['is_geographic_anomaly'] = False
        for uid, group in df.groupby('user_id'):
            known_locations = set()
            geo_flags = []
            for loc in group['location']:
                geo_flags.append(loc not in known_locations)
                known_locations.add(loc)
            df.loc[group.index, 'is_geographic_anomaly'] = geo_flags
        df['is_time_anomaly'] = df['timestamp'].dt.hour.between(2, 5)
        df['is_channel_anomaly'] = False
        for uid, group in df.groupby('user_id'):
            known_channels = set()
            chan_flags = []
            for chan in group['channel']:
                chan_flags.append(chan not in known_channels)
                known_channels.add(chan)
            df.loc[group.index, 'is_channel_anomaly'] = chan_flags
        df['anomaly_score'] = (
            df['is_large_transaction'] * 3 +
            df['is_amount_outlier'] * 2 +
            df['is_frequency_anomaly'] * 2 +
            df['is_geographic_anomaly'] * 1 +
            df['is_time_anomaly'] * 1 +
            df['is_channel_anomaly'] * 1
        )
        user_stats = df.groupby('user_id').agg(
            transaction_count=('transaction_id', 'count'),
            avg_anomaly_score=('anomaly_score', 'mean'),
            total_anomaly_score=('anomaly_score', 'sum')
        ).reset_index()
        user_stats = user_stats[user_stats['avg_anomaly_score'] > 0]
        user_stats = user_stats.sort_values('avg_anomaly_score', ascending=False).head(limit)
        return user_stats.values.tolist()
    except Exception as e:
        st.error(f"Error fetching top anomalous users: {e}")
        return []

# ---

# Main application logic
if page == "Transaction Analysis":
    st.title("🔍 Transaction Anomaly Explainer")
    st.markdown("Use the dropdown below to select a transaction ID and get an AI-generated explanation for why it may be anomalous.")

    df = load_transaction_data()
    if df is not None:
        txn_ids = df['transaction_id'].unique().tolist()
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Dropdown to pick a transaction
            txn_id = st.selectbox("Select a Transaction ID:", txn_ids)
            
            # Choose model
            model_choice = st.radio("Choose AI Model:", ["mistral (local)", "gemini-pro (cloud)"])
            
            if st.button("🧠 Explain Anomaly", type="primary"):
                with st.spinner("Analyzing transaction and generating insights..."):
                    explanation = explain_transaction_ids(txn_id, model_choice)
                    st.success("Explanation Ready!")
                    st.markdown("---")
                    st.markdown(f"**Transaction ID**: `{txn_id}`")
                    st.markdown("### 🤖 AI Explanation")
                    st.write(explanation)

        with col2:
            # Show transaction details
            if txn_id:
                transaction = df[df['transaction_id'] == txn_id].iloc[0]
                st.markdown("### 📋 Transaction Details")
                st.json({
                    "Transaction ID": transaction['transaction_id'],
                    "User ID": int(transaction['user_id']),
                    "Amount": f"₹{transaction['amount']:,.2f}",
                    "Timestamp": transaction['timestamp'],
                    "Location": transaction['location'],
                    "Type": transaction['txn_type'],
                    "Channel": transaction['channel']
                })

        # Graph display
        st.markdown("### 🕸️ Transaction Graph")
        graph = create_pyvis_graph(txn_id)
        if graph:
            graph.save_graph("graph.html")
            with open("graph.html", "r", encoding="utf-8") as f:
                html = f.read()
            components.html(html, height=450)
        else:
            st.warning(f"No graph data available for transaction `{txn_id}`.")

elif page == "Anomaly Detection":
    st.title("🚨 Anomaly Detection Dashboard")
    st.markdown("Real-time anomaly detection using ClickHouse SQL expressions and AI analysis.")
    
    # Get anomaly statistics
    stats = get_anomaly_statistics()
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transactions", f"{stats[0]:,}")
        with col2:
            st.metric("Anomalous Transactions", f"{stats[1] + stats[2] + stats[3] + stats[4] + stats[5] + stats[6]:,}")
        with col3:
            st.metric("Avg Anomaly Score", f"{stats[7]:.2f}")
        with col4:
            st.metric("Max Anomaly Score", "-")
    
    # Anomaly breakdown
    if stats:
        st.markdown("### 📊 Anomaly Breakdown")
        col1, col2 = st.columns(2)
        
        with col1:
            anomaly_data = {
                "Large Transactions": stats[1],
                "Amount Outliers": stats[2],
                "Frequency Anomalies": stats[3],
                "Geographic Anomalies": stats[4],
                "Time Anomalies": stats[5],
                "Channel Anomalies": stats[6]
            }
            st.bar_chart(anomaly_data)
        
        with col2:
            st.markdown("#### Anomaly Types:")
            for anomaly_type, count in anomaly_data.items():
                st.write(f"• **{anomaly_type}**: {count:,}")
    
    # Top anomalous users
    st.markdown("### 👥 Top Anomalous Users")
    top_users = get_top_anomalous_users(10)
    if top_users:
        user_data = pd.DataFrame(top_users, columns=[
            "User ID", "Transaction Count", "Avg Anomaly Score", "Total Anomaly Score"
        ])
        st.dataframe(user_data, use_container_width=True)
    
    # Anomalous transactions table
    st.markdown("### 🚨 Recent Anomalous Transactions")
    anomalies = get_anomalous_transactions()
    if anomalies:
        anomaly_df = pd.DataFrame(anomalies, columns=[
            "Transaction ID", "User ID", "Timestamp", "Amount", "Location", 
            "Type", "Channel", "Large Transaction", 
            "Amount Outlier", "Frequency Anomaly", "Geographic Anomaly", 
            "Time Anomaly", "Channel Anomaly", "Anomaly Score"
        ])
        st.dataframe(anomaly_df, use_container_width=True)
    else:
        st.info("No anomalous transactions found.")

elif page == "User Analytics":
    st.title("👤 User Analytics")
    st.markdown("Detailed analysis of user behavior and anomaly patterns.")
    
    df = load_transaction_data()
    if df is not None:
        # User selection
        user_ids = sorted(df['user_id'].unique())
        selected_user = st.selectbox("Select User ID:", user_ids)
        
        if selected_user:
            # Get user anomaly summary
            user_summary = get_user_anomaly_summary(selected_user)
            
            if user_summary:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Transactions", user_summary[1])
                with col2:
                    st.metric("Large Transactions", user_summary[2])
                with col3:
                    st.metric("Avg Anomaly Score", f"{user_summary[7]:.2f}")
                with col4:
                    anomaly_count = sum(user_summary[2:7])
                    st.metric("Total Anomalies", anomaly_count)
                
                # User transaction history
                user_transactions = df[df['user_id'] == selected_user]
                st.markdown("### 📈 Transaction History")
                
                # Amount over time
                user_transactions['timestamp'] = pd.to_datetime(user_transactions['timestamp'], format='%d-%m-%Y %H:%M')
                user_transactions = user_transactions.sort_values('timestamp')
                
                st.line_chart(user_transactions.set_index('timestamp')['amount'])
                
                # Transaction details
                st.markdown("### 📋 Recent Transactions")
                st.dataframe(user_transactions.tail(10), use_container_width=True)

elif page == "System Statistics":
    st.title("📊 System Statistics")
    st.markdown("Overall system performance and data insights.")
    
    df = load_transaction_data()
    if df is not None:
        # Data overview
        st.markdown("### 📈 Data Overview")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", f"{len(df):,}")
        with col2:
            st.metric("Unique Users", f"{df['user_id'].nunique():,}")
        with col3:
            st.metric("Total Amount", f"₹{df['amount'].sum():,.2f}")
        with col4:
            st.metric("Avg Transaction", f"₹{df['amount'].mean():,.2f}")
        
        # Location analysis
        st.markdown("### 🌍 Location Analysis")
        location_counts = df['location'].value_counts()
        st.bar_chart(location_counts.head(10))
        
        # Channel analysis
        st.markdown("### 📱 Channel Analysis")
        channel_counts = df['channel'].value_counts()
        st.bar_chart(channel_counts)
        
        # Transaction type analysis
        st.markdown("### 💳 Transaction Type Analysis")
        type_counts = df['txn_type'].value_counts()
        st.bar_chart(type_counts)
        
        # Amount distribution (Plotly histogram)
        st.markdown("### 💰 Amount Distribution")
        fig = px.histogram(df, x="amount", nbins=30, title="Transaction Amount Distribution")
        fig.update_layout(bargap=0.1, xaxis_title="Amount", yaxis_title="Frequency")
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.caption("Powered by GraphRAG + ClickHouse SQL Expressions + LLM magic")

