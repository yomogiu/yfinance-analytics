import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime, timedelta

# Configure the page
st.set_page_config(
    page_title="SPY Analysis Dashboard",
    page_icon="📈",
    layout="wide"
)

# API configuration
API_URL = "http://localhost:8001"

def fetch_latest_metrics():
    response = requests.get(f"{API_URL}/metrics/latest")
    return response.json()

def fetch_historical_data(days=252):
    response = requests.get(f"{API_URL}/data/historical", params={"days": days})
    return response.json()

def fetch_validation_report():
    response = requests.get(f"{API_URL}/analysis/validation")
    return response.json()

# Dashboard layout
st.title("SPY Market Analysis Dashboard")

# Sidebar for controls
st.sidebar.header("Controls")
time_period = st.sidebar.selectbox(
    "Select Time Period",
    ["1M", "3M", "6M", "1Y", "2Y", "5Y"],
    index=3
)

# Convert time period to days
period_days = {
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "1Y": 252,
    "2Y": 504,
    "5Y": 1260
}[time_period]

# Fetch data
try:
    metrics = fetch_latest_metrics()
    historical = fetch_historical_data(period_days)
    validation = fetch_validation_report()

    # Current metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("SPY Price", f"${metrics['last_price']:.2f}", 
                 f"{metrics['daily_return']*100:.2f}%")
    with col2:
        st.metric("RSI", f"{metrics['current_rsi']:.2f}")
    with col3:
        st.metric("Volatility", f"{metrics['volatility']*100:.2f}%")
    with col4:
        st.metric("Market Regime", metrics['market_regime'])

    # Create main price chart
    fig = make_subplots(rows=3, cols=1, 
                       shared_xaxes=True,
                       vertical_spacing=0.05,
                       row_heights=[0.5, 0.25, 0.25])

    # Price and MA chart
    fig.add_trace(
        go.Scatter(x=historical['dates'], y=historical['close'],
                  name="SPY", line=dict(color='black')),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=historical['dates'], y=historical['sma_50'],
                  name="50 MA", line=dict(color='blue')),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=historical['dates'], y=historical['sma_200'],
                  name="200 MA", line=dict(color='red')),
        row=1, col=1
    )

    # RSI chart
    fig.add_trace(
        go.Scatter(x=historical['dates'], y=historical['rsi'],
                  name="RSI", line=dict(color='purple')),
        row=2, col=1
    )
    
    # Add RSI levels
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

    # MACD chart
    fig.add_trace(
        go.Scatter(x=historical['dates'], y=historical['macd'],
                  name="MACD", line=dict(color='blue')),
        row=3, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=historical['dates'], y=historical['signal_line'],
                  name="Signal", line=dict(color='orange')),
        row=3, col=1
    )

    # Update layout
    fig.update_layout(
        height=800,
        title="SPY Technical Analysis",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    st.plotly_chart(fig, use_container_width=True)

    # Data validation section
    st.header("Data Quality Metrics")

    # Show warnings if any (unexpected issues)
    if validation.get('warnings'):
        st.error("⚠️ Data Quality Issues:\n" + "\n".join(validation['warnings']))

    # Show informational messages (expected gaps)
    if validation.get('info_messages'):
        st.info("ℹ️ Expected Data Patterns:\n" + "\n".join(validation['info_messages']))

    metrics_df = pd.DataFrame([validation['metrics']])
    st.dataframe(metrics_df)

except requests.exceptions.RequestException as e:
    st.error("Failed to connect to the API. Make sure the FastAPI server is running.")
except Exception as e:
    st.error(f"An error occurred: {str(e)}")

st.markdown("---")
st.markdown("Dashboard last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))