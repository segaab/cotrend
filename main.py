import os
import streamlit as st
from sodapy import Socrata
from data_fetcher import get_last_two_reports
from analysis import aggregate_report_data, analyze_change, analyze_positions

# Asset lists by categories
commodities = ["GOLD - COMMODITY EXCHANGE INC.","SILVER - COMMODITY EXCHANGE INC.","WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE"]
forex = ["JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE", "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE","CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE","BRITISH POUND - CHICAGO MERCANTILE EXCHANGE", "EURO FX - CHICAGO MERCANTILE EXCHANGE","USD INDEX - ICE FUTURES U.S.", "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE","SWISS FRANC - CHICAGO MERCANTILE EXCHANGE"]
indices = ["DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE", "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE", "NIKKEI STOCK AVERAGE YEN DENOM - CHICAGO MERCANTILE EXCHANGE"]

# Initialize Socrata client
MyAppToken = os.getenv('SODAPY_TOKEN')
client = Socrata("publicreporting.cftc.gov", MyAppToken)

# Pull COT data 
cot_data = get_last_two_reports(client)

# Setup Streamlit page
st.set_page_config(layout="wide")
cl = st.columns(2)
with cl[0]:
    st.image("./assets/logo.png", width=350)
with cl[1]:
    st.image("./assets/banner.png", width=350)

# Create two columns for layout
col1, col2 = st.columns(2)

# Display Forex data
with col1:
    st.header("Forex")
    for asset in forex:
        short_asset = asset.split(" -")[0]
        with st.expander(short_asset):
            asset_data = aggregate_report_data(cot_data, asset)
            analytics_df = analyze_change(asset_data)
            chart_data = analyze_positions(analytics_df)
            analytics_df = analytics_df.rename(columns={'group':'Traders','change_in_net_pct':"Net Change %"})
            
            _col = st.columns(2)
            with _col[0]:
                st.table(analytics_df[['Traders', 'Net Change %']])
            with _col[1]:
                st.bar_chart(chart_data, color=["#ff3131","#38b6ff"])

# Display Commodities and Indices data
with col2:
    # Commodities section
    st.header("Commodities")
    for asset in commodities:
        short_asset = asset.split(" -")[0]
        with st.expander(short_asset):
            asset_data = aggregate_report_data(cot_data, asset)
            analytics_df = analyze_change(asset_data)
            chart_data = analyze_positions(analytics_df)
            analytics_df = analytics_df.rename(columns={'group':'Traders','change_in_net_pct':"Net Change %"})
            
            _col = st.columns(2)
            with _col[0]:
                st.table(analytics_df[['Traders', 'Net Change %']])
            with _col[1]:
                st.bar_chart(chart_data, color=["#ff3131","#38b6ff"])

    # Indices section
    st.header("Indices")
    for asset in indices:
        short_asset = asset.split(" -")[0]
        with st.expander(short_asset):
            asset_data = aggregate_report_data(cot_data, asset)
            analytics_df = analyze_change(asset_data)
            chart_data = analyze_positions(analytics_df)
            analytics_df = analytics_df.rename(columns={'group':'Traders','change_in_net_pct':"Net Change %"})
            
            _col = st.columns(2)
            with _col[0]:
                st.table(analytics_df[['Traders', 'Net Change %']])
            with _col[1]:
                st.bar_chart(chart_data, color=["#ff3131","#38b6ff"])