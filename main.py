import os
import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from sodapy import Socrata
from datetime import datetime, timedelta
from aggregate_report_data import aggregate_report_data
# from analyze_position import analyze_positions
# os.environ[""]

def create_normalized_stacked_bar_data(df):
    """
    Creates normalized data for long and short positions for each trader group.

    Parameters:
    df (pd.DataFrame): DataFrame containing long and short positions with percentages.

    Returns:
    pd.DataFrame: DataFrame with normalized percentages for stacked bar charting.
    """
    # Calculate proportions for each trader group
    stacked_data = df[['group', 'latest_long_pct', 'latest_short_pct']].copy()
    stacked_data['total'] = stacked_data['latest_long_pct'] + stacked_data['latest_short_pct']
    stacked_data['normalized_long'] = stacked_data['latest_long_pct'] / stacked_data['total'] * 100
    stacked_data['normalized_short'] = stacked_data['latest_short_pct'] / stacked_data['total'] * 100
    
    # Reshape data for easier plotting
    plot_data = stacked_data.melt(id_vars='group', value_vars=['normalized_long', 'normalized_short'], 
                                  var_name='Position', value_name='Percentage')
    plot_data['Position'] = plot_data['Position'].replace({'normalized_long': 'Long', 'normalized_short': 'Short'})
    return plot_data

def get_last_two_reports(client):
    edt_now = datetime.utcnow() - timedelta(hours=4)

    # Find last Friday's date
    last_friday = edt_now - timedelta(days=(edt_now.weekday() - 4) % 7)

    # If it's Friday and before 3:30 PM ET, use the previous Friday
    report_time = last_friday.replace(hour=15, minute=30, second=0)
    if edt_now.weekday() == 4 and edt_now < report_time:
        last_friday = last_friday - timedelta(weeks=1)

    # Calculate the latest Tuesday and previous Tuesday
    latest_tuesday = last_friday - timedelta(days=3)
    previous_tuesday = latest_tuesday - timedelta(weeks=1)

    # Format dates as strings
    latest_tuesday_str = latest_tuesday.strftime('%Y-%m-%d')
    previous_tuesday_str = previous_tuesday.strftime('%Y-%m-%d')

    # Retrieve the latest and previous reports
    latest_result = client.get("6dca-aqww", where=f"report_date_as_yyyy_mm_dd = '{latest_tuesday_str}'")
    previous_result = client.get("6dca-aqww", where=f"report_date_as_yyyy_mm_dd = '{previous_tuesday_str}'")

    # Convert the results to DataFrames
    latest_df = pd.DataFrame.from_records(latest_result) if latest_result else pd.DataFrame()
    previous_df = pd.DataFrame.from_records(previous_result) if previous_result else pd.DataFrame()

    # Merging the two DataFrames based on 'market_and_exchange_names'
    merged_data = pd.merge(
        previous_df, 
        latest_df, 
        on='market_and_exchange_names', 
        how='outer', 
        suffixes=('_previous', '_latest')
    )

    # Create a list to store the final JSON-like structure
    final_data = []
    
    for _, row in merged_data.iterrows():
        market_name = row['market_and_exchange_names']
        
        # Extract the columns belonging to previous and latest reports
        previous_report = row.filter(like='_previous').dropna().to_dict()
        latest_report = row.filter(like='_latest').dropna().to_dict()
        
        # Clean up the dictionary keys by removing the '_previous' and '_latest' suffixes
        previous_report = {key.replace('_previous', ''): value for key, value in previous_report.items()}
        latest_report = {key.replace('_latest', ''): value for key, value in latest_report.items()}
        
        # Append the result for this market
        final_data.append({
            "market_and_exchange_names": market_name,
            "previous_report": previous_report,
            "latest_report": latest_report
        })

    return final_data
def asset_name_filter(data, asset_name=None):
    """
    Filters the report data based on the provided asset name.

    Parameters:
    data (list of dict): The combined list from get_last_two_reports.
    asset_name (str): The asset name to filter by.

    Returns:
    list of dict: Filtered data where 'market_and_exchange_names' contains the asset_name.
    """
    if asset_name:
        filtered_data = [item for item in data if asset_name.lower() in item['market_and_exchange_names'].lower()]
    else:
        filtered_data = data
    return filtered_data

def filter_results(data, asset_name=None):
    """
    Filters the data by asset name and returns a list of filtered 'market_and_exchange_names'.

    Parameters:
    data (list of dict): The combined list from get_last_two_reports.
    asset_name (str): The asset name to filter by.

    Returns:
    list: A list of 'market_and_exchange_names' from the filtered data.
    """
    filtered_data = asset_name_filter(data, asset_name)
    return [item['market_and_exchange_names'] for item in filtered_data]

def aggregate_report_data(data, asset_name=None):
    """
    Aggregates and nests rows of data from both the previous and latest reports
    under the corresponding 'market_and_exchange_names', matching with asset names.

    Parameters:
    data (list of dict): The combined list from get_last_two_reports.
    asset_name (str): The asset name to filter by.

    Returns:
    pd.DataFrame: Aggregated data where latest and previous reports are nested under each asset name.
    """
    filtered_market_names = filter_results(data, asset_name)

    aggregated_data = []
    for item in data:
        if item['market_and_exchange_names'] in filtered_market_names:
            aggregated_data.append({
                'market_and_exchange_names': item['market_and_exchange_names'],
                'latest_report': item['latest_report'],
                'previous_report': item['previous_report']
            })

    return pd.DataFrame(aggregated_data)

def analyze_change(aggregated_data):
    """
    Analyzes long and short positions, net differences, and changes in net positions
    for Non-commercial, Commercial, and Non-reportable groups.

    Parameters:
    aggregated_data (pd.DataFrame): DataFrame containing aggregated COT reports.

    Returns:
    pd.DataFrame: DataFrame with analyzed position percentages, net differences,
    and changes in net differences.
    """
    analysis_results = []

    for _, row in aggregated_data.iterrows():
        market_name = row['market_and_exchange_names']
        
        latest_report = row['latest_report']
        previous_report = row['previous_report']

        
        # Ensure both reports exist before proceeding
        if not latest_report or not previous_report:
            continue  # Skip this row if any report is missing'

        # latest_report = latest_report[0]  # Assuming only one entry per market
        # previous_report = previous_report[0]  # Assuming only one entry per market

        for group in ['noncomm_positions', 'comm_positions', 'nonrept_positions']:
            # Latest report data
            latest_long = float(latest_report.get(f'{group}_long_all', 0))
            latest_short = float(latest_report.get(f'{group}_short_all', 0))
            total_latest = latest_long + latest_short

            # Previous report data
            previous_long = float(previous_report.get(f'{group}_long_all', 0))
            previous_short = float(previous_report.get(f'{group}_short_all', 0))
            total_previous = previous_long + previous_short

            # Calculate percentages
            latest_long_pct = (latest_long / total_latest) * 100 if total_latest > 0 else 0
            latest_short_pct = (latest_short / total_latest) * 100 if total_latest > 0 else 0
            previous_long_pct = (previous_long / total_previous) * 100 if total_previous > 0 else 0
            previous_short_pct = (previous_short / total_previous) * 100 if total_previous > 0 else 0

            # Net position difference (long - short)
            latest_net_pct = latest_long_pct - latest_short_pct
            previous_net_pct = previous_long_pct - previous_short_pct

            # Change in net position difference
            change_in_net_pct = latest_net_pct - previous_net_pct

            analysis_results.append({
                'market_and_exchange_names': market_name,
                'group': group,
                'latest_long_pct': latest_long_pct,
                'latest_short_pct': latest_short_pct,
                'previous_long_pct': previous_long_pct,
                'previous_short_pct': previous_short_pct,
                'latest_net_pct': latest_net_pct,
                'previous_net_pct': previous_net_pct,
                'change_in_net_pct': change_in_net_pct
            })

    return pd.DataFrame(analysis_results)

def analyze_positions(cot_data):
    # Aggregate long and short positions by trader group
    commercial_long = cot_data.loc[(cot_data['group'] == 'comm_positions'), 'latest_long_pct'].sum()
    commercial_short = cot_data.loc[(cot_data['group'] == 'comm_positions'), 'latest_short_pct'].sum()
    
    non_commercial_long = cot_data.loc[(cot_data['group'] == 'noncomm_positions'), 'latest_long_pct'].sum()
    non_commercial_short = cot_data.loc[(cot_data['group'] == 'noncomm_positions'), 'latest_short_pct'].sum()
    
    non_reportable_long = cot_data.loc[(cot_data['group'] == 'nonrept_positions'), 'latest_long_pct'].sum()
    non_reportable_short = cot_data.loc[(cot_data['group'] == 'nonrept_positions'), 'latest_short_pct'].sum()

    # Format data to match the required output structure
    data = {
        'Trader Group': ['Commercial', 'Non-Commercial', 'Non-Reportable'],
        'Long': [commercial_long, non_commercial_long, non_reportable_long],
        'Short': [commercial_short, non_commercial_short, non_reportable_short]
    }

    position_df = pd.DataFrame(data)

    # Calculate total positions for normalization
    position_df['Total'] = position_df['Long'] + position_df['Short']
    position_df['Long (%)'] = (position_df['Long'] / position_df['Total']) * 100
    position_df['Short (%)'] = (position_df['Short'] / position_df['Total']) * 100
    
    # Keep only the percentage columns for plotting
    position_df = position_df[['Trader Group', 'Long (%)', 'Short (%)']].set_index('Trader Group')
    
    return position_df


# Asset list by catagories
commodities = ["GOLD - COMMODITY EXCHANGE INC.","SILVER - COMMODITY EXCHANGE INC.","WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE"]
commodities_names = ["Gold","Silver","WTI Crude Oil"]
_commodities ={
    'cot_name':["GOLD - COMMODITY EXCHANGE INC.","SILVER - COMMODITY EXCHANGE INC.","WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE"],
    'asset_name':["Gold","Silver","WTI Crude Oil"]
    }
forex = ["JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE", "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE","CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE","BRITISH POUND - CHICAGO MERCANTILE EXCHANGE", "EURO FX - CHICAGO MERCANTILE EXCHANGE","USD INDEX - ICE FUTURES U.S.", "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE","SWISS FRANC - CHICAGO MERCANTILE EXCHANGE"]
forex_names = ["JPY", "AUD","CAD","GBP", "EUR","DXY", "NZD","CHF"]
_forex = {
    'cot_name':["JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE", "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE","CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE","BRITISH POUND - CHICAGO MERCANTILE EXCHANGE", "EURO FX - CHICAGO MERCANTILE EXCHANGE","USD INDEX - ICE FUTURES U.S.", "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE","SWISS FRANC - CHICAGO MERCANTILE EXCHANGE"],
    'asset_name':["JPY", "AUD","CAD","GBP", "EUR","DXY", "NZD","CHF"]
    }

indicies = ["DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE", "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE", "NIKKEI STOCK AVERAGE YEN DENOM - CHICAGO MERCANTILE EXCHANGE"]
_indices = {
    'cot_name':["DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE", "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE", "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE", "NIKKEI STOCK AVERAGE YEN DENOM - CHICAGO MERCANTILE EXCHANGE"],
    'asset_name':["DOW JONES", "S&P 500", "NQ-100","NIKKEI"]
}
# Initialize Socrata client (update your credentials securely)
MyAppToken = os.getenv('SODAPY_TOKEN')
client = Socrata("publicreporting.cftc.gov", MyAppToken)

# Pull COT data 
cot_data = get_last_two_reports(client)
st.set_page_config(layout="wide")
# st.title("CoTrends")
cl = st.columns(2)
with cl[0]:
    st.image("./assets/logo.png",width = 350)
with cl[1]:
    st.image("./assets/banner.png",width = 350)
col1, col2 = st.columns(2)

with col1:
    st.header("Forex")
    for asset in forex:
        asset = asset.split(" -")[0]
        with st.expander(asset):
            asset_data = aggregate_report_data(cot_data, asset)
            analytics_df = analyze_change(asset_data)
            # Prepare data for stacked bar chart
            chart_data = analyze_positions(analytics_df)
            _col = st.columns(2)
            with _col[0]:
                st.table(analytics_df[['group', 'change_in_net_pct']])
            with _col[1]:
                # Display the chart
                st.bar_chart(chart_data)

with col2:
    st.header("Commodities")
    for asset in commodities:
        asset = asset.split(" -")[0]
        with st.expander(asset):
            asset_data = aggregate_report_data(cot_data, asset)
            analytics_df = analyze_change(asset_data)
            # Prepare data for stacked bar chart
            chart_data = analyze_positions(analytics_df)
            _col = st.columns(2)
            with _col[0]:
                st.table(analytics_df[['group', 'change_in_net_pct']])
            with _col[1]:
                # Display the chart
                st.bar_chart(chart_data)

    st.header("Indices")
    for asset in indicies:
        asset = asset.split(" -")[0]
        with st.expander(asset):
            asset_data = aggregate_report_data(cot_data, asset)
            analytics_df = analyze_change(asset_data)
            # Prepare data for stacked bar chart
            chart_data = analyze_positions(analytics_df)
            _col = st.columns(2)
            with _col[0]:
                st.table(analytics_df[['group', 'change_in_net_pct']])
            with _col[1]:
                # Display the chart
                st.bar_chart(chart_data)


