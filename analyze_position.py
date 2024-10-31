import pandas as pd
def analyze_positions(aggregated_data):
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



# # Pulling previous and latest report
# latest_data, previous_data=get_last_two_reports(client)

# # Search & Aggregate Asset Report
# aggregated_data =aggregate_report_data(latest_data, previous_data, asset_name="GOLD")

# # Analyze Aggragted Data
# analyzed_positions = analyze_positions(aggregated_data)

# # Display Analytics
# analyzed_positions.to_json('analyzed_position.json',orient='records', lines=True)

