import pandas as pd
import json

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



