import os
import pandas as pd
import pyarrow.feather as feather
import re

exportPath = '/home/erlend/projects/freqtrade/user_data/data/binance/futures'
path = '/home/erlend/projects/priceData/data/binance_futures/monthly'

def load_and_combine_price_data(ticker):
    """
    Loads 1d price data for the specified ticker, aggregates it into 
    2d, 3d, 4d, and 5d timeframes, and exports to Feather files.

    Parameters:
    - ticker (str): The ticker symbol for which to load and aggregate data.
    """
    # Load 1d price data
    file_path = os.path.join(exportPath, f"{ticker}_USDT-1d-futures.feather")
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    # Load the 1d data into a DataFrame
    df_1d = pd.read_feather(file_path)
    df_1d['date'] = pd.to_datetime(df_1d['date'])

    # Function to resample and aggregate data
    def aggregate_data(df, days):
        return df.resample(f'{days}D', on='date').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()

    # Aggregate data to 2d, 3d, 4d, and 5d timeframes
    df_2d = aggregate_data(df_1d, 2)
    df_3d = aggregate_data(df_1d, 3)
    df_4d = aggregate_data(df_1d, 4)
    df_5d = aggregate_data(df_1d, 5)

    # Function to export DataFrame to Feather
    def export_to_feather(df, days):
        output_filename = f"{ticker}_USDT-{days}d-futures.feather"
        output_path = os.path.join(exportPath, output_filename)
        feather.write_feather(df, output_path)
        print(f"Exported {output_path}")

    # Export each aggregated DataFrame to Feather files
    export_to_feather(df_2d, 2)
    export_to_feather(df_3d, 3)
    export_to_feather(df_4d, 4)
    export_to_feather(df_5d, 5)

# Example usage
load_and_combine_price_data("BTC_USDT")
