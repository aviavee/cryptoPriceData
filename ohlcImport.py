# 1. list directories in binance/price_history
# 2. directorynames = ticker
# 3. list subdirectories (timeframe)
# 4. list all files inside subdirectory
# 5. sort files, first to last
# 6. extract first file, convert data to json save to disk
# 7. continue with next file.
# 8. when finished, gzip json to jsongz (del, temp files)

import os
import zipfile
import csv
import requests
import pandas as pd
import re
import subprocess
import pyarrow.feather as feather
# import threading
import concurrent.futures

logfile = "logBinanceFutures.txt"
importdata_type = "futures"
# Set the path to the root directory you want to loop through
path = '/home/erlend/projects/priceData/data/binance/monthly'
path = '/home/erlend/projects/priceData/data/binance_futures/monthly'
# path = '/home/erlend/projects/freqtrade/user_data/data/binance/price_history/price_history'
exportPath = '/home/erlend/projects/freqtrade/user_data/data/binance' # spot
exportPath = '/home/erlend/projects/freqtrade/user_data/data/binance/futures' # futures
# baseAssets = ['USDT']
baseAssets = ['BTC', 'ETH', 'USDT']
timeFrames = ['1m','1w','1d', '4h', '1h', '30m', '15m', '5m', '1m', '12h', '1mo', '2h', '3d', '3m', '6h', '8h']
# timeFrames = ['1mo','1w']
ActualTimeFrames = list()
# symbol = []

max_threads = 1  # Maximum number of threads to run concurrently
# Save the original working directory
original_dir = os.getcwd()

def get_active_futures_tickers():
    # URL for the Binance Futures API endpoint for market data
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    response = requests.get(url)
    data = response.json()
    
    active_tickers = []
    for symbol_info in data['symbols']:
        if symbol_info['status'] == 'TRADING':  # Only active tickers
            active_tickers.append(symbol_info['symbol'])
    
    return active_tickers

def deleteJsonPriceFiles():
    cmd = "/usr/bin/rm /home/erlend/projects/freqtrade/user_data/data/binance/*.json"
    subprocess.run(cmd, shell=True)

def prepareFilename(filename):
    # regex pattern to extract ticker, duration, and year-month from filename
    # print(filename)
    pattern = r'([A-Z0-9]+)\d*-(\d+[a-z]{1,2})-(\d{4}-\d{2})\.zip'
    data = []
    # regex pattern to extract CVC, ETH, and duration from filename
    # pattern = r'([A-Z]+[A-Z0-9]*)([A-Z]+)-(\d+[wdm])-(\d{4}-\d{2})\.zip'
    # match the pattern against the filename
    match = re.match(pattern, filename)
    # print(match)
    # quit()
    if not match:
        return False

    for word in filename.split("-"):
        # print(word)
        for baseAsset in baseAssets:
            if word.endswith(baseAsset):
                asset = word.replace(baseAsset, "")
                data.insert(0, asset + "_" + baseAsset + "-" + str(match.group(2)))
                data.insert(1, asset + "_" + baseAsset + "/ohlcv/tf_" + str(match.group(2)))
                data.insert(2, asset + "_" + baseAsset)
                data.insert(3, asset + "_" + baseAsset + "_" + baseAsset + "-" + str(match.group(2)))
                return data
        return False

def processDirectory(ticker):
    """
    Processes data in a given ticker directory, extracts and compiles price data, 
    and exports it to a Feather file format.

    Parameters:
    - ticker (str): The path to the directory containing the ticker data.

    Functionality:
    1. **Directory Check**: Confirms that the provided `ticker` path is a directory.
    2. **Timeframes Filtering**: 
       - Lists all subdirectories within the ticker directory.
       - Filters subdirectories to retain only those that match specified timeframes.
    3. **File Collection**: 
       - Recursively walks through each filtered subdirectory to collect `.zip` files.
       - Sorts the collected filenames for sequential processing.
    4. **Data Extraction and Compilation**:
       - Iterates over the `.zip` files, extracting CSV data into DataFrames.
       - Handles bad ZIP files with error handling, resetting the data list and breaking the loop.
    5. **Data Preprocessing**:
       - Concatenates all collected DataFrames into one DataFrame.
       - Converts the first column from epoch time in milliseconds to a formatted UTC datetime string.
       - Renames columns to standardized names: 'date', 'open', 'high', 'low', 'close', 'volume'.
    6. **Data Export**:
       - Exports the processed DataFrame to a Feather file format.
       - Ensures the output path uses a standardized format (e.g., replacing '1mo' with '1Mo').
       - Prints a success message upon exporting the data.

    Notes:
    - **Error Handling**: Catches `zipfile.BadZipfile` exceptions to handle corrupted ZIP files.
    - **Assumptions**: Assumes that the `li` list of DataFrames and `file_list` of `.zip` files 
      are properly populated before exporting.
    - **Dependencies**: Uses pandas for DataFrame handling and pyarrow for Feather file export.

    Outputs:
    - Saves the compiled price data to a Feather file in the specified export path.

    Example:
    >>> processDirectory('/path/to/ticker_directory')

    """
    # Check if the current directory is a directory (not a file)
    if os.path.isdir(ticker):
        # Create a variable for the first subdirectory
        timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", '3d']
        ActualTimeFrames = list()

        for sub_dir in os.listdir(ticker):
            sub_dir_path = os.path.join(ticker, sub_dir)
            
            # Check if subdirectory matches any specified timeframes
            if not sub_dir in timeframes:
                continue
            
            ActualTimeFrames.append(sub_dir)
            for subdir, dirs, files in os.walk(sub_dir_path):
                # Collect all zip files
                file_list = [file for file in files if file.endswith('.zip')]
                file_list.sort()  # Sort filenames by name
                # print(files)

            li = []  # List to hold DataFrames

            # Extract and import CSV data to a variable
            for file_name in file_list:
                try:
                    df1 = pd.read_csv(os.path.join(sub_dir_path, file_name), index_col=None, header=None)
                except zipfile.BadZipfile:
                    li = []
                    print(f"Bad ZIP file: " + os.path.join(sub_dir_path, file_name))
                    deleteJsonPriceFiles()
                    continue

                li.append(df1)

            # Skip exporting if no data was imported
            if not li:
                continue

            H5data = prepareFilename(file_list[0])      
            if not H5data:
                continue


            # Concatenate all dataframes
            df = pd.concat(li, ignore_index=True)

            # Create an intermediate column for numeric conversion
            df['epoch_numeric'] = pd.to_numeric(df.iloc[:, 0], errors='coerce')

            # Convert to nullable integer type
            df['epoch_numeric'] = df['epoch_numeric'].astype('Int64')

            # Identify and handle non-numeric values
            non_numeric_values = df[df['epoch_numeric'].isnull()][df.columns[0]]
            if not non_numeric_values.empty:
                # Handle non-numeric values (e.g., drop rows)
                df = df.dropna(subset=['epoch_numeric'])

            # Convert to datetime
            df['datetime'] = pd.to_datetime(df['epoch_numeric'], unit='ms', errors='coerce')

            # Check for failed datetime conversions
            if df['datetime'].isnull().any():
                print("Some datetime conversions failed.")

            # Localize to UTC
            df['datetime'] = df['datetime'].dt.tz_localize('UTC')

            # Format the datetime to the desired output
            df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S%z')

            # Drop the intermediate 'epoch_numeric' column
            df = df.drop(columns=['epoch_numeric'])

            # Remove the original epoch column (first column)
            df = df.drop(df.columns[0], axis=1)

            # Move the 'datetime' column to position 0
            cols = df.columns.tolist()
            cols.remove('datetime')
            df = df[['datetime'] + cols]


            # Create a mapping for renaming columns
            rename_mapping = {
                df.columns[0]: 'date',
                df.columns[1]: 'open',
                df.columns[2]: 'high',
                df.columns[3]: 'low',
                df.columns[4]: 'close',
                df.columns[5]: 'volume'
            }

            # Rename the columns
            df.rename(columns=rename_mapping, inplace=True)

            # Convert columns to appropriate data types
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Remove initial rows with zero volume if the first row has zero volume
            if df.iloc[0]['volume'] == 0:
                df = df.loc[df['volume'].ne(0).idxmax():]

            # Select the relevant columns
            outData = df.iloc[:, :6]

            # Specify the output path for the Feather file
            output_path = os.path.join(exportPath, H5data[3] + "-futures.feather")  # 3 futures    0 spot
            
            # # Verify the results


            # Use regex to replace '1mo' with '1Mo' in the output path
            output_path = re.sub(r'1mo', '1Mo', output_path, flags=re.IGNORECASE)

            # Save the DataFrame to a Feather file
            feather.write_feather(outData, output_path)

            print("Exported " + output_path)
            # symbol = H5data[2]

def main():
    """
    Main function to process directories containing ticker data.

    This function performs the following operations:
    1. Retrieves the list of active futures tickers by calling `get_active_futures_tickers()`.
    2. Iterates over directories in a specified path to find ticker data folders.
    3. Checks if the directory name matches the specified base assets and whether it has already been processed.
    4. Processes the directories using multithreading for improved performance.

    Notes:
    - The function skips directories that do not match the specified base assets in `baseAssets`.
    - If the directory name has already been processed (i.e., it is in `downloaded_tickers`), it is skipped.
    - If an exception occurs during processing, the exception is printed.

    Variables:
    - downloaded_tickers: List of tickers that have already been processed.
    - dir_name: Name of the current directory being processed.
    - ticker: Full path of the current directory.

    Functions:
    - get_active_futures_tickers(): Retrieves a list of active futures tickers.
    - processDirectory(ticker): Processes the data in the specified ticker directory.

    Exception Handling:
    - If an error occurs while processing a directory, the exception is caught and printed.
    """
    import concurrent.futures

    downloaded_tickers = get_active_futures_tickers()
    # downloaded_tickers = ['BTCUSDT']
    # if os.path.exists(os.path.join(exportPath, logfile)):
    #     with open(os.path.join(exportPath, logfile), "r") as f:
    #         downloaded_tickers = f.read().splitlines()
    # print(downloaded_tickers)

    # Collect tickers to process
    tickers_to_process = []

    # Loop through all TICKER folders.
    for dir_name in os.listdir(path):
        # Create a variable for the current directory
        ticker = os.path.join(path, dir_name)
        # print(ticker)

        # if dir_name not in 'ETHUSDT':
        #     continue

        # if not any(timeFrame in sub_dir for timeFrame in timeFrames):
        if not any(baseAsset in dir_name for baseAsset in baseAssets):
            continue

        if dir_name in downloaded_tickers:
            tickers_to_process.append(ticker)

    # Function to process a single ticker
    def process_ticker(ticker):
        try:
            processDirectory(ticker)
            # with open(os.path.join(exportPath, logfile), "a") as f:
            #     f.write(f"{os.path.basename(ticker)}\n")
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # Use multithreading to process tickers
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        executor.map(process_ticker, tickers_to_process)


if __name__ == "__main__":
    main()