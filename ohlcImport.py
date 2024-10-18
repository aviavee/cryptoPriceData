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
import json
import pandas as pd
import re
import subprocess
import pyarrow.feather as feather
# import threading
import concurrent.futures

logfile = "logBinance.txt"
# Set the path to the root directory you want to loop through
path = '/home/erlend/projects/priceData/data/binance/monthly'
# path = '/home/erlend/projects/freqtrade/user_data/data/binance/price_history/price_history'
exportPath = '/home/erlend/projects/freqtrade/user_data/data/binance'
# baseAssets = ['USDT']
baseAssets = ['BTC', 'ETH', 'USDT']
timeFrames = ['1m','1w','1d', '4h', '1h', '30m', '15m', '5m', '1m', '12h', '1mo', '2h', '3d', '3m', '6h', '8h']
# timeFrames = ['1mo','1w']
ActualTimeFrames = list()
# symbol = []

max_threads = 1  # Maximum number of threads to run concurrently
# Save the original working directory
original_dir = os.getcwd()

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
                return data
        return False

def processDirectory(ticker):
        # Check if the current directory is a directory (not a file)
    # print(ticker)
    if os.path.isdir(ticker):
        # Create a variable for the first subdirectory
        timeframes = os.listdir(ticker)
        ActualTimeFrames = list()

        for sub_dir in os.listdir(ticker):
            sub_dir_path = os.path.join(ticker, sub_dir)
            # print(sub_dir_path)
            
            # if not any(timeFrame in sub_dir for timeFrame in timeFrames):
            if not sub_dir in timeFrames:
                continue
            # try:
            #     timeFrames.index(sub_dir)

            ActualTimeFrames.append(sub_dir) 
            for subdir, dirs, files in os.walk(sub_dir_path):
                # list all files in an array
                file_list = []
                for file in files:
                    # check for zip files and files with correct timeframe as listed in timeframes tuple
                    if file.endswith('.zip'):
                        file_list.append(file)
                # sort filenames by name
                file_list.sort()

            # quit()
            # extract and import CSV data to a variable
            li = []
            # df = pd.concat((pd.read_csv(os.path.join(sub_dir_path, f)) for f in file_list), ignore_index=True)
            # print(df)

            for file_name in file_list:
                
                # print(os.path.join(sub_dir_path, file_name))
                # df = pd.read_csv(os.path.join(sub_dir_path, file_name), index_col=None, header=0)
                try:
                    df1 = pd.read_csv(os.path.join(sub_dir_path, file_name), index_col=None, header=None)
                except zipfile.BadZipfile:
                    li = []
                    print(f"Bad ZIP file: " + os.path.join(sub_dir_path, file_name))
                    deleteJsonPriceFiles()
                    break

                li.append(df1)
            
            # if no data imported, skip exporting.
            if not li:
                continue

            H5data = prepareFilename(file_list[0])
            if not H5data:
                continue

            # Assuming 'li' is already defined and populated with DataFrames
            df = pd.concat(li, ignore_index=True)

            # Convert the first column (epoch time in milliseconds) to datetime format
            df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], unit='ms', errors='coerce')

            # Localize the datetime to UTC (if required)
            df.iloc[:, 0] = df.iloc[:, 0].dt.tz_localize('UTC')

            # Format the datetime to the desired output
            df.iloc[:, 0] = df.iloc[:, 0].dt.strftime('%Y-%m-%d %H:%M:%S%z')

            # Create a mapping for renaming columns
            rename_mapping = {
                df.columns[0]: 'date',   # Rename the first column to 'date'
                df.columns[1]: 'open',   # Rename the second column to 'open'
                df.columns[2]: 'high',   # Rename the third column to 'high'
                df.columns[3]: 'low',    # Rename the fourth column to 'low'
                df.columns[4]: 'close',  # Rename the fifth column to 'close'
                df.columns[5]: 'volume'  # Rename the sixth column to 'volume'
            }

            # Rename the columns using the mapping
            df.rename(columns=rename_mapping, inplace=True)

            # Select the relevant columns (which should now include all named columns)
            outData = df.iloc[:, :6]

            # Specify the output path for the Feather file
            output_path = os.path.join(exportPath, H5data[0] + ".feather")

            # Use regex to replace '1mo' with '1Mo' in the output path
            output_path = re.sub(r'1mo', '1Mo', output_path, flags=re.IGNORECASE)

            # Save the DataFrame to a Feather file
            feather.write_feather(outData, output_path)

            print("Exported " + H5data[0] + ".feather")
            symbol = H5data[2]
        
        # if symbol:
        # tf = " ".join(ActualTimeFrames)
        # freqtrade_home = '/home/erlend/projects/freqtrade'
        # os.chdir(freqtrade_home)
        # cmd = f'./.env/bin/freqtrade convert-data --candle-types spot --tradingmode spot --format-from json --format-to feather -c /home/erlend/projects/freqtrade/user_data/configBackup.json -d /home/erlend/projects/freqtrade/user_data/data/binance -p ' + symbol + ' --timeframes ' + tf
        # subprocess.run(cmd, shell=True)
        # os.chdir(original_dir)
        # deleteJsonPriceFiles()
        # print("Converted " + symbol + " timeframes " + tf)
        # name = str()
        # quit()


def main():
    downloaded_tickers = []
    # if os.path.exists(os.path.join(exportPath, logfile)):
    #     with open(os.path.join(exportPath, logfile), "r") as f:
    #         downloaded_tickers = f.read().splitlines()

    # Loop through all TICKER folders.
    for dir_name in os.listdir(path):
        # Create a variable for the current directory
        ticker = os.path.join(path, dir_name)
        # print(ticker)

        if dir_name not in 'ETHUSDT':
            continue

        # if not any(timeFrame in sub_dir for timeFrame in timeFrames):
        if not any(baseAsset in dir_name for baseAsset in baseAssets):
            continue
        
        if dir_name not in downloaded_tickers:
            try:
                processDirectory(ticker)
                with open(os.path.join(exportPath, logfile), "a") as f:
                    f.write(f"{dir_name}\n")
                    # False
            except Exception as e:
                print(e)

if __name__ == "__main__":
    main()