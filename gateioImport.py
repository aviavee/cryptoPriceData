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
# import threading
import concurrent.futures

logfile = "logGateIO.txt"
# Set the path to the root directory you want to loop through
path = '/home/erlend/projects/priceData/data/gateio'
# path = '/home/erlend/projects/freqtrade/user_data/data/binance/price_history/price_history'
exportPath = '/home/erlend/projects/freqtrade/user_data/data/gateio'
baseAssets = ['BTC', 'ETH', 'BUSD' ,'USDT']
# baseAssets = ('BTC', 'ETH', 'BUSD', 'USDT')
timeFrames = ["1m", "5m", "1h", "4h", "1d"]
# timeFrames = ['1m']
ActualTimeFrames = list()

max_threads = 1  # Maximum number of threads to run concurrently

def deleteJsonPriceFiles():
    cmd = "/usr/bin/rm /home/erlend/projects/freqtrade/user_data/data/gateio/*.json"
    subprocess.run(cmd, shell=True)

def prepareFilename(filename):
    # regex pattern to extract ticker, duration, and year-month from filename
    # print(filename)
    # pattern = r'([A-Z0-9]+)\d*-(\d+[a-z]{1,2})-(\d{4}-\d{2})\.zip'
    pattern = r'([A-Z0-9_]+)-(\d{6})\.csv\.gz'
    data = []
    # regex pattern to extract CVC, ETH, and duration from filename
    match = re.match(pattern, filename)

    if not match:
        return False

    for word in filename.split("-"):
        # print(word)
        for baseAsset in baseAssets:
            if word.endswith(baseAsset):
                data.insert(0, str(match.group(1)) + "-" + str(match.group(2)))
                data.insert(1, str(match.group(1)) + "/ohlcv/tf_" + str(match.group(2)))
                data.insert(2, str(match.group(1)))
                return data
        return False

def processDirectory(ticker):
    symbol = str()
        # Check if the current directory is a directory (not a file)
    if os.path.isdir(ticker):
        # Create a variable for the first subdirectory
        timeframes = os.listdir(ticker)
        ActualTimeFrames = list()

        for sub_dir in os.listdir(ticker):
            sub_dir_path = os.path.join(ticker, sub_dir)
            
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
                    if file.endswith('.gzip'):
                        file_list.append(file)
                # sort filenames by name
                file_list.sort()

            # extract and import CSV data to a variable
            li = []
            # df = pd.concat((pd.read_csv(os.path.join(sub_dir_path, f)) for f in file_list), ignore_index=True)
            # print(df)

            for file_name in file_list:
                
                # print(os.path.join(sub_dir_path, file_name))
                # df = pd.read_csv(os.path.join(sub_dir_path, file_name), index_col=None, header=0)
                try:
                    df1 = pd.read_csv(os.path.join(sub_dir_path, file_name), compression='gzip', index_col=None, header=None)
                except zipfile.BadZipfile:
                    li = []
                    deleteJsonPriceFiles()
                    break

                li.append(df1)
            
            # if no data imported, skip exporting.
            if not li:
                continue
            # print(file_list[0])
            # quit()
            H5data = prepareFilename(file_list[0])
            if not H5data:
                continue

            df = pd.concat(li,ignore_index=True)
            outData = df.iloc[:, :6]
            col1 = outData.pop(1)
            outData.insert(len(outData.columns), col1.name, col1)
            outData.columns = range(6)
            # print(outData)
            # quit()
            # output data to HDF5 file
            output_path = os.path.join(exportPath, H5data[0] + "-" + sub_dir +".json")
            outData.to_json(output_path, orient='values')
            # print(H5data[1])
            print("Exported " + H5data[0] + " to " + output_path)
            symbol = H5data[2]
            # print(symbol)
                
        if symbol and ActualTimeFrames:
            tf = " ".join(ActualTimeFrames)
            cmd = '/home/erlend/projects/freqtrade/.env/bin/freqtrade convert-data --candle-types spot --tradingmode spot --format-from json --format-to feather -c /home/erlend/projects/freqtrade/user_data/GateIO.json -d /home/erlend/projects/freqtrade/user_data/data/gateio -p ' + symbol + ' --timeframes ' + tf
            subprocess.run(cmd, shell=True)
            deleteJsonPriceFiles()
            print("Converted " + symbol + " timeframes " + tf)
            name = str()
            # quit()

def main():
    downloaded_tickers = []
    if os.path.exists(os.path.join(exportPath, logfile)):
        with open(os.path.join(exportPath, logfile), "r") as f:
            downloaded_tickers = f.read().splitlines()

    # Loop through all TICKER folders.
    for dir_name in os.listdir(path):
        # Create a variable for the current directory
        ticker = os.path.join(path, dir_name)
        # print(ticker)
        # quit()

        # if dir_name not in 'ACAUSDT':
        #     continue

        # if not any(timeFrame in sub_dir for timeFrame in timeFrames):
        if not any(baseAsset in dir_name for baseAsset in baseAssets):
            continue
        
        if dir_name not in downloaded_tickers:
            try:
                processDirectory(ticker)
                with open(os.path.join(exportPath, logfile), "a") as f:
                    f.write(f"{dir_name}\n")
                    quit()
            except Exception as e:
                print(e)

if __name__ == "__main__":
    main()