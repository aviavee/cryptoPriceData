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

# Set the path to the root directory you want to loop through
path = '/home/erlend/projects/priceData/data/binance'
exportPath = '/home/erlend/projects/freqtrade/user_data/data/binance'
baseAssets = ['USDT', 'BTC']
# baseAssets = ('BTC', 'ETH', 'BUSD', 'USDT')
timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", '3d', '1mo']
# timeFrames = ['1w','1d', '4h', '1h', '30m', '15m', '5m', '3m', '1m']
# timeFrames = ['12h','1m', '1mo', '2h', '3d', '3m', '6h', '8h']
ActualTimeFrames = list()
symbol = []

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
                # print(asset)
                data.insert(0, asset + "_" + baseAsset + "-" + str(match.group(2)))
                data.insert(1, asset + "_" + baseAsset + "/ohlcv/tf_" + str(match.group(2)))
                # print(data)
                # quit()
                return data
        return False

# Loop through all TICKER folders.
for dir_name in os.listdir(path):
    # Create a variable for the current directory
    ticker = os.path.join(path, dir_name)
    # print(ticker)

    if dir_name not in 'KEYUSDT':
        continue

    # if not any(timeFrame in sub_dir for timeFrame in timeFrames):
    if not any(baseAsset in dir_name for baseAsset in baseAssets):
        continue
    
    # Check if the current directory is a directory (not a file)
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
                    deleteJsonPriceFiles()
                    break

                li.append(df1)
            
            # if no data imported, skip exporting.
            if not li:
                continue

            H5data = prepareFilename(file_list[0])
            if not H5data:
                continue

            df = pd.concat(li,ignore_index=True)
            outData = df.iloc[:, :6]
            # print(outData)
            # quit()
            # output data to HDF5 file
            output_path = os.path.join(exportPath, H5data[0] + ".json")
            outData.to_json(output_path, orient='values')
            # print(H5data[1])
            print("Exported " + H5data[0])
            symbol = H5data[0].split("-")
            # quit()

        if symbol:
            tf = " ".join(ActualTimeFrames)
            cmd = '/home/erlend/projects/freqtrade/.env/bin/freqtrade convert-data --candle-types spot --tradingmode spot --format-from json --format-to feather -c /home/erlend/projects/freqtrade/user_data/configBackup.json -d "/home/erlend/projects/freqtrade/user_data/data/binance" -p ' + symbol[0] + ' --timeframes ' + tf
            subprocess.run(cmd, shell=True)
            deleteJsonPriceFiles()
            print("Converted " + symbol[0] + " timeframes " + tf)
            symbol = []
            # quit()

        # quit()

