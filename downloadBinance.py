"""
This Python script is designed to download candlestick data for various trading pairs from the 
Binance cryptocurrency exchange. It uses the Binance public API to fetch the list of trading pairs 
and then downloads monthly candlestick data in a compressed format for each available trading pair. 
The script supports multiple timeframes and employs multithreading to enhance the efficiency of data 
downloads.

Key features include:
- Fetching active trading pairs that involve USDT, ETH, and BTC as quote assets from the Binance 
  exchange.
- Downloading candlestick data for a range of specified timeframes, including 1 minute, 3 minutes, 
  up to 1 month.
- Utilizing a ThreadPoolExecutor to download data for different trading pairs and timeframes 
  concurrently, significantly improving the speed of data acquisition.
- Handling HTTP errors gracefully and skipping downloads for unavailable data.
- Organizing downloaded files into directories based on the trading pair and timeframe for easy 
  access and management.
- Keeping a log of downloaded tickers to avoid re-downloading data.

The script requires external libraries including requests for HTTP requests, datetime and dateutil 
for date manipulation, os for filesystem operations, and concurrent.futures for multithreading.

Note: This script is intended for educational and research purposes, and its use should comply with 
Binance's API terms of service.
"""

import os
import requests
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor

# Set the base URL for the Binance data download
base_url = "https://data.binance.vision"

# Define the directory to save the downloaded files
save_dir = "data/binance/monthly"

# Define the number of threads to use for downloading files
num_threads = 15
num_errors = []
max_errors = 2

# Define the available timeframes
timeframes = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", '3d', '1mo']

# Download active trading pairs from Binance
def get_usdt_btc_trading_pairs():
    # Set up the API endpoint and parameters
    endpoint = "https://api.binance.com/api/v3/exchangeInfo"

    # Make the API request
    response = requests.get(endpoint)

    # Parse the response JSON and extract the trading pairs
    pairs = [f"{pair['symbol']}" for pair in response.json()["symbols"] if pair["quoteAsset"] == "USDT"]
    # pairs += [f"{pair['symbol']}" for pair in response.json()["symbols"] if pair["quoteAsset"] == "BUSD"]
    pairs += [f"{pair['symbol']}" for pair in response.json()["symbols"] if pair["quoteAsset"] == "ETH"]
    pairs += [f"{pair['symbol']}" for pair in response.json()["symbols"] if pair["quoteAsset"] == "BTC"]
    # Return the list of trading pairs
    return pairs

# Define the function to download a file from the Binance data download URL
def download_file(url, save_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

# Define the function to download all available candlestick data for a given ticker and timeframe
def download_candlestick_data(ticker, timeframe):
    # Set the parameters for the candlestick data URL
    biz = "spot"
    interval = timeframe
    # print(interval)
    today = date.today() - relativedelta(months=+1)
    year_month = today.strftime("%Y-%m")
    # print(year_month)
    # quit()

    # Create a subfolder inside the save directory for the current ticker and timeframe
    ticker_dir = os.path.join(save_dir, ticker, timeframe)
    os.makedirs(ticker_dir, exist_ok=True)

    # Download candlestick data for each day in the current month
    while True:
        year_month = today.strftime("%Y-%m")
        # url = f"{base_url}/data/{biz}/daily/klines/{ticker}/{interval}/{ticker}-{interval}-{year_month}-{today.day:02d}.zip"
        url = f"{base_url}/data/{biz}/monthly/klines/{ticker}/{interval}/{ticker}-{interval}-{year_month}.zip"
        # print(url)
        # save_path = os.path.join(ticker_dir, f"{ticker}-{interval}-{year_month}-{today.day:02d}.zip")
        save_path = os.path.join(ticker_dir, f"{ticker}-{interval}-{year_month}.zip")

        if os.path.exists(save_path) and os.path.getsize(save_path) == 0:
            os.remove(save_path)
        elif os.path.exists(save_path):
            today = today - relativedelta(months=+1)
            continue
        
        try:
            download_file(url, save_path)
            print(f"Downloaded {url} to {save_path}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"{url} not found. Skipping...")
                break
        # Move to the previous day
        today = today - relativedelta(months=+1)

# Define the function to download all available candlestick data for a given ticker
def download_candlestick_data_all_timeframes(ticker):
    # Download candlestick data for each timeframe using multithreading
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda tf: download_candlestick_data(ticker, tf), timeframes)

# Define the main function to download all available candle

def main():
    # Create the directory to save the downloaded files
    os.makedirs(save_dir, exist_ok=True)

    # Retrieve all trading pairs from the Binance API
    tickers = get_usdt_btc_trading_pairs()
    # print(tickers)

    # Check the log file for previously downloaded tickers
    downloaded_tickers = []
    if os.path.exists(os.path.join(save_dir, "logBinance.txt")):
        with open(os.path.join(save_dir, "logBinance.txt"), "r") as f:
            downloaded_tickers = f.read().splitlines()

    # print(downloaded_tickers)
    # Download candlestick data for each ticker and timeframe using multithreading
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for ticker in tickers:
            if ticker not in downloaded_tickers:
                download_candlestick_data_all_timeframes(ticker)
                with open(os.path.join(save_dir, "logBinance.txt"), "a") as f:
                    f.write(f"{ticker}\n")

if __name__ == "__main__":
    main()