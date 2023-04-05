import os
import requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

#Download active trading pairs from Gate.IO
def get_usdt_btc_trading_pairs():
    # Set up the API endpoint and parameters
    endpoint = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    params = {"settle": "usdt,btc"}

    # Make the API request
    response = requests.get(endpoint, params=params)
    
    # Parse the response JSON and extract the trading pairs
    pairs = [pair["id"] for pair in response.json()]

    # Return the list of trading pairs
    return pairs

# Set the base URL for the Gate.io data download
base_url = "https://download.gatedata.org"

# Define the directory to save the downloaded files
save_dir = "/home/erlend/projects/priceData/data/gateio"

# Define the number of threads to use for downloading files
num_threads = 5

# Define the available timeframes
timeframes = ["1m", "5m", "1h", "4h", "1d"]

# Define the function to download a file from the Gate.io data download URL
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
    today = date.today() - timedelta(days=50)
    # last_month = date.today() - timedelta(days=50)
    year_month = today.strftime("%Y%m")

    # Create a subfolder inside the save directory for the current ticker and timeframe
    ticker_dir = os.path.join(save_dir, ticker, timeframe)
    os.makedirs(ticker_dir, exist_ok=True)

    # Download candlestick data for each month, starting from last month
    while True:
        url = f"{base_url}/{biz}/candlesticks_{timeframe}/{year_month}/{ticker}-{year_month}.csv.gz"
        save_path = os.path.join(ticker_dir, f"{ticker}-{year_month}.csv.gz")
        if os.path.exists(save_path):
            print(f"File {save_path} already exists, skipping download")
        else:
            try:
                download_file(url, save_path)
                print(f"Downloaded {url} to {save_path}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"{url} not found. Skipping...")
                    break
        # Move to the previous month
        today = today - timedelta(days=30)
        year_month = today.strftime("%Y%m")

# Define the function to download all available candlestick data for a given ticker
def download_candlestick_data_all_timeframes(ticker):
    # Download candlestick data for each timeframe using multithreading
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda tf: download_candlestick_data(ticker, tf), timeframes)

# Define the main function to download all available candlestick data for all tickers and timeframes
def main():
    # Create the directory to save the downloaded files
    os.makedirs(save_dir, exist_ok=True)

    # Retrieve all USDT and BTC trading pairs from the Gate.io API
    tickers = get_usdt_btc_trading_pairs()

    # Download candlestick data for each ticker and timeframe using multithreading
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        try:
            executor.map(download_candlestick_data_all_timeframes, tickers)
        except KeyboardInterrupt:
            print("Keyboard interrupt detected, waiting for threads to finish...")
            executor.shutdown(wait=True)


if __name__ == "__main__":
    main()