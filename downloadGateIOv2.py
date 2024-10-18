import os
import requests
import gzip
import argparse
from datetime import datetime,date, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import logging  # Import the logging module

# Configure logging
logging.basicConfig(filename='debug.log', level=logging.WARNING, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Add argument parsing to the script to handle the --check-files option
parser = argparse.ArgumentParser(description='Download and check for missing candlestick data from Gate.io.')
parser.add_argument('--check-files', action='store_true', help='Check for missing files and download them.')
args = parser.parse_args()

#Download active trading pairs from Gate.IO
def get_usdt_btc_trading_pairs():
    logging.debug("Entering get_usdt_btc_trading_pairs function")
    endpoint = "https://api.gateio.ws/api/v4/spot/currency_pairs"
    params = {"settle": "usdt,btc,eth"}
    response = requests.get(endpoint, params=params)
    pairs = [pair["id"] for pair in response.json()]
    return pairs

base_url = "https://download.gatedata.org"
save_dir = "data/gateio"
baseAssets = ['BTC', 'ETH' ,'USDT']
num_threads = 1
timeframes = ["1m", "5m", "1h", "4h", "1d"]

def download_file(url, save_path):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

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
        # Check if ticker ends with any value in baseAssets
        if any(ticker.endswith(asset) for asset in baseAssets):
            url = f"{base_url}/{biz}/candlesticks_{timeframe}/{year_month}/{ticker}-{year_month}.csv.gz"
            save_path = os.path.join(ticker_dir, f"{ticker}-{year_month}.csv.gz")
            if os.path.exists(save_path):
                # Check if the existing file is a valid gzip file
                try:
                    with gzip.open(save_path, 'rb') as f:
                        # Try reading some data to ensure it's a valid gzip file
                        f.read(1)
                    # print(f"File {save_path} is a valid gzip file, skipping download")
                    # File is valid, skip download
                    pass
                except (OSError, EOFError) as e:
                    # If an error occurs, the file is not a valid gzip file
                    print(f"File {save_path} is not a valid gzip file. Deleting file...")
                    os.remove(save_path)
                    # After deleting, you might want to download the file again
                    try:
                        download_file(url, save_path)
                        print(f"Redownloaded {url} to {save_path}")
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 404:
                            # print(f"{url} not found. Skipping...")
                            break
            else:
                # File does not exist, proceed with download
                try:
                    download_file(url, save_path)
                    print(f"Downloaded {url} to {save_path}")
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        # print(f"{url} not found. Skipping...")
                        break
        else:
            # print(f"Ticker {ticker} does not end with a value contained in baseAssets, skipping...")
            # Optionally, you can decide to break or continue based on your loop's logic
            pass
            # break

        # Move to the previous month
        today = today - relativedelta(months=1)
        year_month = today.strftime("%Y%m")

def parse_date_from_filename(filename):
    try:
        # Assuming the filename format is 'TICKER-YYYYMM.csv.gz'
        date_str = filename.split('-')[-1].split('.')[0]  # Extract 'YYYYMM'
        return datetime.strptime(date_str, '%Y%m')
    except ValueError:
        return None

def construct_download_url(base_url, ticker, timeframe, year_month):
    return f"{base_url}/spot/candlesticks_{timeframe}/{year_month}/{ticker}-{year_month}.csv.gz"

def find_missing_dates(start_date, end_date):
    # Generate a list of all expected months between start and end dates
    total_months = lambda dt: dt.month + 12 * dt.year
    missing_months = []
    for tot_m in range(total_months(start_date), total_months(end_date)):
        y, m = divmod(tot_m, 12)
        missing_months.append(datetime(y, m + 1, 1))
    return missing_months

def check_and_download_missing_files(ticker, timeframe, base_url, save_dir):
    ticker_dir = os.path.join(save_dir, ticker, timeframe)
    os.makedirs(ticker_dir, exist_ok=True)
    
    files = [f for f in os.listdir(ticker_dir) if os.path.isfile(os.path.join(ticker_dir, f))]
    dates = list(filter(None, (parse_date_from_filename(f) for f in files)))
    
    if not dates:
        print(f"No files found in {ticker_dir}. Skipping missing file check.")
        return
    
    dates.sort()
    start_date, end_date = dates[0], dates[-1]
    missing_dates = find_missing_dates(start_date, end_date)

    for missing_date in missing_dates:
        year_month = missing_date.strftime('%Y%m')
        if missing_date not in dates:
            url = construct_download_url(base_url, ticker, timeframe, year_month)
            save_path = os.path.join(ticker_dir, f"{ticker}-{year_month}.csv.gz")
            if not os.path.exists(save_path):
                print(f"Missing file detected: {save_path}. Attempting to download...")
                try:
                    download_file(url, save_path)
                    print(f"Downloaded missing file: {url}")
                except requests.exceptions.HTTPError as e:
                    print(f"Error downloading {url}: {e}")
                    if os.path.exists(save_path):
                        os.remove(save_path)  # Clean up partially downloaded file

def is_valid_gzip_file(filepath):
    try:
        with gzip.open(filepath, 'rb') as f:
            f.read(1)
        return True
    except:
        os.remove(filepath)  # Delete the file if it's not a valid gzip
        return False

def download_candlestick_data_all_timeframes(ticker):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        if args.check_files:
            executor.map(lambda tf: check_and_download_missing_files(ticker, tf), timeframes)
        else:
            executor.map(lambda tf: download_candlestick_data(ticker, tf), timeframes)

def main():
    os.makedirs(save_dir, exist_ok=True)
    tickers = get_usdt_btc_trading_pairs()
    progress_bar = tqdm(total=len(tickers), desc='Processing Tickers', position=0)
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(download_candlestick_data_all_timeframes, ticker): ticker for ticker in tickers}
        for future in futures:
            ticker = futures[future]
            try:
                result = future.result()
                progress_bar.update(1)
            except KeyboardInterrupt:
                print("Keyboard interrupt detected, waiting for threads to finish...")
                executor.shutdown(wait=True)
                break
    progress_bar.close()

if __name__ == "__main__":
    main()
