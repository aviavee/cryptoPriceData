import os
from datetime import datetime, timedelta

def find_files_in_timeframe(path):
    """Yields all files within a specific timeframe directory."""
    for file in os.listdir(path):
        if file.endswith(".csv.gz"):
            yield file

def parse_date_from_filename(filename):
    """Extracts the date from the filename."""
    try:
        # Assuming the filename format is like 'USDP_USDT-202401.csv.gz'
        date_str = filename.split('-')[-1].split('.')[0]  # Extract '202401'
        return datetime.strptime(date_str, '%Y%m')
    except ValueError:
        return None

def construct_download_url(base_url, ticker, timeframe, year_month):
    """Constructs a direct download URL for the missing file."""
    return f"{base_url}/spot/candlesticks_{timeframe}/{year_month}/{ticker}-{year_month}.csv.gz"

def find_missing_files(base_directory, base_url):
    """Finds missing files based on year and month continuity within each ticker/timeframe, between the min and max dates, and prints download URLs."""
    for ticker in os.listdir(base_directory):
        ticker_path = os.path.join(base_directory, ticker)
        if os.path.isdir(ticker_path):
            for timeframe in os.listdir(ticker_path):
                timeframe_path = os.path.join(ticker_path, timeframe)
                if os.path.isdir(timeframe_path):
                    files = list(find_files_in_timeframe(timeframe_path))
                    dates = [parse_date_from_filename(file) for file in files]
                    dates = [date for date in dates if date is not None]
                    if dates:
                        dates.sort()
                        start_date = dates[0]
                        end_date = dates[-1]
                        current_date = start_date
                        while current_date <= end_date:
                            year_month = current_date.strftime('%Y%m')
                            expected_filename = f"{ticker}-{year_month}.csv.gz"
                            if not any(file.startswith(expected_filename.split('-')[0]) for file in files if parse_date_from_filename(file) == current_date):
                                print(f"Missing file in {ticker}/{timeframe}: {expected_filename}")
                                download_url = construct_download_url(base_url, ticker, timeframe, year_month)
                                print(f"Download URL: {download_url}")
                            current_date += timedelta(days=31)
                            current_date = datetime(current_date.year, current_date.month, 1)  # Normalize to first of month

# Use the specified working folder 'data/gateio'.
base_url = "https://download.gatedata.org"
find_missing_files('data/gateio', base_url)
