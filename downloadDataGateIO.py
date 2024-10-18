"""
Freqtrade Data Downloader Script

This script automates the process of downloading candlestick data using freqtrade from Gate.io, 
adhering to the API's limitation of not exceeding 10,000 candlesticks per request. It supports 
flexible timeframes specified in formats like 1m (minute), 4h (hour), 1d (day), 1w (week), and 1M (month), 
and calculates the necessary segments to download the entire requested dataset within these constraints.

Functionality:
- Parses the user-specified timeframe and calculates the corresponding duration in minutes.
- Dynamically calculates the date segments to ensure each data download request does not exceed 10,000 candlesticks.
- Executes the freqtrade download-data command for each calculated segment, allowing for complete data retrieval without hitting API limits.

Input Variables:
- start_date_str: The start date for data download in 'YYYYMMDD' format (e.g., '20180101').
- end_date_str: The end date for data download in 'YYYYMMDD' format (e.g., '20240117'). Ensure this date is not beyond the current date.
- config_file: Path to the freqtrade configuration file (e.g., 'user_data/GateIO.json').
- timeframe_str: Desired timeframe for the data in specific formats (e.g., '1m', '4h', '1d', '1w', '1M').

Output:
- The script does not return data but executes the freqtrade command-line tool to download and save the data as specified in the freqtrade configuration.

Prerequisites:
- Python 3.6 or higher.
- Freqtrade installed and accessible in the environment from which this script is run.
- A valid Gate.io configuration file for freqtrade.

Usage:
- Adjust the 'start_date_str', 'end_date_str', 'config_file', and 'timeframe_str' variables as needed.
- Run the script in a Python environment where freqtrade is installed and accessible.

Note:
- The script assumes that the API's rate limiting is handled internally by freqtrade.
- The month duration is approximated as 30.5 days for simplicity in calculations.
"""

import subprocess
from datetime import datetime, timedelta

def parse_timeframe(timeframe_str):
    """Parse the timeframe string and return the timeframe in minutes."""
    units = {'m': 1, 'h': 60, 'd': 1440, 'w': 10080, 'M': 43800}  # Approximate month as 30.5 days
    unit = timeframe_str[-1]
    if unit not in units:
        raise ValueError(f"Unsupported timeframe unit: {unit}")
    try:
        quantity = int(timeframe_str[:-1])
        return quantity * units[unit]
    except ValueError:
        raise ValueError(f"Invalid timeframe format: {timeframe_str}")

def calculate_segments(start_date_str, end_date_str, timeframe_minutes):
    """Calculate time segments for downloading data based on the given timeframe."""
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    candlesticks_per_day = 1440 / timeframe_minutes
    max_days_per_segment = 10000 / candlesticks_per_day

    segments = []
    current_start_date = start_date
    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=max_days_per_segment - 1), end_date)
        segments.append((current_start_date.strftime("%Y%m%d"), current_end_date.strftime("%Y%m%d")))
        current_start_date = current_end_date + timedelta(days=1)
    
    return segments

def download_data(segment, config_file, timeframe_str):
    """Download data for a given segment."""
    command = f"freqtrade download-data --trading-mode spot -c {config_file} -t {timeframe_str} --timerange {segment[0]}-{segment[1]}"
    print(f"Executing: {command}")
    subprocess.run(command, shell=True)

def main():
    start_date_str = "20180101"
    end_date_str = "20240117"
    config_file = "user_data/GateIO.json"
    timeframe_str = "15m"  # Example format: 1m, 4h, 1d, 1w, 1M
    
    # Convert timeframe to minutes for segment calculation
    try:
        timeframe_minutes = parse_timeframe(timeframe_str)
    except ValueError as e:
        print(e)
        return

    segments = calculate_segments(start_date_str, end_date_str, timeframe_minutes)
    for segment in segments:
        download_data(segment, config_file, timeframe_str)

if __name__ == "__main__":
    main()
