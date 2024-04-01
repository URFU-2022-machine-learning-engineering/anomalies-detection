from pathlib import Path
import pandas as pd
import requests
import json
import datetime
import time
import logging
import subprocess
from typing import Any

# Setup logging
logging.basicConfig(filename='log_fetcher.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def get_unix_timestamp_ns(dt: datetime.datetime) -> int:
    """Convert a datetime object to Unix timestamp in nanoseconds."""
    return int(dt.timestamp() * 1e9)


def fetch_logs(start_timestamp: datetime.datetime, end_timestamp: datetime.datetime) -> list[str]:
    """Fetch logs from Loki within the specified timestamp range."""
    loki_url = "http://192.168.111.66:3100/loki/api/v1/query_range"
    params = {
        "query": "{container_name=\"/sr-api\"} |= `` | json | __error__=``",
        "start": get_unix_timestamp_ns(start_timestamp),
        "end": get_unix_timestamp_ns(end_timestamp),
        "limit": 5000,
    }
    response = requests.get(loki_url, params=params)
    if response.status_code == 200:
        logging.info("Logs fetched successfully.")
        logs = response.json()
        return [entry[1] for result in logs['data']['result'] for entry in result['values']]
    else:
        logging.error(f"Failed to fetch logs. Status code: {response.status_code}")
        return []


def parse_logs_to_dataframe(log_entries: list[str]) -> pd.DataFrame:
    """Parse log entries and return a DataFrame."""
    parsed_log_entries: list[dict[str, Any]] = []
    for log_entry in log_entries:
        entry_dict = json.loads(log_entry)
        # Extract common fields and any additional ones
        parsed_entry = {**entry_dict, "time": pd.to_datetime(entry_dict["time"], unit='ms')}
        parsed_log_entries.append(parsed_entry)

    if parsed_log_entries:
        logging.info("Log entries parsed successfully.")
    else:
        logging.warning("No log entries to parse.")

    return pd.DataFrame(parsed_log_entries)


def dvc_add_and_push():
    """Add and push the given file to DVC remote storage."""
    try:
        subprocess.run(['dvc', 'add', "data/"], check=True)
        subprocess.run(['git', 'add', "data.dvc"], check=True)
        subprocess.run(['git', 'commit', '-m', 'Update logs'], check=True)
        subprocess.run(['dvc', 'push'], check=True)
        logging.info(f"Logs added and pushed to DVC successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to add or push logs to DVC. Error: {e}")


# Directory setup
parent_path = Path(__file__).resolve().parents[1]
data_dir = parent_path / "data"
logs_dir = data_dir / "logs"
logs_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
df_name = "logs.csv"

# Initialize an empty DataFrame to store logs
df_logs: pd.DataFrame = pd.DataFrame()

try:
    while True:
        logging.info("Script execution started.")

        # Define the time range for fetching logs
        end_timestamp: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
        start_timestamp: datetime.datetime = end_timestamp - datetime.timedelta(minutes=30)  # Last 30 minutes

        log_entries: list[str] = fetch_logs(start_timestamp, end_timestamp)
        new_logs_df: pd.DataFrame = parse_logs_to_dataframe(log_entries)

        df_logs = pd.concat([df_logs, new_logs_df], ignore_index=True).drop_duplicates(subset=['time'])

        # Save DataFrame to CSV
        csv_path = logs_dir / df_name
        df_logs.to_csv(csv_path, index=False)
        logging.info(f"Logs updated and saved at {csv_path}.")

        # DVC Add and Push
        dvc_add_and_push()

        logging.info("Sleeping for 30 minutes...")
        time.sleep(1800)  # Sleep for 30 minutes
except KeyboardInterrupt:
    logging.info("Script terminated by user.")
except Exception as e:
    logging.exception(f"An error occurred: {e}")
