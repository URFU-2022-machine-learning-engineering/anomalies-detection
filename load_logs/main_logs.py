from pathlib import Path

import requests
import datetime
import pandas as pd
import json
from yaml import safe_load


def get_unix_timestamp_ns(dt):
    """Convert a datetime object to Unix timestamp in nanoseconds."""
    return int(dt.timestamp() * 1e9)


def get_config(path: Path | str) -> dict:
    """Load the configuration from a config file."""
    with open(path) as file:
        config = safe_load(file)
    return config


parent_path = Path(__file__).parents[1]
config_path = parent_path / "config.yml"
server = get_config(config_path)["loki"]


def fetch_logs_for_hour(start_hour):
    """Fetch logs for a specific hour."""
    params = {
        "query": "{container_name=\"/sr-api\"} |= `` | json | __error__=``",
        "start": get_unix_timestamp_ns(start_hour),
        "end": get_unix_timestamp_ns(start_hour + datetime.timedelta(hours=1)),
        "limit": 5000,
    }
    response = requests.get(loki_url, params=params)
    if response.status_code == 200:
        logs = response.json()
        print(logs)
        return [entry[1] for result in logs['data']['result'] for entry in result['values']]
    else:
        print(f"Failed to fetch logs for hour starting at {start_hour}. Status code: {response.status_code}")
        return []


# Setup
end_timestamp = datetime.datetime.now(datetime.timezone.utc)
start_timestamp = end_timestamp - datetime.timedelta(days=1)
loki_url = server

# Collect logs
all_log_entries = []
current_hour = start_timestamp
epoch = 0
while current_hour < end_timestamp:
    print(f"Fetching logs for {current_hour}")
    epoch += 1
    print(f"Epoch: {epoch}")
    hour_log_entries = fetch_logs_for_hour(current_hour)
    print(f"Number of logs fetched: {len(hour_log_entries)}")
    all_log_entries.extend(hour_log_entries)
    current_hour += datetime.timedelta(hours=1)


# Handle logs here
def parse_log_entry(log_entry):
    """Parse a log entry and prepare it for DataFrame insertion."""
    entry_dict = json.loads(log_entry)
    parsed_entry = {
        "level": entry_dict.get("level", ""),
        "message": entry_dict.get("message", ""),
        "time": entry_dict.get("time", 0)
    }

    # Handle additional fields
    for key, value in entry_dict.items():
        if key not in ["level", "message", "time"]:
            parsed_entry[f"add_{key}"] = value

    return parsed_entry


# Parse each log entry
parsed_log_entries = [parse_log_entry(log) for log in all_log_entries]

# Create a DataFrame
df_logs = pd.DataFrame(parsed_log_entries)

# Set 'time' as the index of the DataFrame and convert to datetime
df_logs.set_index('time', inplace=True)
df_logs.index = pd.to_datetime(df_logs.index, unit='ms')

print(df_logs.head())  # Display the first few rows of the DataFrame
