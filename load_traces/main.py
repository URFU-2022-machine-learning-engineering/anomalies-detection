from datetime import datetime

import pandas as pd
import shutil
import os
from pathlib import Path
from utils.date_utils import get_date_strings
from utils.fetch_jaeger_traces import fetch_jaeger_traces
from utils.convert_tag_value import convert_tag_value

service_name = "sr-api"
start_date_str, end_date_str = get_date_strings()
limit = 25000
operation = "/upload"  # Directly pass the operation name
parent_path = Path(__file__).parents[1]
data_dir = parent_path / "data"
backup_dir = data_dir / "backups" / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

username = os.getenv("JAEGER_USERNAME")
password = os.getenv("JAEGER_PASSWORD")

server = "http://192.168.111.66:16686/api/traces"


def get_most_recent_file(directory, pattern):
    """Get the most recent file matching the pattern in the given directory."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


# Check for the most recent CSV and Parquet files
most_recent_csv = get_most_recent_file(data_dir, "*.csv")
most_recent_parquet = get_most_recent_file(data_dir, "*.parquet")

# Load the most recent data if available
if most_recent_csv and most_recent_parquet:
    previous_data_df = pd.read_parquet(most_recent_parquet)
else:
    previous_data_df = pd.DataFrame()

traces = fetch_jaeger_traces(
    server=server,
    service_name=service_name,
    start_date=start_date_str,
    end_date=end_date_str, limit=limit, operation=operation,
    username=username,
    password=password
)

# Simplified version of the code
spans_list = [
    {
        **{
            'spanID': span.get('spanID'),
            'traceID': span.get('traceID'),
            'operationName': span.get('operationName'),
            'serviceName': span.get('process', {}).get('serviceName'),
            'startTime': span.get('startTime'),
            'duration': span.get('duration')
        },
        **{
            'tag_' + tag.get('key'): convert_tag_value(tag.get('value'), tag.get('type', 'string'))
            for tag in span.get('tags', [])
        }
    }
    for trace in traces.get('data', [])
    for span in trace.get('spans', [])
]

# Create a DataFrame
spans_df = pd.DataFrame(spans_list)

# Optional: Convert startTime to a readable datetime format
spans_df['startTime'] = pd.to_datetime(spans_df['startTime'], unit='us')

# Define file paths for daily and full (cumulative) backups
date_suffix = datetime.now().strftime("%Y-%m-%d")
daily_csv_filename = f"{data_dir}/daily_upload_spans_{date_suffix}.csv"
daily_parquet_filename = f"{data_dir}/daily_upload_spans_{date_suffix}.parquet"
full_csv_filename = f"{data_dir}/full_upload_spans.csv"
full_parquet_filename = f"{data_dir}/full_upload_spans.parquet"

# Save today's data (daily backup) without affecting previous daily backups
spans_df.to_csv(path_or_buf=daily_csv_filename, index=False)
spans_df.to_parquet(path=daily_parquet_filename, index=False)

# For full data backup: Check if a full backup exists, load it, append new data, and save
if os.path.exists(full_csv_filename) and os.path.exists(full_parquet_filename):
    # Load existing full data
    full_data_df = pd.read_parquet(full_parquet_filename)
else:
    # Initialize empty DataFrame if full data doesn't exist yet
    full_data_df = pd.DataFrame()

# Append today's data to the full data DataFrame
full_data_combined_df = pd.concat([full_data_df, spans_df], ignore_index=True)

# Save the updated full data (cumulative backup)
full_data_combined_df.to_csv(path_or_buf=full_csv_filename, index=False)
full_data_combined_df.to_parquet(path=full_parquet_filename, index=False)


backup_dir.mkdir(parents=True, exist_ok=True)

if most_recent_csv:
    shutil.move(most_recent_csv, backup_dir / most_recent_csv.name)
if most_recent_parquet:
    shutil.move(most_recent_parquet, backup_dir / most_recent_parquet.name)

