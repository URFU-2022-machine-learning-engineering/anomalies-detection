import os
import pandas as pd


def process_spans_data(directory_path):
    """
    Iterates over subfolders within the given directory, finds CSV files,
    concatenates them into a single DataFrame, converts 'startTime' to datetime,
    removes duplicates based on 'spanID', and drops empty columns.

    Parameters:
    - directory_path: Path to the directory containing subfolders with CSV files.

    Returns:
    - DataFrame: The processed spans data.
    """
    all_dataframes = []  # List to hold data from all CSV files

    # Iterate over subfolders and files within the given directory
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, low_memory=False)
                try:
                    df['startTime'] = pd.to_datetime(df['startTime'])
                except Exception as e:
                    print(f"Error converting startTime to datetime in file {file_path}: {e}")
                all_dataframes.append(df)

    if all_dataframes:
        concatenated_df = pd.concat(all_dataframes, ignore_index=True)
        # Remove duplicates based on 'spanID'
        unique_df = concatenated_df.drop_duplicates(subset=['spanID'])
        # Drop empty columns
        cleaned_df = unique_df.dropna(axis=1, how='all')
        return cleaned_df
    else:
        print("No CSV files found in the directory.")
        return pd.DataFrame()


def enrich_spans_with_logs(spans_df, logs_file_path):
    """
    Enriches spans data with logs based on matching 'spanID'.

    Parameters:
    - spans_df: DataFrame containing the spans data.
    - logs_file_path: Path to the logs.csv file used for enriching the data.

    Returns:
    - DataFrame: The enriched spans data.
    """
    # Load the logs data
    logs_df = pd.read_csv(logs_file_path, low_memory=False)
    # Rename 'span_id' to 'spanID' to match the spans DataFrame
    logs_df.rename(columns={'span_id': 'spanID'}, inplace=True)
    # Merge with logs data on 'spanID'
    enriched_df = pd.merge(spans_df, logs_df, on='spanID', how='left')
    return enriched_df


if __name__ == "__main__":
    spans_df = process_spans_data("data/backups")
    enriched_df = enrich_spans_with_logs(spans_df, "data/logs/logs.csv")
    enriched_df.to_csv("enriched_spans.csv", index=False)
