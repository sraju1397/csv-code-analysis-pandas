import pandas as pd
import os
from datetime import datetime
import re


def parse_date_from_filename(file_name):
    # Extract date from filename
    match = re.search(r'(\d{2})(\d{2})(\d{4})\.csv$', file_name)
    if match:
        day, month, year = match.groups()
        return datetime(int(year), int(month), int(day))
    return None


def filter_ticker(df_filter, file_name):
    # Check if required columns exist
    required_columns = ['Ticker', 'Time', 'Date']
    missing_columns = [col for col in required_columns if col not in df_filter.columns]
    if missing_columns:
        print(f"Warning: Missing columns in {file_name}: {', '.join(missing_columns)}")
        return None

    # Filter and process the dataframe
    df_filter = df_filter[
        df_filter['Ticker'].str.startswith('BANKNIFTY') &
        ~df_filter['Ticker'].str.contains('CE', na=False) &
        ~df_filter['Ticker'].str.contains('PE', na=False)
        ]

    # Group by Date and Time
    grouped = df_filter.groupby(['Date', 'Time'])

    # Process each group
    result_rows = []
    for _, group in grouped:
        # Sum Volume and Open Interest
        total_volume = group['Volume'].sum()
        total_open_interest = group['Open Interest'].sum()

        # Select the row with the highest Volume
        max_volume_row = group.loc[group['Volume'].idxmax()].copy()

        # Update Volume and Open Interest with the summed values
        max_volume_row['Volume'] = total_volume
        max_volume_row['Open Interest'] = total_open_interest

        result_rows.append(max_volume_row)

    # Combine results into a new DataFrame
    result_df = pd.DataFrame(result_rows)

    # Sort the result
    result_df = result_df.sort_values(by=['Date', 'Time', 'Ticker'])

    # Add filename column
    result_df['Filename'] = filename

    return result_df


# Main script
path = 'data/'
output_path = 'output'
output_file = os.path.join(output_path, 'out.csv')
processed_files_log = os.path.join(output_path, 'processed_files.txt')

# Ensure the output directory exists
os.makedirs(output_path, exist_ok=True)

# Get the set of already processed files
processed_files = set()
if os.path.exists(processed_files_log):
    with open(processed_files_log, 'r') as f:
        processed_files = set(f.read().splitlines())

# Collect all CSV files with their dates
all_files = []
for root, _, files in os.walk(path):
    for file in files:
        if file.endswith('.csv'):
            file_date = parse_date_from_filename(file)
            if file_date:
                all_files.append((os.path.join(root, file), file_date))

# Sort files by date, newest first
all_files.sort(key=lambda x: x[1], reverse=True)

# Process files
newly_processed_files = []
for file_path, file_date in all_files:
    filename = os.path.basename(file_path)
    if filename in processed_files:
        print(f"Skipping {file_path} (already processed)")
        continue

    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        df_modified = filter_ticker(df, filename)

        if df_modified is not None and not df_modified.empty:
            # Append the modified data to the output CSV
            df_modified.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False,
                               encoding='utf-8')
            print(f"File processed: {file_path}")
            newly_processed_files.append(filename)
        else:
            print(f"No data to write after filtering: {file_path}")
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

# Update the processed files log
with open(processed_files_log, 'a') as f:
    for filename in newly_processed_files:
        f.write(f"{filename}\n")

print("All files processed. Updated output file saved.")
