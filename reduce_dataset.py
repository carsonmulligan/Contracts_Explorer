import pandas as pd
import numpy as np
import os
import glob
from pathlib import Path

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

# List of columns we want to keep
COLUMNS_TO_KEEP = [
    'contract_transaction_unique_key',
    'recipient_name',
    'awarding_agency_name',
    'current_total_value_of_award',
    'potential_total_value_of_award',
    'action_date',
    'period_of_performance_current_end_date',
    'transaction_description',
    'prime_award_base_transaction_description',
    'recipient_duns',
    'awarding_agency_code',
    'awarding_sub_agency_name',
    'award_type',
    'naics_code',
    'naics_description'
]

def process_csv(file_path):
    print(f"\nProcessing {file_path}...")
    try:
        # Read only the columns we need with more flexible date parsing
        df = pd.read_csv(file_path, usecols=COLUMNS_TO_KEEP, low_memory=False)
        
        # Handle date columns more carefully
        date_columns = ['action_date', 'period_of_performance_current_end_date']
        for col in date_columns:
            # Convert to datetime with coerce to handle invalid dates
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns
        df['current_total_value_of_award'] = pd.to_numeric(df['current_total_value_of_award'], errors='coerce')
        df['potential_total_value_of_award'] = pd.to_numeric(df['potential_total_value_of_award'], errors='coerce')
        
        print(f"Found {len(df)} contracts in file")
        return df
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None

# Find all CSV files
csv_files = []
for pattern in ['FY20*_All_Contracts_Full_*/*.csv', 'FY*_All_Contracts_Full_*.csv']:
    csv_files.extend(glob.glob(pattern))

print(f"Found {len(csv_files)} CSV files to process")

# Process all CSV files
all_data = []
for file in csv_files:
    df = process_csv(file)
    if df is not None and len(df) > 0:
        all_data.append(df)

if not all_data:
    print("No valid data found!")
    exit(1)

# Combine all dataframes
print("\nCombining all data...")
combined_df = pd.concat(all_data, ignore_index=True)
print(f"Total contracts found: {len(combined_df)}")

# Clean the data
print("\nCleaning data...")
# Remove duplicates based on unique key
combined_df = combined_df.drop_duplicates(subset='contract_transaction_unique_key')
print(f"Contracts after removing duplicates: {len(combined_df)}")

# Drop rows with any null values in critical columns
critical_columns = [
    'contract_transaction_unique_key',
    'recipient_name',
    'awarding_agency_name',
    'current_total_value_of_award',
    'action_date'
]
combined_df = combined_df.dropna(subset=critical_columns)
print(f"Contracts after removing null values in critical columns: {len(combined_df)}")

# Remove contracts with zero or negative values
combined_df = combined_df[combined_df['current_total_value_of_award'] > 0]
print(f"Contracts after removing zero/negative values: {len(combined_df)}")

# Sort by contract value and take top contracts plus a random sample of smaller ones
print("\nSampling data...")
threshold = combined_df['current_total_value_of_award'].quantile(0.8)  # Top 20% by value
large_contracts = combined_df[combined_df['current_total_value_of_award'] >= threshold]
small_contracts = combined_df[combined_df['current_total_value_of_award'] < threshold].sample(
    n=min(len(large_contracts), len(combined_df))
)
df_sampled = pd.concat([large_contracts, small_contracts])

# Final cleaning of the sampled dataset
df_sampled = df_sampled.fillna({
    'transaction_description': 'No description available',
    'prime_award_base_transaction_description': 'No base description available',
    'naics_description': 'Unclassified'
})

# Save as parquet
print("Saving as parquet...")
df_sampled.to_parquet('contracts_sample.parquet', compression='gzip')

# Save as CSV for backup
print("Saving as CSV...")
df_sampled.to_csv('contracts_sample.csv', index=False)

# Print size information
print("\nSize information:")
print(f"Total CSV files processed: {len(csv_files)}")
print(f"Original total contracts: {len(combined_df)}")
print(f"Sampled contracts: {len(df_sampled)}")
print(f"Parquet file size: {sizeof_fmt(os.path.getsize('contracts_sample.parquet'))}")
print(f"CSV file size: {sizeof_fmt(os.path.getsize('contracts_sample.csv'))}")

# Print value ranges
print("\nValue ranges in sample:")
print(f"Min contract value: ${df_sampled['current_total_value_of_award'].min():,.2f}")
print(f"Max contract value: ${df_sampled['current_total_value_of_award'].max():,.2f}")
print(f"Mean contract value: ${df_sampled['current_total_value_of_award'].mean():,.2f}")

# Print date ranges
print("\nDate ranges in sample:")
print(f"Earliest action date: {df_sampled['action_date'].min():%Y-%m-%d}")
print(f"Latest action date: {df_sampled['action_date'].max():%Y-%m-%d}")
if not df_sampled['period_of_performance_current_end_date'].isna().all():
    print(f"Earliest end date: {df_sampled['period_of_performance_current_end_date'].min():%Y-%m-%d}")
    print(f"Latest end date: {df_sampled['period_of_performance_current_end_date'].max():%Y-%m-%d}")

print("Done!") 