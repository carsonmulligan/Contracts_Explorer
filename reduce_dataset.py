import pandas as pd
import numpy as np
import os

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

# List of columns we want to keep (these are the most important ones for our app)
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

# Read the CSV file
print("Reading CSV file...")
df = pd.read_csv('FY2025_All_Contracts_Full_20250107_1.csv', usecols=COLUMNS_TO_KEEP)

# Convert date columns to datetime
date_columns = ['action_date', 'period_of_performance_current_end_date']
for col in date_columns:
    df[col] = pd.to_datetime(df[col])

# Filter for contracts ending after Feb 1st 2025
print("Filtering data...")
df = df[df['period_of_performance_current_end_date'] >= '2025-02-01']

# Sort by contract value and take top contracts plus a random sample of smaller ones
print("Sampling data...")
threshold = df['current_total_value_of_award'].quantile(0.8)  # Top 20% by value
large_contracts = df[df['current_total_value_of_award'] >= threshold]
small_contracts = df[df['current_total_value_of_award'] < threshold].sample(n=min(len(large_contracts), len(df)))
df_sampled = pd.concat([large_contracts, small_contracts])

# Save as parquet
print("Saving as parquet...")
df_sampled.to_parquet('contracts_sample.parquet', compression='gzip')

# Save as CSV for backup
print("Saving as CSV...")
df_sampled.to_csv('contracts_sample.csv', index=False)

# Print size information
print("\nSize information:")
print(f"Number of original rows: {len(df)}")
print(f"Number of sampled rows: {len(df_sampled)}")
print(f"Parquet file size: {sizeof_fmt(os.path.getsize('contracts_sample.parquet'))}")
print(f"CSV file size: {sizeof_fmt(os.path.getsize('contracts_sample.csv'))}")
print("Done!") 