import pandas as pd
import sqlite3

# Connect to the database
conn = sqlite3.connect('contracts.db')

# Read the data
df = pd.read_sql('SELECT * FROM contracts LIMIT 5', conn)

# Print info about the dataframe
print("DataFrame Info:\n")
df.info()

print("\nFirst few rows:\n")
print(df.head())

# Close the connection
conn.close() 