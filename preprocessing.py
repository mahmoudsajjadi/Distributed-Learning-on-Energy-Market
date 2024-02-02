import pandas as pd
import gc

del gc.garbage[:]

data_path = 'Datasets/PECAN_St/1minute_data_california/1minute_data_california.csv'
chunk_size = 100000  # Adjust the chunk size according to your memory constraints

chunks = pd.read_csv(data_path, chunksize=chunk_size)

total_rows = 0
total_chunks = 0
unique_homes_id = set()
dfs_by_home_id = {}

for chunk in chunks:
    total_chunks += 1
    print(total_chunks)
    total_rows += len(chunk)
    
    for home_id in chunk['dataid'].unique():
        home_data = chunk[chunk['dataid'] == home_id]
        
        if home_id in dfs_by_home_id:
            dfs_by_home_id[home_id] = pd.concat([dfs_by_home_id[home_id], home_data])
        else:
            dfs_by_home_id[home_id] = home_data

# Process each dataframe to add total_consumption column and select required columns
for home_id, df in dfs_by_home_id.items():
    print(f'home_id: {home_id}')
    df['total_consumption'] = df['grid'].fillna(0) + df['solar'].fillna(0) + df['solar2'].fillna(0) + df['battery1'].fillna(0)
    dfs_by_home_id[home_id] = df[['dataid', 'localminute', 'leg1v', 'leg2v', 'total_consumption']]
    dfs_by_home_id[home_id].sort_values(by='localminute')

# Print information for each home
for home_id, df in dfs_by_home_id.items():
    print(f"Home ID: {home_id}, Number of rows: {len(df)}")

print("Total number of chunks:", total_chunks)
print("Total number of rows:", total_rows)
print("Unique dataids:", unique_homes_id)
print("Total number of unique dataids:", len(unique_homes_id))
