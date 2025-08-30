import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random

# Config
n_rows = 100000  # number of events to generate
n_users = 50   # number of unique users
n_tracks = 100 # number of unique tracks

# Generate random data
user_ids = np.random.randint(1, n_users + 1, size=n_rows)
track_ids = np.random.randint(1, n_tracks + 1, size=n_rows)

# Generate random timestamps within last 30 days
end_time = datetime.now()
start_time = end_time - timedelta(days=30)
timestamps = [
    int((start_time + timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))).timestamp() * 1000)
    for _ in range(n_rows)
]

# Create DataFrame
df = pd.DataFrame({
    "user_id": user_ids,
    "track_id": track_ids,
    "timestamp": timestamps
})

# Save to Parquet
df.to_parquet("history/30/1/user_listening_history.parquet", engine="pyarrow", index=False)

print("Parquet file 'user_listening_history.parquet' generated with", n_rows, "rows.")
