import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Config
n_rows = 1000  # number of events to generate
n_users = 50   # number of unique users
n_tracks = 200 # number of unique tracks

# Generate random data
user_ids = np.random.randint(1, n_users + 1, size=n_rows)
track_ids = np.random.randint(1, n_tracks + 1, size=n_rows)

# Generate random timestamps within last 30 days
end_time = datetime.now()
start_time = end_time - timedelta(days=30)
timestamps = [
    start_time + timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))
    for _ in range(n_rows)
]

# Create DataFrame
df = pd.DataFrame({
    "user_id": user_ids,
    "track_id": track_ids,
    "timestamp": timestamps
})

# Save to Parquet
df.to_parquet("30/1/user_listening_history.parquet", engine="pyarrow", index=False)

print("Parquet file 'user_listening_history.parquet' generated with", n_rows, "rows.")
