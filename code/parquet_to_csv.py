import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# 1. Get the path from your .env file
output_dir = os.getenv("NHANES_OUTPUT_DIR")
parquet_path = f"{output_dir}.parquet" # Construct the filename

print(f"Reading: {parquet_path}...")
df = pd.read_parquet(parquet_path)

# 2. Export to CSV (readable by Excel)
csv_path = f"{output_dir}.csv"
print(f"\nSaving to CSV: {csv_path}...")
df.to_csv(csv_path, index=False)

print("Done!")