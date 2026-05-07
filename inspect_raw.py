"""
inspect_raw.py — Quick diagnostic on raw_leads.csv before building cleaner.py.
Run once, paste output, then delete this file.
"""

import pandas as pd

df = pd.read_csv("data/raw/raw_leads.csv")

print("=" * 60)
print(f"SHAPE: {df.shape[0]} rows x {df.shape[1]} columns")

print("\nMISSING VALUES:")
print(df.isnull().sum().to_string())

print("\nEMPTY STRINGS PER COLUMN:")
for col in df.columns:
    empty = df[col].astype(str).str.strip().eq("").sum()
    if empty > 0:
        print(f"  {col}: {empty} empty")

print("\nREVIEW_COUNT — first 15 raw values:")
print(df["review_count"].head(15).tolist())

print("\nREVIEW_COUNT dtype:", df["review_count"].dtype)

print("\nRATING — first 15 raw values:")
print(df["rating"].head(15).tolist())

print("\nRATING dtype:", df["rating"].dtype)

print("\nWEBSITE — first 5 values:")
for i, val in enumerate(df["website"].head(5).tolist()):
    print(f"  [{i}] {val}")

print("\nMAPS_URL — first 3 values:")
for i, val in enumerate(df["maps_url"].head(3).tolist()):
    print(f"  [{i}] {val}")

print("\nROWS WITH MISSING WEBSITE:")
missing_web = df["website"].isnull() | df["website"].astype(str).str.strip().eq("")
print(f"  {missing_web.sum()} / {len(df)}")

print("\nROWS WITH MISSING RATING:")
missing_rating = df["rating"].isnull() | df["rating"].astype(str).str.strip().eq("")
print(f"  {missing_rating.sum()} / {len(df)}")

print("\nROWS WITH MISSING REVIEW_COUNT:")
missing_rev = df["review_count"].isnull() | df["review_count"].astype(str).str.strip().eq("")
print(f"  {missing_rev.sum()} / {len(df)}")

print("\nSAMPLE ROW 0 (full):")
print(df.iloc[0].to_dict())

print("\nSAMPLE ROW 1 (full):")
print(df.iloc[1].to_dict())

print("=" * 60)