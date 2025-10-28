# ============================================================
# Mini Project 1: Basic Full Pipeline (Single Script Version)
# Author: Arul
# ============================================================

import os
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

# ============================================================
# 1. PREPARE FOLDERS 
# ============================================================
data_folder = "data"
output_folder = "output"
logs_folder = "logs"

os.makedirs(output_folder, exist_ok=True)
os.makedirs(logs_folder, exist_ok=True)

# ============================================================
# 2. LOAD & VALIDATE FILES 
# ============================================================
def log_info(message):
    print(f"\033[94m[INFO]\033[0m {message}]]")
    with open (os.path.join(logs_folder, "pipeLine.log.text"), "a") as log_file:
        log_file.write(f"{message} \n")

# ============================================================
# 3. LOAD & VALIDATE FILES 
# ============================================================
try: 
    all_files = [file for file in os.listdir(data_folder) if file.endswith(".csv")]
    if not all_files :
        raise FileNotFoundError("No CSV file found in data folder.")
    
    dataframes = []
    for file in all_files:
        file_path = os.path.join(data_folder, file)
        log_info(f"Loading {file}")
        df = pd.read_csv(file_path)
        
        required_cols = ['Date', 'Product', 'Price', 'Quantity', 'Revenue']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            log_info(f"Skipping {file} (missing columns: {missing_cols})")
            continue
        
        dataframes.append(df)

    merge_df = pd.concat(dataframes, ignore_index=True)
    log_info(f"Total merged rows: {len(merge_df)} \n")
    log_info(f"Loaded {len(all_files)} file successfully")

except Exception as error:
    log_info(f"Error loading files: {error}")
    exit()

# ============================================================
# 4. Handle Missing Values
# ============================================================
log_info("Handling Missing Values ...")
missing_before = merge_df.isna().sum().sum()
for col in merge_df.columns:
    if merge_df[col].dtype == 'object':
        mode_val = merge_df[col].mode()[0]
        merge_df[col] = merge_df[col].fillna(mode_val)
    else :
        mean_val = merge_df[col].mean()
        merge_df[col] = merge_df[col].fillna(mean_val)
missing_after = merge_df.isna().sum().sum()
log_info(f"Missing value handled: {missing_before - missing_after} filled")

# ============================================================
# 5. Handle Duplicates
# ============================================================
before = len(merge_df)
merge_df.drop_duplicates(inplace=True)
after = len(merge_df)
log_info(f"Duplicates removed: {before - after}")

# ============================================================
# 6. Detect Outliers and Anomalies
# ============================================================
log_info("Handling outliers and anomalies ...")

numeric_cols = merge_df.select_dtypes(include=['int64', 'float64']).columns

# --- IQR Method ---
for col in numeric_cols:
    Q1 = merge_df[col].quantile(0.25)
    Q3 = merge_df[col].quantile(0.75)
    IQR = Q3 - Q1
    
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    merge_df[col] = np.where(merge_df[col] < lower, lower, np.where(merge_df[col] >  upper, upper, merge_df[col]))

# --- Isolation Forest for Anomaly Flag
iso = IsolationForest(contamination=0.02, random_state=42)
merge_df['anomaly_flag'] = iso.fit_predict(merge_df[numeric_cols])


# ============================================================
# 7. DATA TYPE CONVERSION
# ============================================================
log_info("Converting data types ...")
merge_df['Date'] = pd.to_datetime(merge_df['Date'], errors = 'coerce')
for col in ['Price', 'Quantity', 'Revenue']:
    merge_df[col] = pd.to_numeric(merge_df[col], errors = 'coerce')

# ============================================================
# 8. NORMALIZATION (0-1)
# ============================================================
log_info("Normalizing numeric colums ...")
scaler = MinMaxScaler()
merge_df[numeric_cols] = scaler.fit_transform(merge_df[numeric_cols])

# ============================================================
# 9. SAVE OUTPUT & SUMMARY
# ============================================================
output_file  = os.path.join(output_folder, "final_cleaned.csv")
merge_df.to_csv(output_file, index=False)

summary_file = os.path.join(output_folder, "final_summary.txt")
with open(summary_file, 'w') as file:
    file.write("=== Pipeline Summary === \n")
    file.write(f"Total files processed: {len(all_files)} \n")
    file.write(f"Total rows merged: {len(merge_df)} \n")
    file.write(f"Duplicate Removed: {before - after} \n")
    file.write(f"Anomalies detected: {(merge_df['anomaly_flag'] == -1).sum()} \n")
    file.write("Normalization: Min-Max Scaling (0-1) \n")
    file.write("Pipeline Status: SUCCESS \n")

log_info("Pipeline complete succesfully")