# ============================================================
# Mini Project 1: Modular Full Pipeline - Function-Based Version
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
# 2. DEFINE HELPER FUNCTION FOR LOGGING
# ============================================================
def log_info (message) :
    print(f"\033[94m[INFO]\033[0m {message}")

    with open(os.path.join(logs_folder, "pipeline.log.txt"), "a") as log_file:
        log_file.write(f"{message}\n")

# ============================================================
# 3. LOAD & VALIDATE FILES 
# ============================================================

def load_files(folder_path) :
    
    try :
        all_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]
        if not all_files:
            raise FileNotFoundError("No CSV file found in data folder")
        
    
        dataframes =[]
        for file in all_files:
            file_path = os.path.join(folder_path, file)
            log_info(f"Loading {file}")
            df = pd.read_csv(file_path)    

            required_col = ['Date', 'Product', 'Price', 'Quantity', 'Revenue']
            missing_col = [col for col in required_col if col not in df.columns]
            if missing_col:
                log_info(f"Skipping {file} (missing columns: {missing_col})")
                continue

            dataframes.append(df)

        merge_df = pd.concat(dataframes, ignore_index=True)
        log_info(f"Total merged rows: {len(merge_df)} \n")
        log_info(f"Loaded {len(all_files)} files successfully")
        return merge_df
    
    except Exception as error :
        log_info(f"Error loading files: {error}")
        exit()
        

# ============================================================
# 4. HANDLE MISSING VALUE
# ============================================================

def handle_missing(df) :
    missing_before = df.isna().sum().sum()
    for col in df.columns:
        if df[col].dtype == 'object':
            mode_val = df[col].mode()[0]
            df[col] = df[col].fillna(mode_val)

        else:
            mean_val = df[col].mean()
            df[col] = df[col].fillna(mean_val) 
    missing_after = df.isna().sum().sum()
    log_info(f"Missing value handled: {missing_before - missing_after} filled")
    return df

# ============================================================
# 5. HANDLE DUPLICATES
# ============================================================
def handle_duplicates(df):
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)
    remove = before - after
    log_info(f"Handle duplicates: {remove}")
    return df, remove

# ============================================================
# 6. DETECT OUTLIERS AND ANOMALIES
# ============================================================
def detect_outliers_and_anomalies (df) :
    log_info("Handling outliers and anomalies ...")

    numeric_cols = df.select_dtypes(include=['int64', 'float64', 'float32', 'int32']).columns

    # --- IQR Method ---
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3- Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        df[col] = np.where(df[col] < lower_bound, lower_bound,  np.where(df[col] > upper_bound, upper_bound, df[col]))

        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
        if len(outliers) > 0:
            log_info(f"Outliers detected in column {col}: {len(outliers)} rows")

    # --- Isolation Forest for Anomaly Flag ---
    if len(numeric_cols) > 0 :
        iso = IsolationForest(contamination=0.02, random_state=42)
        df['anomaly_flag'] = iso.fit_predict(df[numeric_cols])
        anomalies = (df['anomaly_flag'] == -1).sum()
        log_info(f"Anomalies detected: {anomalies} rows")

    else :
        df['anomaly_flag'] = 1
        log_info("No numeric columns found. Skipping anomaly detection.")

    log_info("Outliers and anomalies handled")
    return df

# ============================================================
# 7. TYPE CONVERSION & NORMALIZATION
# ============================================================
def convert_and_normalize(df):
    log_info("Converting data types ...")

    # --- Date to datetime ---

    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    for col in ['Price', 'Quantity', 'Revenue'] :
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    log_info("Normalizing numeric columns ...")
    numeric_cols = df.select_dtypes(include=['int64', 'float64', 'float32', 'int32']).columns
    scaler = MinMaxScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    log_info("Normalization complete.")
    return df

# ============================================================
# 8. SAVE RESULT AND SUMMARY
# ============================================================
def save_results(df, output_folder, duplicates_removed) :

    output_path = os.path.join(output_folder, "final_cleaned.csv")
    df.to_csv (output_path, index = False)
    
    summary_result = os.path.join(output_folder, 'final_summary.txt')
    with open(summary_result, 'w') as file:
        file.write(f"Total rows final: {len(df)}\n")
        file.write(f"Duplicates removed: {duplicates_removed}\n")
        file.write(f"Anomalies detected: {(df['anomaly_flag'] == -1).sum()}\n")
        file.write("Normalization: Min-Max Scaling (0-1)\n")
        file.write("Pipeline Status: SUCCESS \n")
    
    log_info(f"Saved cleaned data to {output_path}")
    log_info(f"Saved summary to {summary_result}")


# ============================================================
# 9. MAIN PIPELINE
# ============================================================

def main_pipeline():
    df = load_files(data_folder)
    df = handle_missing(df)
    df, dup_removed = handle_duplicates(df)
    df = detect_outliers_and_anomalies(df)
    df = convert_and_normalize(df)
    save_results(df, output_folder, dup_removed)
    log_info("Pipeline completed successfully!")

if __name__== '__main__':
    main_pipeline()