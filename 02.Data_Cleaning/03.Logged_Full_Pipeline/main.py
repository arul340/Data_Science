# ============================================================
# Mini Project 3: Logged Full Pipeline with Error Control
# Author: Arul
# ============================================================

import os
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler
import logging

# ============================================================
# 1. PREPARE FOLDERS 
# ============================================================
data_folder = 'data'
output_folder = 'output'
logs_folder = 'logs'

os.makedirs(output_folder, exist_ok=True)
os.makedirs(logs_folder, exist_ok=True)

# ============================================================
# 2. DEFINE HELPER FUNCTION FOR LOGGING
# ============================================================

def log_info (message) :
    print(f"\033[94m[INFO]\033[0m {message}")
    
    with open(os.path.join(logs_folder, "pipeline.log"), "a" ) as file:
        file.write(f"{message} \n")

# ============================================================
# 3. SETUP LOGGING SYSTEM
# ============================================================

log_path = os.path.join(logs_folder, "pipeline.log")
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console = logging.StreamHandler()
console.setLevel (logging.INFO)
formatter = logging.Formatter("%(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


# ============================================================
# 4. SETUP LOGGING SYSTEM
# ============================================================

def load_files (folder_path) :
    try : 
        all_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]
        if not all_files:
            raise FileNotFoundError("No CSV files found in data folder")
        
        
        dataframes = []
        for file in all_files :
            file_path  = os.path.join(folder_path, file)
            log_info(f"Loading {file}")
            logging.info(f"Loading {file}")
            df = pd.read_csv(file_path)
            required_cols = ['Date', 'Product', 'Price', 'Quantity', 'Revenue']
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                log_info(f"Skipping {file} (missing columns: {missing})")
                logging.warning(f"Skipping {file} (missing columns: {missing})")
                continue
            dataframes.append(df)

        merged = pd.concat(dataframes, ignore_index=True)
        log_info(f"Total merged rows: {len(merged)}")
        logging.info(f"Total merged rows: {len(merged)}")
        return merged
    except Exception as error :
        log_info(f"Error loading files: {error}")
        logging.error(f"Error loading files: {error}")
        exit()

# ============================================================
# 5. HANDLE WITH ERROR CONTROL
# ============================================================

def handle_missing (df):
    try:
        before_missing = df.isna().sum().sum()
        for col in df.columns:
            if df[col].dtype == 'object':
                mode_val = df[col].mode()[0]
                df[col] = df[col].fillna(mode_val)
            else:
                mean_val = df[col].mean()
                df[col] = df[col].fillna(mean_val)
        after_missing = df.isna().sum().sum()

        log_info(f"Missing value handled: {before_missing - after_missing} filled")
        logging.info(f"Missing value handled: {before_missing - after_missing} filled")

        log_info(f"Missing value handled successfully")
        logging.info(f"Missing value handled successfully")

        return df
    
    except Exception as error:
        log_info(f'Error handling missing values: {error}')
        logging.error(f"Error handling missing values: {error}")
        return df, 0

def handle_duplicate(df):
    try:
        before_dup = len(df)
        df = df.drop_duplicates()
        after_dup = len(df)
        log_info(f"Duplicated removed: {before_dup - after_dup}")
        logging.info(f"Duplicated removed: {before_dup - after_dup}")
        return df, before_dup - after_dup
    except Exception as error:
        log_info(f"Error handling duplicates: {error}")
        logging.error(f"Error handling duplicates: {error}")
        return df, 0
    
def detect_outliers_and_anomalies(df) :
    log_info("Handling outliers and anomalies...")
    logging.info("Handling outliers and anomalies...")
    try:
        num_col = df.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns

        # --- IQR Method ---
        for col in num_col:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            
            IQR = Q3 - Q1

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            df[col] = np.where(df[col] < lower_bound, lower_bound, np.where(df[col] > upper_bound, upper_bound, df[col]))

            outliers = df[(df[col] < lower_bound) |( df[col] > upper_bound)]

            if len(outliers) > 0 :
                log_info(f"Outliers detected in column {col}: {len(outliers) } rows")

        # --- Isolation Forest for anomaly flag ---
        if len(num_col) > 0 :
            iso = IsolationForest(contamination=0.02, random_state=42)
            df['anomaly_flag'] = iso.fit_predict(df[num_col])
            anomalies = (df['anomaly_flag'] == -1).sum()
            log_info(f"Anomaly detect: {anomalies} rows")
            logging.info(f"Anomaly detect: {anomalies} rows")
        else :
            df['anomaly_flag'] = 1
            log_info("No numeric columns found. Skipping anomaly detection.")
            logging.info("No numeric columns found. Skipping anomaly detection.")
        return df
    except Exception as error:
        log_info(f"Error handling outliers and anomalies: {error}")
        logging.error(f"Error handling outliers and anomalies: {error}")
        return df, 0
    

def convert_and_normalize_data (df):
    log_info("Converting and normalizing data ...")

    try: 

        # --- Date Time ---
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # --- Numeric Cols ---
        for col in ["Price", "Quantity", "Revenue"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        log_info("Normalizing numeric columns ...") 
        num_cols = df.select_dtypes(include = ['int64', 'float64', 'int32', 'float32']).columns
        scaler = MinMaxScaler()
        df[num_cols] = scaler.fit_transform(df[num_cols])
        log_info("Normalization complete.")
        logging.info("Normalization complete.")
        return df
    
    except Exception as error:
        log_info(f"Error converting and normalizing data: {error}")
        logging.error(f"Error converting and normalizing data: {error}")
        return df, 0
    

# ============================================================
# 6 SAVE RESULT AND SUMMARY
# ============================================================

def save_result (df, output_folder, duplicate_removed):

    output_path = os.path.join(output_folder, "final_cleaned.csv")
    df.to_csv (output_path, index= False)

    summary_path = os.path.join(output_folder, "final_summary.txt")
    with open(summary_path, 'w') as file:
        file.write(f"Total rows final: {len(df)}\n")
        file.write(f"Duplicates removed: {duplicate_removed}\n")
        file.write(f"Anomalies detected: {(df['anomaly_flag'] == -1).sum()}\n")
        file.write("Normalization: Min-Max Scaling (0-1)\n")
        file.write("Pipeline Status: SUCCESS \n")

    log_info(f"Saved cleaned data to {output_path}")
    logging.info(f"Saved cleaned data to {output_path}")
    log_info(f"Saved summary to {summary_path}")
    logging.info(f"Saved summary to {summary_path}")
    
    
# ============================================================
# 7. MAIN PIPELINE
# ============================================================
def main_pipeline() :
    logging.info("=== Starting Logged Full Pipeline ===")
    log_info("=== Starting Logged Full Pipeline ===")

    df = load_files('data')
    if df.empty:
        logging.error("No data loaded. Pipeline stopped.")
        return
    
    df = handle_missing(df)
    df, removed = handle_duplicate(df)
    df = detect_outliers_and_anomalies(df)
    df = convert_and_normalize_data(df)

    save_result(df, 'output', removed)

    log_info("Pipeline completed successfully!")
    logging.info("Pipeline completed successfully!")


if __name__ == "__main__" :
    main_pipeline()

    


        





        

