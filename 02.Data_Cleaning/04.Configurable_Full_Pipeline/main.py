# ============================================================
# Mini Project 4: Configurable Full Pipeline (Dynamic Settings)
# Author: Arul
# ============================================================
import os 
import json
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import logging


def log_info(message):
    print(f"\033[94m[INFO]\033[0m {message}")
    logging.info(message)

# ============================================================
# 1. LOAD CONFIG FILE
# ============================================================
def load_config(config_path= "config.json"):
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
        print("Config file loaded successfully.")
        return config
    except Exception as error:
        log_info(f"Error loading config file: {error}")
        logging.error(f"Error loading config file: {error}")

# ============================================================
# 1. SETUP FOLDERS AND LOGGGING
# ============================================================
def setup_environment(config):
    folders = config["folders"]
    os.makedirs(folders["data"], exist_ok=True)
    os.makedirs(folders["output"], exist_ok=True)
    os.makedirs(folders["logs"], exist_ok=True)

    log_path = os.path.join(folders["logs"], "pipeline.log.txt")
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)

    logging.info("=== Pipeline Initialized ===")
    log_info(f"Environment setup and  logging ready.")

# ============================================================
# 2. CORE PIPELINE FUNCTION (To Be Impelmented)
# ============================================================

#  --- Load Files ---
def load_files(folder_path):
    try:
        all_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]
        if not all_files:
            raise FileNotFoundError("No CSV files found in data folder")
        

        dataframes = []
        for file in all_files:
            file_path = os.path.join(folder_path, file)
            logging.info(f"Loading {file}")
            log_info(f"Loading {file}")
            df = pd.read_csv(file_path)

            required_cols = ['Date', 'Product', 'Price', 'Quantity', 'Revenue']
            missing = [col for col in required_cols if col not in df.columns]
            if missing :
                log_info(f"Skipping {file} (missing columns: {missing})")
                logging.info(f"Skipping {file} (missing columns: {missing})")
                continue

            dataframes.append(df)

        merged_file = pd.concat(dataframes, ignore_index=True)
        log_info(f"Total merged rows: {len(merged_file)}")
        logging.info(f"Total merged rows: {len(merged_file)}")
        return merged_file
    
    except Exception as error:
        log_info(f"Error loading files: {error}")
        logging.info(f"Error loading files: {error}")
        exit()

# --- handle Missing Value  ----
def handle_missing(df) :
    try:
        before_handle = df.isna().sum().sum()
        for col in df.columns:
            if df[col].dtype == 'object':
                mode_val = df[col].mode()[0]
                df[col] = df[col].fillna(mode_val)
            else: 
                mean_val = df[col].mean()
                df[col] = df[col].fillna(mean_val)

        after_handle = df.isna().sum().sum()

        log_info(f"Missing value handled: {before_handle - after_handle} filled")
        logging.info (f"Missing value handled: {before_handle - after_handle} filled")
        return df

    except Exception as error:
        log_info(f"Error handling missing values: {error}")
        logging.info(f"Error handling missing value: {error}")
        exit()


# --- handle Duplicates ---
def handle_duplicates (df) :
    try:
        before_handle = len(df)
        df = df.drop_duplicates()
        after_handle = len(df)

        log_info(f"Duplicated removed: {before_handle - after_handle}")
        logging.info(f"Duplicated removed: {before_handle - after_handle}")
        return df, before_handle - after_handle
    except Exception as error:
        log_info(f"Error handling duplicates: {error}")
        logging.info(f"Error handling duplicates: {error}")
        exit()

#  --- Detect Outliers and Anomalies ---
def detect_outliers_and_anomalies (df, config) :
    try:
        num_col = df.select_dtypes(include = ['int64', 'float64', 'int32', 'float32']).columns

        for col in num_col: 
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            df[col] = np.where(df[col] < lower_bound, lower_bound, np.where(df[col] > upper_bound, upper_bound, df[col]))

            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]

            if len(outliers) > 0 :
                log_info(f"Outliers detected in column {col}: {len(outliers)} rows")
                logging.info(f"Outliers detected in column {col} : {len(outliers)} rows")

            else :
                log_info(f"No outliers detected in column {col}")
                logging.info(f"No outliers detected in column {col}")

        if len (num_col) > 0 :
            contamination = float(config["parameters"]["isolation_forest_contamination"])
            random_state = config["parameters"]["random_state"]
            iso= IsolationForest(contamination = contamination, random_state = random_state)
            df['anomaly_flag'] = iso.fit_predict(df[num_col])
            anomalies = (df['anomaly_flag'] == -1).sum()
            log_info(f"Anomalies detected: {anomalies} rows")
            logging.info(f"Anomalies detected: {anomalies} rows")

        return df

    except Exception as error :
        log_info(f"Error handling outliers and anomalies: {error}")
        logging.info (f"Error handling outliers and anomalies: {error}")
        exit()

# --- Normalize Data ---
def normalize_data(df, method="minmax"):
    try:
        if method == "minmax":
            scaler = MinMaxScaler()
        elif method == "standard":
            scaler = StandardScaler()
        else :
            raise ValueError(f"Invalid normalization method: {method}")
        
        # --- Date Time ---

        if "Date" in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors="coerce")
        
        # --- Numeric Cols ---
        for col in ['Price', 'Quantity', 'Revenue'] :
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            

        log_info("Normalizing numeric columns ...")
        logging.info("Normalizing numeric columns ...")

        num_cols = df.select_dtypes(include = ['int64', 'float64', 'int32', 'float32']).columns
        df[num_cols] = scaler.fit_transform(df[num_cols])

        log_info("Normalization complete.")
        logging.info("Normalization complete.")

        return df 
    
    except Exception as error:
        log_info(f"Error normalizing data: {error}")
        logging.info(f"Error normalizing data: {error}")
        exit()


# --- Save Result ---
def save_results(df, config, dup_removed):
    try:
        output_path = os.path.join(config["folders"]["output"], f"final_cleaned_{pd.Timestamp.now().strftime('%Y-%m-%d_%H.%M.%S')}.csv")
        df.to_csv(output_path, index = False)

        summary_path = os.path.join(config["folders"]["output"], "final_summary.txt")
        with open(summary_path, "w") as file:
            file.write(f"Total rows final: {len(df)} \n")
            file.write(f"Duplicates removed: {dup_removed} \n")
            file.write(f"Anomalies detected: {(df['anomaly_flag'] == -1).sum()} \n")
            file.write(f"Normalization: {config['settings'] ['normalization']} \n")
            file.write("Pipeline Status: SUCCESS \n")
                       
        log_info(f"Saved cleaned data to {output_path}")
        logging.info(f"Saved cleaned data to {output_path}")
        log_info(f"Saved summary to {summary_path}")
        logging.info(f"Saved summary to {summary_path}")

    except FileNotFoundError as error:
        log_info(f"Error saving results: {error}")
        logging.info(f"Error saving results: {error}")
        exit()

# ============================================================
# 3. Main Pipeline
# ===========================================================

def main() :
    config = load_config()
    setup_environment (config)

    try :
        log_info("=== Pipeline Started ===")
        logging.info("=== Pipeline Started ===")
        df = load_files(config['folders']['data'])

        if not isinstance(df, pd.DataFrame):
            log_info("Load_files did not return a DataFrame. Pipeline stopped.")
            logging.error("Load_files did not return a DataFrame. Pipeline stopped.")
            return

        if df.empty:
            log_info("No data loaded. Pipeline stopped.")
            logging.error("No data loaded. Pipeline stopped.")
            return

         
        if config["settings"] ["handle_missing"]:
            df = handle_missing(df)

        if config["settings"] ["handle_duplicates"]:
            df, dup_removed = handle_duplicates(df)
        
        else :
            dup_removed = 0

        if config["settings"]["detect_outliers_and_anomalies"]:
            df = detect_outliers_and_anomalies(df, config)

        if config["settings"]["normalization"]:
            df = normalize_data(df, config['settings']['normalization'])

        save_results(df, config, dup_removed)
        
        logging.info("=== Pipeline Completed ===")
        log_info("=== Pipeline Completed ===")
         
    except Exception as error:
        log_info(f"Pipeline Failed: {error}")
        logging.info(f"Pipeline Failed: {error}")


if __name__ == "__main__" :
    main()