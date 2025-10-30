# ============================================================
# Mini Project 5: Production-Ready Pipeline (Master Version)
# Author: Arul
# ============================================================

import os
import json
import pandas as pd
import numpy as np
import logging
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler, StandardScaler

# ============================================================
# 1. SETUP ENVIRONMENT
# ============================================================
def setup_environment(config) :
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
    formatter = logging.Formatter("%[(levelname)s] - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    logging.info("==== Pipeline Initialized ====")
    log_info(f"Environment setup complete.")

    return folders

def log_info(message):
    print(f"\033[94m[INFO]\033[0m {message}")
    logging.info(message)


def load_config(config_path = "config.json"):
    try :
        with open(config_path, 'r') as file:
            config = json.load(file)
        log_info("Config file loaded successfully.")
        return config
    except Exception as error:
        log_info(f"Error loading config file: {error}")
        logging.error(f"Error loading config file: {error}")
        exit()
           
# ============================================================
# 2. LOAD AND VALIDATE DATE
# ============================================================
def load_files (config, folders):
    try:
        data_folder = folders["data"]
        all_files = [file for file in os.listdir(data_folder) if file.endswith(".csv")]
        if not all_files:
            raise FileNotFoundError("No CSV files found in data folder")
        
        dataframes = []
        for file in all_files:
            file_path = os.path.join(data_folder, file)
            log_info(f"Loading {file}")
            logging.info(f"Loading {file}")
            df = pd.read_csv(file_path)

            required_cols = ["Date", "Product", "Price", "Quantity", "Revenue"]
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                log_info(f"Skipping {file} (missing columns: {missing})")
                logging.warning(f"Skipping {file} (missing columns: {missing})")
                continue

            dataframes.append(df)
            
        df = pd.concat(dataframes, ignore_index=True)
        log_info(f"Total merged rows: {len(df)}")
        logging.info(f"Total merged rows: {len(df)}")
        return df
    
    except Exception as error:
        log_info(f"Error loading files: {error}")
        logging.error(f"Error loading files: {error}")
        return pd.DataFrame()

# ============================================================
# 3. HANDLE MISSING VALUES AND DUPLICATES
# ============================================================
def handle_missing(df, config):
    try:
        if not config["settings"]["handle_missing"]:
            log_info("Skipping missing value handling (disabled in config)") 
            logging.info("Skipping missing value handling (disabled in config)")
            return df

        before_handle = df.isna().sum().sum()
        for col in df.columns:
            if df[col].dtype == "object":
                mode_val = df[col].mode()[0]
                df[col] = df[col].fillna(mode_val)
            else:
                mean_val = df[col].mean()
                df[col] = df[col].fillna(mean_val) 
        after_handle = df.isna().sum().sum()

        log_info(f"Missing value handled: {before_handle - after_handle} filled")
        logging.info(f"Missing value handled: {before_handle - after_handle} filled")
        return df
    
    except Exception as error:
        log_info(f"Error handling missing values: {error}")
        logging.info(f"Error handling missing value: {error}")
        return df


def handle_duplicates (df, config) :
    try:
        if not config["settings"]["handle_duplicates"]:
            log_info("Skipping duplicate handling (disabled in config)")
            logging.info("Skipping duplicate handling (disabled in config)")
            return df, 0
    
        before_handle = len(df)
        df = df.drop_duplicates()
        after_handle = len(df)
        duplicates_removed = before_handle - after_handle

        log_info(f"Duplicated removed: {before_handle - after_handle} rows")
        logging.info(f"Duplicated removed: {before_handle - after_handle} rows")
        return df, duplicates_removed
    
    except Exception as error:
        log_info(f"Error handling duplicates: {error}")
        logging.info(f"Error handling duplicates: {error}")      
        exit()

# ============================================================
# 4. DETECT AOUTLIERS AND ANOMALIES - ISOLATION FOREST
# ============================================================
def detect_outliers_and_anomalies(df, config):
    try:
        if not config["settings"]["detect_outliers_and_anomalies"] :
            log_info("Skipping outliers and anomalies detection (disabled in config)")
            logging.info("Skipping outliers and anomalies detection (disabled in config)")
            return df
        
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
                log_info(f"Outliers detected in column {col} : {len(outliers)} rows")
                logging.info(f"Outliers detected in column {col} : {len(outliers)} rows")
            else :
                log_info(f"No outliers detected in column {col}")
                logging.info(f"No outliers detected in column {col}")

        if len(num_col) > 0:
            contamination = float(config["parameters"]["isolation_forest_contamination"])
            random_state = config["parameters"]["random_state"]

            iso = IsolationForest(contamination = contamination, random_state= random_state)
            df['anomaly_flag'] = iso.fit_predict(df[num_col])
            anomalies = (df["anomaly_flag"] == -1).sum()

            log_info(f"Anomalies detected: {anomalies} rows")
            logging.info(f"Anomalies detected: {anomalies} rows")
        return df
        
    except Exception as error:
        log_info(f"Error handling outliers and anomalies: {error}")
        logging.info(f"Erorr handling outliers and anomalies: {error}")
        return df, 0

# ============================================================
# 5.  CONVERT AND NORMALIZE DATA
# ============================================================
def convert_and_normalize(df, config):
    try:
        norm_method = config["settings"]["normalization"]
        scaler = MinMaxScaler() if norm_method == "minmax" else StandardScaler()
    
        # --- Date Time ---     
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        
        # --- Numeric Cols ---
        for col in ["Price", "Quantity", "Revenue"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        log_info("Normalizing numeric columns ...")
        logging.info("Normalizing numeric columns...")

        num_cols = df.select_dtypes(include = ['int64', 'float64', 'int32', 'float32']).columns
        df[num_cols] = scaler.fit_transform(df[num_cols])       
        log_info(f"Normalization complete using {norm_method}")
        logging.info(f"Normalization complete using {norm_method}")
        return df
    
    except Exception as error:
        log_info(f"Error converting and nomalizing data: {error}")
        logging.info(f"Error converting and normalizing data: {error}")
        return df
    

# ============================================================
# 6.  SAVE OUTPUT AND SUMMARY
# ============================================================
def save_result(df, folders, summary):
    try:
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H.%M.%S")
        output_path = os.path.join(folders["output"], f"final_cleaned_{timestamp}.csv")
        summary_path = os.path.join(folders["output"], f"final_summary_{timestamp}.txt")
        df.to_csv(output_path, index =False)

        with open(summary_path, "w") as file:
            file.write(f"Total rows final: {len(df)}\n")
            file.write(f"Missing values: {summary['missing_values']}\n")
            file.write(f"Duplicates removed: {summary['duplicates_removed']}\n")
            file.write(f"Anomalies detected: {summary['anomalies_detected']}\n")
            file.write("Normalization: Complete\nPipeline Status: SUCCESS\n")

        log_info(f"Saved cleaned data to {output_path}")
        logging.info(f"Saved cleaned data to {output_path}")
        log_info(f"Saved summary to {summary_path}")
        logging.info(f"Saved summary to {summary_path}")
    except Exception as error:
        log_info(f"Error saving results: {error}")
        logging.info(f"Error saving results: {error}")

# ============================================================
# 6.  MAIN PIPELINE
# ============================================================
def main ():
    config = load_config("config.json")
    folders = setup_environment(config)

    try :
        df = load_files(config, folders)
        if df.empty:
            log_info("No data found in input folder.")
            logging.info("No data found in input folder.")
            return
        

        df = handle_missing(df, config)
        df, duplicates_removed = handle_duplicates(df, config)
        df = detect_outliers_and_anomalies(df, config)
        df = convert_and_normalize(df, config)

        summary = {
            "missing_values" : df.isna().sum().sum(),
            "duplicates_removed" : duplicates_removed,
            "anomalies_detected" : int((df["anomaly_flag"] == -1).sum() if "anomaly_flag" in df.columns else 0)
        }

        save_result(df, folders, summary)

        log_info("Pipeline completed successfully!")
        logging.info("Pipeline completed successfully!")

    except Exception as error:
        log_info(f"Error running pipeline: {error}")
        logging.info(f"Error running pipeline: {error}")
        exit()


if __name__ == "__main__":
    main()