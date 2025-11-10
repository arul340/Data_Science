import pandas as pd
import numpy as np
from typing import Dict
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from .log_info import log_info

# columns NOT used for outlier/anomaly detection
_EXCLUDE_OUTLIER = {"employee_id"}
# columns to be scaled
_SCALE_COLS = ("salary", "working_hours", "projects_done", "performance_score")

def clean_data (df:pd.DataFrame, missing_value_strategy: str = "mean") -> pd.DataFrame:
    # Drop duplicates, fill in missing values ​​(mean/median for numeric, mode for categorical)
    df = df.drop_duplicates().copy()

    for col in df.columns:
        if df[col].dtype == "object":
            mode = df[col].mode(dropna=True)
            if not mode.empty:
                df[col] = df[col].fillna(mode.iloc[0])
        else:
            filler = df[col].mean() if missing_value_strategy == "mean"  else df[col].median()
            df[col] = df[col].fillna(filler)

    return df

def detect_outliers_and_anomalies(df:pd.DataFrame, parameters:Dict) -> pd.DataFrame:
    # Clip outliers via IQR, then detect anomalies using IsolationForest (optional).
    try:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.difference(list(_EXCLUDE_OUTLIER))

        # IQR clipping
        for col in numeric_cols:
            Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
            n_out = int(((df[col] < lower) | (df[col] > upper)).sum())
            if n_out:
                log_info(f"Outliers in '{col}': {n_out} rows clipped")
            df[col] = np.clip(df[col], lower, upper)

        # Isolation Forest
        enable_iforest = parameters.get("enable_isolation_forest", True)
        if isinstance(enable_iforest, str):
            enable_iforest = enable_iforest.lower() == "true"

        if enable_iforest and len(numeric_cols) > 0:
            contamination = float(parameters.get("isolation_forest_contamination", 0.02))
            random_state = int(parameters.get("random_state", 42))
            iso = IsolationForest(contamination=contamination, random_state=random_state)
            df['anomaly_flag'] = iso.fit_predict(df[numeric_cols])
            log_info(f"Anomalies detected: {(df['anomaly_flag'] == -1).sum()} rows")

        else:
            df["anomaly_flag"] = 1
        
        return df

    except Exception as error:
        log_info(f"Error in detect_outliers_and_anomalies: {error}")
        return df
    
def normalize_data(df:pd.DataFrame, parameters:Dict) -> pd.DataFrame:
    # Scale the selected numeric column with the MinMax/Standard scaler.
    try:
        # make sure it's numeric
        for col in _SCALE_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        method = str(parameters.get("normalization", "minmax")).lower()
        scaler = MinMaxScaler() if method == "minmax" else StandardScaler()

        cols_to_scale = [col for col in _SCALE_COLS if col in df.columns]
        if cols_to_scale:
            df[cols_to_scale] = scaler.fit_transform(df[cols_to_scale])
            log_info(f"Normalization complete on {cols_to_scale} using '{method}'")

        return df
    
    except Exception as error:
        log_info(f"Error in normalize_data: {error}")
        return df