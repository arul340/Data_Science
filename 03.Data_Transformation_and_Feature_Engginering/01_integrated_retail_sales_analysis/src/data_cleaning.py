import pandas as pd
import numpy as np
from src.log_info import log_info
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from typing import Dict

_EXCLUDE_OUTLIER = {"id_transaksi", "latitude", "longitude"}
_SCALE_COLS = ("jumlah", "harga_satuan", "diskon")

def clean_data(df: pd.DataFrame, missing_strategy: str = "mean") -> pd.DataFrame:
    df = df.drop_duplicates().copy()

    for col in df.columns:
        if df[col].dtype == "object":
            mode = df[col].mode(dropna=True)
            if len(mode):
                df[col] = df[col].fillna(mode[0])
        else:
            val = df[col].median() if missing_strategy == "median" else df[col].mean()
            df[col] = df[col].fillna(val)

    if "tanggal_transaksi" in df.columns:
        df["tanggal_transaksi"] = pd.to_datetime(df["tanggal_transaksi"], errors="coerce")

    if "id_transaksi" in df.columns:
        df["id_transaksi"] = pd.to_numeric(df["id_transaksi"], errors="coerce").astype("Int64")

    return df

def detect_outliers_and_anomalies(df: pd.DataFrame, parameters: Dict) -> pd.DataFrame:
    try:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.difference(list(_EXCLUDE_OUTLIER))

        # IQR capping
        for col in numeric_cols:
            Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR  
            n_out = int(((df[col] < lower) | (df[col] > upper)).sum())
            if n_out:
                log_info(f"Outliers detected in column {col}: {n_out} rows")
            df[col] = np.clip(df[col], lower, upper)

        # IsolationForest
        if bool(parameters.get("enable_isolation_forest", True)) and len(numeric_cols) > 0:
            contamination = float(parameters.get("isolation_forest_contamination", 0.02))
            random_state = int(parameters.get("random_state", 42))
            iso = IsolationForest(contamination=contamination, random_state=random_state)
            df["anomaly_flag"] = iso.fit_predict(df[numeric_cols])
            log_info(f"Anomalies detected: {(df['anomaly_flag'] == -1).sum()} rows")
        else:
            df["anomaly_flag"] = 1

        return df
    except Exception as e:
        log_info(f"Error in detect_outliers_and_anomalies: {e}")
        return df

def convert_and_normalize(df: pd.DataFrame, parameters: Dict) -> pd.DataFrame:
    # Normalize selected numeric columns only (exclude ID, date, lat/lon, computed features).
    try:
        if "tanggal_transaksi" in df.columns:
            df["tanggal_transaksi"] = pd.to_datetime(df["tanggal_transaksi"], errors="coerce")

        for col in _SCALE_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        method = parameters.get("normalization", "minmax")
        scaler = MinMaxScaler() if method == "minmax" else StandardScaler()
        cols_to_scale = [c for c in _SCALE_COLS if c in df.columns]

        if cols_to_scale:
            df[cols_to_scale] = scaler.fit_transform(df[cols_to_scale])

        log_info(f"Normalization complete on {cols_to_scale} using {method}")
        return df
    except Exception as e:
        log_info(f"Error in convert_and_normalize: {e}")
        return df
