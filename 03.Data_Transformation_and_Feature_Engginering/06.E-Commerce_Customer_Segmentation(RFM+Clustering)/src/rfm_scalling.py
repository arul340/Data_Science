from sklearn.preprocessing import MinMaxScaler, StandardScaler
from pathlib import Path
from typing import Dict, Any
from .log_info import log_info
import pandas as pd

def scale_rfm_features(rfm_df: pd.DataFrame, output_path: str | Path,  parameters : Dict [str, Any], logger=None) -> pd.DataFrame:
    # Scale RFM features using MinMaxScaler
    try:
        if logger:
            log_info(logger, "Starting RFM Scaling...")

        rfm = rfm_df.copy()
        normalization_type = parameters.get("normalization", "minmax")

        scaler = MinMaxScaler() if normalization_type == "minmax" else StandardScaler()

        scaled_data = scaler.fit_transform(rfm[["recency", "frequency", "monetary"]])

        rfm_scaled = pd.DataFrame(
            scaled_data, columns=["recency_scaled", "frequency_scaled", "monetary_scaled"]
        )

        rfm_scaled.insert(0, "customer_id", rfm["customer_id"])

        # If path is directory -> create directory + use default file name
        output_path = Path(output_path)

        if  output_path.suffix == "":
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / "rfm_scaled.csv"
        else:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_file = output_path

        # Save to CSV
        rfm_scaled.to_csv(output_file, index=False)
    
        if logger :
            log_info (logger, f"RFM features scaled file save to : {output_path.resolve()}")

        return rfm_scaled

    except Exception as error:
        if logger:
            log_info(logger, f"Failed to scale RFM features: {error}")
        raise error

