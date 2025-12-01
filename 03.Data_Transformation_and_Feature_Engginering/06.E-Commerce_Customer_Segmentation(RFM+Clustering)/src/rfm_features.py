from __future__ import annotations
from .log_info import  log_info, log_error, log_success
from pathlib import Path

import pandas as pd


# === RFM FEATURES ===
def  compute_rfm_features(df:pd.DataFrame, output_path:str| Path, logger=None) -> pd.DataFrame:
    # Compute Recency, Frequency, and Monetary (RFM) features for customer segmentation.

    try:
        if logger: 
            log_info (logger, "Starting RFM Feature Engineering...")

        df = df.copy()

        #  -> Ensure order_date datetime
        if df["order_date"].dtype != "datetime64[ns]":
            df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        #  -> Reference Date (max transaction)
        reference_date = df["order_date"].max()

        if logger:
            log_info(logger, f"Reference date for recency: {reference_date.date()}")

        #  -> Count RECENCY
        recency_df = (
            df.groupby("customer_id")["order_date"].max().reset_index()
        )

        recency_df["recency"] = (reference_date - recency_df["order_date"]).dt.days
        recency_df.drop(columns=["order_date"], inplace=True)

        # -> Count FREQUENCY
        frequency_df = (
            df.groupby("customer_id")["order_id"].count().reset_index().rename(columns={"order_id": "frequency"})
        )

        # -> Count MONETARY 
        monetary_df = (
            df.groupby("customer_id")["amount"].sum().reset_index().rename(columns={"amount" : "monetary"})
        )

        #  -> Merge R-F-M
        rfm = recency_df.merge(frequency_df, on="customer_id", how="left")
        rfm = rfm.merge(monetary_df, on="customer_id", how="left")

        # Save Output CSV
        output_path = Path (output_path)

        # If path is directory -> Create directory + user default file name
        if output_path.suffix == "":
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / "rfm_features.csv"
        else:
            output_path.parent.mkdir(parents=True,
            exist_ok=True)
            output_file = output_path

        rfm.to_csv(output_file, index=False)

        if logger:
            log_success(logger, f"RFM features save to: {output_path.resolve()}")
            
        return rfm
        
    except Exception as error:
        if logger:
            log_error(logger, f"RFM Feature Engineering failed: {error}")
        raise error

            

    


   
