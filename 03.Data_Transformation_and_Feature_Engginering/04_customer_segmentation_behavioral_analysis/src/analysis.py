from __future__ import annotations
from pathlib import Path
from typing import Any
import pandas as pd

def generate_customer_report(df: pd.DataFrame, output_path:Path, logger: Any,) -> None:
    # Generate aggregated metrics per customer segment and save as CSV

    # Output columns:
    #     - customer_segment
    #     - n_customers
    #     - age_mean
    #     - income_mean
    #     - spending_score_mean
    #     - visits_mean
    #     - avg_transaction_value_mean
    #     - spending_efficiency_mean
    #     - loyalty_score_mean
    #     - engagement_index_mean

    if "customer_segment" not in df.columns:
        msg = "Column 'customer_segment' not found in features DataFrame. Did you run Feature Engineering?"
        logger.error(msg)
        raise ValueError(msg)

    # Make sure valid segment to process
    # (Unclassified customers are not included in the report)
    agg = (
        df.groupby("customer_segment", dropna=False).agg(
            n_customers=("customer_id", "count"),
            age_mean=("age", "mean"),
            income_mean=("annual_income_usd", "mean"),
            spending_score_mean=("spending_score", "mean"),
            visits_mean=("visits_per_month", "mean"),
            avg_transaction_value_mean=("avg_transaction_value", "mean"),
            spending_efficiency_mean=("spending_efficiency", "mean"),
            loyalty_score_mean=("loyalty_score", "mean"),
            engagement_index_mean=("engagement_index", "mean"),
        ).reset_index()
    ) 

    # Save to CSV file
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(output_path, index=False)

    logger.info(f"Customer segment report saved to: {output_path}")
    logger.info(f"Report preview:\n{agg.head()}")