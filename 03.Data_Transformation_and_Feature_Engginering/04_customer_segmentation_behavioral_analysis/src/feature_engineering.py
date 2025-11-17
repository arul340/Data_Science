from __future__ import annotations
from typing import Any, Dict, List
import numpy as np
import pandas as pd

def _encode_gender(df:pd.DataFrame) -> pd.Series:
    # Encode gender into numeric: Female -> 0, Male -> 1, Other/Unknown -> -1.
    mapping = {"Female": 0, "Male": 1}
    return df["gender"].map(mapping).fillna(-1).astype(int)

def _encode_city(df: pd.DataFrame) -> pd.Series:
    # Encode city to numeric IDs using a stable mapping based on sorted unique values (so it's deterministic).
    unique_cities = sorted(df["city"].dropna().unique())
    city_to_id = {city: idx for idx, city in enumerate(unique_cities)}
    return df["city"].map(city_to_id).fillna(-1).astype(int)

def _compute_spending_efficiency(df:pd.DataFrame) -> pd.Series:
    # Spending efficiency
    return df["spending_score"] * df["visits_per_month"]

def _compute_loyalty_score(df: pd.DataFrame) -> pd.Series:
    # Loyalty score - normalize days_since_last_purchase first
    max_days = df["days_since_last_purchase"].max()
    if max_days > 0:
        return 1.0 - (df["days_since_last_purchase"] / max_days)
    else:
        return pd.Series(1.0, index=df.index)

def _compute_engagement_index(
        spending_efficiency: pd.Series,
        visits_per_month: pd.Series,
        loyalty_score: pd.Series,
) -> pd.Series:
    # Engagement index: rata-rata dari tiga metrik utama (0..1):
    # - spending_efficiency (normalized)
    # - visits_per_month (already normalized)
    # - loyalty_score (already normalized)
    
    # Normalize spending_efficiency
    max_spending_eff = spending_efficiency.max()
    if max_spending_eff > 0:
        spending_eff_norm = spending_efficiency / max_spending_eff
    else:
        spending_eff_norm = spending_efficiency
    
    return (spending_eff_norm + visits_per_month + loyalty_score) / 3.0

def _assign_segment(
        engagement_index: pd.Series,
        high_threshold: float = 0.75,
        medium_threshold: float = 0.4,
) -> pd.Series:
    # Customer segmentation based engagement_index:
    # - High-Value: engagement_index >= high_threshold
    # - Medium-Value: medium_threshold <= engagement_index < high_threshold
    # - Low_Value: engagement_index < medium_threshold

    segments = []
    for val in engagement_index:
        if np.isnan(val):
            segments.append("Unclassified")
        elif val >= high_threshold:
            segments.append("High-Value")
        elif val >= medium_threshold:
            segments.append("Medium-Value")
        else:
            segments.append("Low-Value")
    
    return pd.Series(segments, index=engagement_index.index)

def engineer_customer_features(df: pd.DataFrame, parameters: Dict[str, Any], logger) -> pd.DataFrame:
    # Create model-ready features for customer segmentation:
    # - Encode gender and city
    # - Compute:
    #     * spending_efficiency (0..1)
    #     * loyalty_score (0..1)
    #     * engagement_index (0..1)
    # - Assign customer_segment based on engagement_index

    df = df.copy()

    # === Encode categorical variables ===
    df["gender_encoded"] = _encode_gender(df)
    df["city_encoded"] = _encode_city(df)
    logger.info("Encoded 'gender' and 'city' into numeric features.")

    # === Compute engineered metrics (all in 0..1 range) ===
    df["spending_efficiency"] = _compute_spending_efficiency(df)
    df["loyalty_score"] = _compute_loyalty_score(df)
    df["engagement_index"] = _compute_engagement_index(
        df["spending_efficiency"],
        df['visits_per_month'],
        df["loyalty_score"]
    )

    logger.info("Computed spending_efficiency, loyalty_score, and engagement_index.")

    # === Customer Segmentation ===
    high_threshold = float(parameters.get("segment_high_threshold", 0.75))
    medium_threshold = float(parameters.get("segment_medium_threshold", 0.4))

    df["customer_segment"] = _assign_segment(
        df["engagement_index"], high_threshold=high_threshold, medium_threshold=medium_threshold
    )

    # Info ringkas distribusi segmen
    segment_counts = df["customer_segment"].value_counts(dropna=False)
    logger.info(f"Customer segment distribution: \n{segment_counts}")

    # === Choose columns to save as output features ===
    features_cols = [
        "customer_id",
        "age",
        "annual_income_usd",
        "spending_score",
        "visits_per_month",
        "avg_transaction_value",
        "days_since_last_purchase",
        "gender_encoded",
        "city_encoded",
        "spending_efficiency",
        "loyalty_score",
        "engagement_index",
        "customer_segment",
    ]

    missing_features_cols = [col for col in features_cols if col not in df.columns]
    if missing_features_cols:
        logger.warning(
            f"The following expected feature columns are missing and will be skipped: "
            f"{missing_features_cols}"
        )
    
    final_cols = [col for col in features_cols if col in df.columns]
    df_out = df[final_cols].copy()
    logger.info(f"Feature engineering completed. Output features: {df_out.shape}")

    return df_out