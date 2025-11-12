import pandas as pd
import numpy as np
from datetime import datetime

def create_features (df:pd.DataFrame) -> pd.DataFrame:
    # Add new features for home price analysis
    df = df.copy()

    # Take the current year
    current_year = datetime.now().year

    # === New Features ===
    df["price_per_sqft"] = np.where(
        df["area_sqft"] == 0,
        np.nan,
        df['price'] / df["area_sqft"]
    )

    fill_mode = df["price_per_sqft"].median()
    df["price_per_sqft"].fillna(fill_mode)
    
    df["house_age"] =  current_year - df["year_built"]
    df["location_store"] = 100 / (df["distance_to_city_km"] + 1)

    # Location categorization based on distance
    df["location_category"] = pd.cut(
        df["distance_to_city_km"],
        bins=[0, 3, 7, np.inf],
        labels=["Central", "Suburban", "Rural"],
        include_lowest=True
    )

    return df
