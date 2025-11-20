from __future__ import annotations
import pandas as pd


# === TIME FEATURES ===
def time_features(df:pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Sort by time
    df = df.sort_values("transaction_date")

    # Basic time components
    df["year"] = df["transaction_date"].dt.year
    df["month"] = df["transaction_date"].dt.month
    df["day"] = df["transaction_date"].dt.day
    df["weekday"] = df["transaction_date"].dt.day_of_week
    df["week_number"] = df["transaction_date"].dt.isocalendar().week.astype(int)
    
    # Weekend Indicator
    df["is_weekend"] = df["weekday"] >= 5 #Sat-Sun

    # Hour & Time-of-Day
    df["hour"] = df["transaction_date"].dt.hour

    def categorize_time(hour: int) -> str:
        if hour < 6:
            return "night"
        elif hour < 12:
            return "morning"
        elif hour < 18:
            return "Afternoon"
        else:
            return "evening"
        
    df["time_of_day"] = df["hour"].apply(categorize_time)
    return df

# === SALES FEATURES ===

def sales_features (df:pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Revenue
    df["revenue"] = df["unit_price"] * df["quantity"]

    # Price is per item already
    df["avg_price_per_item"] = df["unit_price"]

    # Flag full hour transactions
    df["is_exact_hour"] = df["transaction_date"].dt.minute == 0

    # Important: Sorting untuk rolling window
    df = df.sort_values(["store_id", "transaction_date"])

    # Rolling window daily-based
    df["rolling_7_revenue"] = (
        df.set_index("transaction_date").groupby("store_id")["revenue"].rolling("7D", min_periods=1).sum().reset_index(level=0, drop=True)
    )

    # Rank revenue per day per store
    df["daily_sales_rank"] = (
        df.groupby(["store_id", df["transaction_date"].dt.date])["revenue"].rank(method="dense", ascending=False)
    )
    
    return df

# === Costumer Behavior Feature ===
def customer_behavior(df:pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # First purchase flag
    df["first_purchase_date"] = df.groupby("customer_id")["transaction_date"].transform("min")
    df["is_first_purchase"] = df["transaction_date"] == df["first_purchase_date"]

    # Costumer Visit Count (lifetime)
    df["customer_visit_count"] = df.groupby("customer_id")["transaction_date"].transform("count")

    # Customer frequency per month
    df["customer_monthly_frequency"] = (
        df.groupby(["customer_id", "year", "month"])["transaction_date"].transform("count")
    )

    df.drop(columns=["first_purchase_date"], inplace=True)
    return df



