from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd
import numpy as np

from .log_info import (
    setup_logger,
    log_stage,
    log_info,
    log_success,
)

# Helpers - Time Features 
def _ensure_time_feature(df: pd.DataFrame) -> pd.DataFrame:
    # Make sure time columns are needed present:
    #  -> transaction_datetime
    #  -> date_only
    #  -> day_name (Monday, Thuesday, ...)
    #  -> day_of_week (0=Monday, 6=Sunday)
    #  -> Hour (0-23)
    #  -> is_weekend (bool)

    df = df.copy()
    
   # 1. Create transaction_datetime from existing transaction_date
    if "transaction_datetime" in df.columns:
        datetime_series = pd.to_datetime(df["transaction_datetime"])
    elif "transaction_date" in df.columns:
        # Use the existing transaction_date column which is already datetime
        datetime_series = pd.to_datetime(df["transaction_date"])
        df["transaction_datetime"] = datetime_series
    else:
        # Fallback to old logic (if date and time columns exist)
        if "date" not in df.columns:
            raise KeyError("Column 'date' or 'transaction_date' is required for time-based analysis.")
        date_series = pd.to_datetime(df["date"])

        if "time" in df.columns:
            # Combine date + time
            datetime_series = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
        else:
            # if time is not available, default to 00:00
            datetime_series = date_series

        df["transaction_datetime"] = datetime_series

    datetime_series = pd.to_datetime(df["transaction_datetime"])

    # 2. Time derivative
    df["date_only"] = datetime_series.dt.date
    df["day_of_week"] = datetime_series.dt.dayofweek
    df["day_name"] = datetime_series.dt.day_name()
    df["hour"] = datetime_series.dt.hour
    df["is_weekend"] = df["day_of_week"] >= 5

    return df

# === 1. Daily Analysis ===
def _daily_analysis(df:pd.DataFrame) -> Dict[str, pd.DataFrame] :
    # Daily:
    #  -> Total Daily revenue
    #  -> avergae daily items sold
    #  -> top-selling products per day
    df = df.copy()

    if "revenue" not in df.columns:
        raise KeyError("Column 'revenue' os required. Make sure feature_engineering has created it.")
    if "quantity" not in df.columns:
        raise KeyError("Column 'quantity' is required for daily analysis.")
    
    # Total revenue & avg items per transaction per day
    daily_summary = (
        df.groupby("date_only", dropna=False).agg(
            total_revenue=("revenue", "sum"),
            total_items_sold=("quantity", "sum"),
            avg_items_per_transaction=("quantity", "mean"),
            n_transaction=("transaction_id", "nunique")
        ).reset_index().rename(columns={"date_only": "date"})
    )

    # Top-selling product per day (by revenue)
    daily_product = (
        df.groupby(["date_only", "product_id"], dropna=False)["revenue"].sum().reset_index()
    )

    idx = (
        daily_product.groupby("date_only")["revenue"].idxmax().dropna().astype(int)
    )
    daily_top_products = (
        daily_product.loc[idx].rename(
            columns={
                "date_only": "date",
                "product_id": "top_product_id",
                "revenue": "top_product_revenue",
            }
        ).reset_index(drop=True)
    ) 
    return {
        "daily_summary": daily_summary,
        "daily_top_products": daily_top_products
    }

# === 2. Weekly Analysis ===
def _weekly_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    # Weekly:
    #  -> Revenue by day-of-week
    #  -> Weekend vs Weekday performance
    #  -> Busiest day
    #  -> slowest day

    df = df.copy()

    # Revenue by day_name
    dow_summary = (
        df.groupby("day_name", dropna=False).agg(
            total_revenue=("revenue", "sum"),
            n_transaction=("transaction_id", "nunique"),
            total_items=("quantity", "sum")
        ).reset_index()
    )

    # Add day_of_week index (for sort Monday-Sunday)
    day_order = (
        df[["day_name", "day_of_week"]].drop_duplicates().set_index("day_name")["day_of_week"].to_dict()
    )
    dow_summary["day_of_week_index"] = dow_summary["day_name"].map(day_order)
    dow_summary = dow_summary.sort_values("day_of_week_index").reset_index(drop=True)

    # Weekend vs Weekday
    weekend_summary = (
        df.groupby("is_weekend", dropna=False).agg(
            total_revenue=("revenue", "sum"),
            n_transaction=("transaction_id", "nunique"),
            total_items=("quantity", "sum")
        ).reset_index()
    )
    weekend_summary["segment"] = np.where (
        weekend_summary["is_weekend"], "Weekend", "Weekday"
    )

    # Busiest & slowest day (by revenue)
    busiest_row = dow_summary.loc[dow_summary["total_revenue"].idxmax()]
    slowest_row = dow_summary.loc[dow_summary["total_revenue"].idxmin()]

    kpi ={
        "busiest_day": busiest_row["day_name"],
        "busiest_day_revenue": float(busiest_row["total_revenue"]),
        "slowest_day": slowest_row["day_name"],
        "slowest_day_revenue": float(slowest_row["total_revenue"])
    }
    return {
        "dow_summary": dow_summary.drop(columns=["day_of_week_index"]),
        "weekend_summary": weekend_summary[["segment", "total_revenue", "n_transaction", "total_items"]],
        "weekly_kpi": kpi
    }

# === 3. Hourly Analysis ===
def _hourly_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    # Hourly:
    #  -> Peak hour sales
    #  -> Heatmap  hour vs day-of-week (revenue)

    df = df.copy()

    # Revenue per hour(aggregated)
    hourly_revenue = (
        df.groupby("hour", dropna=False)["revenue"].sum().reset_index().sort_values("hour")
        )
    
    if not hourly_revenue.empty:
        peak_row = hourly_revenue.loc[hourly_revenue["revenue"].idxmax()]
        peak_hour = int(peak_row["hour"])
        peak_revenue = float(peak_row["revenue"])
    else:
        peak_hour = None
        peak_revenue = None

    # Heat map hour vs day-of-week-name
    heatmap = (
        df.pivot_table(
            index="day_name",
            columns="hour",
            values="revenue",
            aggfunc="sum",
            fill_value=0.0,
        ).sort_index()
    )

    kpi = {
        "peak_hour": peak_hour,
        "peak_hour_revenue": peak_revenue
    }
    return {
        "hourly_revenue": hourly_revenue,
        "hourly_heatmap": heatmap,
        "hourly_kpi": kpi
    }

# === 4. Store Performance Analysis ===
def _store_performance(df: pd.DataFrame) -> Dict[str, Any]:
    # Store Performance:
    #  -> Revenue by store
    #  -> Top performing store
    #  -> Product mix per store

    df = df.copy()

    # Revenue  per store
    store_revenue = (
        df.groupby("store_id", dropna=False).agg(
            total_revenue=("revenue", "sum"),
            n_transaction=("transaction_id", "nunique"),
            total_items=("quantity", "sum")
        ).reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    if not store_revenue.empty:
        top_store_row = store_revenue.loc[0]
        top_store_id = top_store_row["store_id"]
        top_store_revenue = float(top_store_row["total_revenue"])
    else:
        top_store_id = None
        top_store_revenue = None

    #  Product mix per store (share of revenue by category)
    if "category" in df.columns:
        store_category = (
            df.groupby(["store_id", "category"], dropna=False)["revenue"].sum().reset_index().rename(columns={"revenue" : "category_revenue"})
        )

        store_totals = store_category.groupby("store_id")["category_revenue"].transform("sum")
        store_category["revenue_share"] = store_category["category_revenue"] / store_totals
        product_mix_per_store = store_category.sort_values(
            ["store_id", "revenue_share"], ascending=[True, False]
        )
    else:
        product_mix_per_store = pd.DataFrame(columns=["store_id", "category", "category_revenue", "revenue_share"])

    kpi = {
        "top_store_id": top_store_id,
        "top_store_revenue": top_store_revenue,
    }

    return {
        "store_revenue": store_revenue,
        "product_mix_per_store": product_mix_per_store,
        "store_kpi": kpi
    }

# === 5. Payment Method Analysis
def _payment_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # Payment:
    #  -> Payment method usage trend (daily count)
    #  -> Revenue composition by payment method

    df = df.copy()

    # Usage trend per day
    payment_trend = (
        df.groupby(["date_only", "payment_method"], dropna=False)["transaction_id"].nunique().reset_index().rename(columns={"transaction_id" : "n_transaction", "date_only" : "date"}).sort_values(["date", "payment_method"])
    )

    # Revenue Compotion
    payment_revenue = (
        df.groupby("payment_method", dropna=False).agg(
            total_revenue=("revenue", "sum"),
            n_transactions=("transaction_id", "nunique"),
        ).reset_index()
    )

    total_rev_all = payment_revenue["total_revenue"].sum()
    if total_rev_all > 0:
        payment_revenue["revenue_share"] = payment_revenue["total_revenue"] / total_rev_all
    else:
        payment_revenue["revenue_share"] = 0.0

    return{ 
        "payment_trend": payment_trend,
        "payment_revenue": payment_revenue
    }
  

# === 6. Customer Behavior ===
def _custumer_behavior(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # Customer behavior
    #  -> repeat customer (order > 1)
    #  -> new vs returning costumer ration

    df = df.copy()
    if "customer_id" not in df.columns:
        raise KeyError("Column 'customer_id' is required for customer behavior analysis" )
    
    customer_orders = (
        df.groupby("customer_id", dropna=False)["transaction_id"].nunique().reset_index().rename(columns={"transaction_id" : "n_transactions"})
    )

    n_customer = len(customer_orders)
    if n_customer == 0:
        summary = pd.DataFrame(
            [
                {
                    "n_customers": 0,
                    "n_new_customers": 0,
                    "n_repeat_customers": 0,
                    "repeat_ratio": 0.0,
                    "new_ration": 0.0,
                    "avg_transaction_per_customer": 0.0,
                }
            ]
        )

        return {"customer_behavior": summary}
    
    n_repeat = (customer_orders["n_transactions"] > 1).sum()
    n_new = (customer_orders["n_transactions"] == 1).sum()

    summary = pd.DataFrame(
        [
            {
                "n_customers": n_customer,
                "n_new_customers": n_new,
                "n_repeat_customers": n_repeat,
                "repeat_ratio": n_repeat / n_customer,
                "new_ratio": n_new / n_customer,
                "avg_transaction_per_customer": customer_orders["n_transactions"].mean(),
            }
        ]
    )

    return {"customer_behavior": summary}
    


    # MAIN ENTRY: generate_report
def generate_report(df: pd.DataFrame, output_path: str | Path, logger=None) -> Dict[str, Any]:
    # Run full analysis pipeline and save some CSV files

    # Parameters
    # df : pd.DataFrame
    # -> Feature engineering data.

    # output_path : str | Path
    # -> If the path ends in .csv → it is considered the "main report file".
    # -> The folder where the file is located will be used to store other CSV analyses.
    # If the path is a folder → we save the main report in:
    # -> --- <folder>/store_sales_analysis_report.csv ---

    # logger : logging.Logger, optional
    # -> Logger from setup_logger().
    
    # Returns

    # Dict[str, Any]
    # Dictionary containing various DataFrames and KPIs from the analysis results.

    if logger is None:
        logger = setup_logger()

    log_stage(logger, "Analysis Pipeline")
    output_path = Path(output_path)

    if output_path.suffix.lower() == ".csv":
        report_dir = output_path.parent
        main_report_path= output_path
    else:
        report_dir = output_path
        main_report_path = output_path / "store_sales_analysis_report.csv"

    report_dir.mkdir(parents=True, exist_ok=True)
    log_info(logger, f"Report will be saved under: {report_dir.resolve()}")

    # Makesure time feature available
    df_time = _ensure_time_feature(df)

    #  ---- 1. Daily ----
    log_info(logger, "Running daily analysis...")
    daily_result = _daily_analysis(df_time)
    daily_result["daily_summary"].to_csv(report_dir / "daily_summary.csv", index=False)

    daily_result["daily_top_products"].to_csv(report_dir / "daily_top_products.csv", index=False)

    #  --- 2. Weekly ---
    log_info(logger, "Running weekly analysis...")
    weekly_results = _weekly_analysis(df_time)
    weekly_results["dow_summary"].to_csv(report_dir / "dow_summary.csv", index=False)
    
    weekly_results["weekend_summary"].to_csv(report_dir / "weekend_summary.csv", index=False)

    #  --- 3. Hourly ---
    log_info(logger, "Running hourly analysis...")
    hourly_results = _hourly_analysis(df_time)
    hourly_results["hourly_revenue"].to_csv(
        report_dir / "hourly_revenue.csv", index=False)
    
    # Save heatmap as CSV pivot
    hourly_results["hourly_heatmap"].to_csv(
        report_dir / "hourly_revenue_heatmap.csv")
    
    #  --- 4. Store Performance ----
    log_info(logger, "Running store performance analysis...")
    store_results = _store_performance(df_time)
    store_results["store_revenue"].to_csv(
        report_dir / "store_revenue.csv", index=False
    )
    store_results["product_mix_per_store"].to_csv(
        report_dir / "store_revenue_mix.csv", index= False
    )

    # --- 5. Payment Method ---
    log_info(logger, "Running payment method analysis... ")
    payment_results = _payment_analysis(df_time)
    payment_results["payment_trend"].to_csv(
        report_dir / "payment_trend_daily.csv", index=False
    )
    payment_results["payment_revenue"].to_csv(
        report_dir / "payment_revenue_composition.csv", index=False
    )

    # --- 6. Customer Behavior ---
    log_info(logger, "Running customer behavior analysis...")
    customer_result = _custumer_behavior(df_time)
    customer_result["customer_behavior"].to_csv(
        report_dir / "customer_behavior_summary.csv", index=False 
    )

    # MAIN KPI SUMMARY (store_sales_analysis_report.csv)
    log_info(logger, "Building main KPI summary report...")

    total_revenue = float(df_time["revenue"].sum())
    total_transaction = int(df_time["transaction_id"].nunique())
    total_items = float(df_time["quantity"].sum())

    #  Busiest / slowest day & peak hour & top store
    weekly_kpi = weekly_results["weekly_kpi"]
    hourly_kpi = hourly_results["hourly_kpi"]
    store_kpi = store_results["store_kpi"]
    customer_summary = customer_result["customer_behavior"].loc[0].to_dict()

    main_report = pd.DataFrame(
        [
            {
                "total_revenue": total_revenue,
                "total_transactions" : total_transaction,
                "total_items": total_items,
                "avg_revenue_per_transaction": total_revenue / total_transaction
                if total_transaction > 0
                else 0.0,
                "busiest_day": weekly_kpi.get("busiest_day"),
                "busiest_day_revenue": weekly_kpi.get("busiest_day_revenue", 0.0),
                "slowest_day": weekly_kpi.get("slowest_day"),
                "slowest_day_revenue": weekly_kpi.get("slowest_day_revenue", 0.0),
                "peak_hour": hourly_kpi.get("peak_hour"),
                "peak_hour_revenue": hourly_kpi.get("peak_hour_revenue", 0.0),
                "top_store_id": store_kpi.get("top_store_id"),
                "top_store_revenue": store_kpi.get("top_store_revenue", 0.0),
                "n_customers": customer_summary.get("n_customers", 0),
                "n_new_customers": customer_summary.get("n_new_customers", 0),
                "n_repeat_customers": customer_summary.get("n_repeat_customers", 0),
                "repeat_ratio": customer_summary.get("repeat_ratio", 0.0),
                "new_ratio": customer_summary.get("new_ratio", 0.0),
                "avg_transaction_per_customer": customer_summary.get(
                    "avg_transaction_per_customer", 0.0
                )
            }
        ]
    )

    main_report.to_csv(main_report_path, index=False)
    log_success(logger, f"Main analysis report saved to {main_report_path}")

    #  Return all results if you want to use them in a notebook/testing

    all_results: Dict[str, Any] = {
        "daily": daily_result,
        "weekly": weekly_results,
        "hourly": hourly_results,
        "store": store_results,
        "payment": payment_results,
        "customer": customer_result,
        "main_report": main_report
    }

    return all_results
