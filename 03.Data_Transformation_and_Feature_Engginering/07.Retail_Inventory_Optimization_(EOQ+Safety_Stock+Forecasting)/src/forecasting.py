import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import SimpleExpSmoothing, Holt
from statsmodels.tsa.api import SimpleExpSmoothing as SESModel
from statsmodels.tsa.api import Holt as HoltModel

def forecast_sku(df, sku_id, product_name, output_path="output/forecast", periods=3):
    os.makedirs(output_path, exist_ok=True)

    sku_df = df[df["Product_ID"] == sku_id].copy()
    sku_df = sku_df.sort_values("Date")

    sku_df["Date"] = pd.to_datetime(sku_df["Date"])
    sku_df = sku_df.set_index("Date")
    sku_df = sku_df.asfreq("MS")     # 🔥 FIX: set frequency to Month Start

    y = sku_df["Qty_Sold"]

    models = {}
    rmse_scores = {}

    # ========== MODEL 1: Naive ==========
    try:
        naive_pred = np.repeat(y.iloc[-1], len(y))
        rmse_scores["Naive"] = np.sqrt(np.mean((y - naive_pred) ** 2))
    except:
        rmse_scores["Naive"] = np.inf

    # ========== MODEL 2: SMA (Simple Moving Avg) ==========
    try:
        sma = y.rolling(window=3).mean().dropna()
        repeated_pred = np.repeat(sma.iloc[-1], len(y))   # FIX: .iloc
        rmse_scores["SMA"] = np.sqrt(np.mean((y.iloc[-len(repeated_pred):] - repeated_pred) ** 2))
    except:
        rmse_scores["SMA"] = np.inf

    # ========== MODEL 3: SES ==========
    try:
        ses = SimpleExpSmoothing(y).fit()
        ses_pred = ses.fittedvalues
        rmse_scores["SES"] = np.sqrt(np.mean((y - ses_pred) ** 2))
        models["SES"] = ses
    except:
        rmse_scores["SES"] = np.inf

    # ========== MODEL 4: Holt Linear Trend ==========
    try:
        holt = Holt(y).fit()
        holt_pred = holt.fittedvalues
        rmse_scores["Holt"] = np.sqrt(np.mean((y - holt_pred) ** 2))
        models["Holt"] = holt
    except:
        rmse_scores["Holt"] = np.inf

    # ========== BEST MODEL ==========
    best_model_name = min(rmse_scores, key=rmse_scores.get)
    print(f"[BEST] {sku_id} → Best Model = {best_model_name}")

    if best_model_name in ["SES", "Holt"]:
        model = models[best_model_name]
        future_pred = model.forecast(periods)
    else:
        value = y.iloc[-1] if best_model_name == "Naive" else sma.iloc[-1]
        index_future = pd.date_range(start=y.index[-1] + pd.DateOffset(months=1), periods=periods, freq="MS")
        future_pred = pd.Series([value] * periods, index=index_future)

    # Save output
    output_file = os.path.join(output_path, f"{sku_id}_forecast.csv")
    future_pred.to_csv(output_file, header=["Forecast"])

    # Plot
    plt.figure(figsize=(10, 5))
    plt.plot(y.index, y.values, label="Historical")
    plt.plot(future_pred.index, future_pred.values, label="Forecast", linestyle="--")
    plt.title(f"Forecast → {sku_id} | {product_name}")
    plt.legend()

    plt.savefig(os.path.join(output_path, f"{sku_id}_forecast.png"))
    plt.close()   


def run_forecasting(df_timeseries, output_folder="output/forecasts/", forecast_horizon=3):
    os.makedirs(output_folder, exist_ok=True)

    sku_list = df_timeseries["Product_ID"].unique()

    print("\n====== FORECASTING STARTED ======\n")

    for sku in sku_list:
        sku_name = df_timeseries[df_timeseries["Product_ID"] == sku]["Product_Name"].iloc[0]

        print(f"[INFO] Forecasting → {sku} | {sku_name}")

        forecast_sku(
            df=df_timeseries,
            sku_id=sku,
            product_name=sku_name,
            output_path=output_folder,
            periods=forecast_horizon
        )

    print("\n=== FORECASTING COMPLETED ===\n")
