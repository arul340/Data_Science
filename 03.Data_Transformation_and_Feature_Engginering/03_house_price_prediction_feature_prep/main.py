import json
from pathlib import Path
from src.data_loader import load_data
from src.utils import setup_logging
from src.data_cleaning import clean_data, detect_outliers_and_anomalies, normalize_data
from src.analysis import generate_report
from src.feature_engineering import create_features



def main():
    # === Load Data ===

    # Load Config
    config_path = Path("config/settings.json")
    config = json.loads(config_path.read_text())

    # load logger 
    logger = setup_logging(config["log_path"])
    logger.info("==== Pipeline Initialized ====")

    # Load Data
    df = load_data(config["raw_data_path"])
    logger.info(f"Data loaded: {df.shape}")
    print (df.info())

    # === Clean Data and Anomaly ===
    df = clean_data(df, config["missing_value_strategy"])
    logger.info("Cleaning complete")

    df = detect_outliers_and_anomalies(df, config["parameters"])
    logger.info("Outlier & anomaly handling complete")

    # === Features Enggineering ===
    df = create_features(df)
    logger.info("Feature engineering complete")

    # === Normalization ===
    df = normalize_data(df, config["parameters"])
    logger.info("Normalization complete")

    # === Save processed data ===
    processed_data_path = Path(config["processed_data_path"]) / "house_price_cleaned.csv"
    processed_data_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(processed_data_path, index=False)
    logger.info(f"Data saved to {config["processed_data_path"]}")

    #  === Generate Report ===
    report_path = Path(config["report_path"]) / "report.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    generate_report(df, str(report_path))
    logger.info(f"Report saved to {config["report_path"]}")

    fig_path = Path(config["fig_path"])
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    generate_report(df, output_path="output/reports/report.csv", fig_path=str(fig_path))
    logger.info(f"Correlation heatmap saved to {fig_path}")


    logger.info("=== Pipeline Complete ===")
    

if __name__ == "__main__":
    main()