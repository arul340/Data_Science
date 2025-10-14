# File Handler.Py module
from pathlib import Path
import csv


# =================================================
# Function for read sales data from CSV file
# =================================================
def read_sales(file_path: Path):
    # -----------------------------------------------
    # Read csv file(sales.csv) and return list of dict
    # Each line will map  automatically to a dict based haeader
    # -----------------------------------------------
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)


# =================================================
# Function for write summary data to CSV file
# =================================================   
def write_summary(file_path: Path, data: list[dict], fieldnames: list[str]):


    # Make sure folder exists
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

