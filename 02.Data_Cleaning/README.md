# 📂 02_Data_Cleaning

This folder contains a complete collection of **Data Cleaning Pipelines**, built step-by-step to simulate how real-world data preparation evolves — from basic scripts to fully automated, production-ready systems.

Each mini project refines the previous one by adding **structure, modularity, configurability, and logging**, forming a solid foundation for any Data Engineer or Data Scientist.

---

## 🎯 Learning Objectives

By completing this module, you will be able to:

- Build complete **data cleaning pipelines** with reusable functions
- Handle **missing values, duplicates, outliers, and anomalies** efficiently
- Apply **normalization and data transformation** (MinMaxScaler)
- Manage **error handling** and **logging systems** in Python
- Implement **config-driven** dynamic pipelines using JSON settings
- Structure professional data projects with clear directories
- Produce **production-grade reports** with logs and summaries

---

## 🧱 Folder Structure

```text
02_Data_Cleaning/
├── 01.Basic_Full_Pipeline/
│ ├── data/
│ ├── logs/
│ ├── output/
│ └── main.py
│
├── 02.Modular_Full_Pipeline/
│ ├── data/
│ ├── logs/
│ ├── output/
│ └── main.py
│
├── 03.Logged_Full_Pipeline/
│ ├── data/
│ ├── logs/
│ ├── output/
│ └── main.py
│
├── 04.Configurable_Full_Pipeline/
│ ├── data/
│ ├── logs/
│ ├── output/
│ ├── config.json
│ └── main.py
│
├── 05.Production-Ready_Pipeline/
│ ├── data/
│ ├── logs/
│ ├── output/
│ ├── config.json
│ └── main.py
│
└── README.md

---
```

## 🧩 Project Breakdown

### 1️⃣ **Basic Full Pipeline**

A single-script pipeline to handle missing values, duplicates, and outliers using IQR.
Ideal for beginners to understand step-by-step cleaning flow.

### 2️⃣ **Modular Full Pipeline (Function-Based)**

Refactored into clean, reusable functions for better readability and debugging.

### 3️⃣ **Logged Full Pipeline (with Error Control)**

Adds structured logging, error handling (`try-except`), and automatic report generation.

### 4️⃣ **Configurable Full Pipeline (Dynamic Settings)**

Introduces `config.json` for parameterized control — no more hardcoded variables.

### 5️⃣ **Production-Ready Pipeline (Master Version)**

A complete, professional-grade data cleaning system with:

- Timestamped outputs
- Dynamic config
- Full logging and reporting
- Error recovery system
- Ready for deployment

---

## ⚙️ Tech Stack

| Category          | Tools / Libraries                            |
| ----------------- | -------------------------------------------- |
| Core              | Python 3.10+                                 |
| Data Handling     | pandas, numpy                                |
| Machine Learning  | scikit-learn (IsolationForest, MinMaxScaler) |
| Logging           | logging (built-in)                           |
| Config Management | JSON                                         |

---

## 📊 Example Log Output

```text
[INFO] Config file loaded successfully.
[INFO] Environment setup complete.
[INFO] Loading sales_jan.csv
[INFO] Total merged rows: 56
[INFO] Missing value handled: 6 filled
[INFO] Duplicates removed: 1
[INFO] No outliers detected in column Price
[INFO] Normalization complete.
[INFO] Saved cleaned data to output/final_cleaned_2025-10-30_08.45.31.csv
[INFO] Pipeline completed successfully!

---
```

## 🧑‍💻 Author

**Hasrul Sani (Arul)**
Learning Data Science for professional readiness — building portfolio one project at a time.
📍 _Indonesia_ | 🌐 [GitHub Profile](https://github.com/arul340)

---

## 🧭 Next Learning Stage

> Coming up next: `03_Data_Transformation_and_Feature_Engineering` ⚙️
> Focus: Feature encoding, scaling, dimensionality reduction, and dataset optimization for ML pipelines.

```

```
