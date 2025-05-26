# 🏗️ IoT Delta Lakehouse (Databricks + PySpark)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.3.2-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.3.0-brightgreen)](https://delta.io/)

A production-grade Delta Lakehouse pipeline that ingests, transforms, and analyzes IoT sensor logs using Apache Spark, Delta Lake, and Databricks Community Edition.

> Supports both batch and streaming ingestion, layered data architecture (Bronze/Silver/Gold), time travel, Z-Ordering, and external CSV export.

---

## 📦 Tech Stack

- **Apache Spark (PySpark)** – distributed data processing
- **Delta Lake** – ACID transactions and data versioning
- **Databricks** – notebook orchestration and Delta integration
- **Structured Streaming** – real-time file ingestion
- **Pandas + Flask** *(optional)* – lightweight API server
- **Power BI** *(optional)* – BI/visualization layer

---

## 📁 Project Structure

```
iot-delta-lakehouse/
├── notebooks/ # All Databricks notebooks (Bronze, Silver, Gold, etc.)
├── src/ # Python scripts for local Delta processing
├── data/ # Raw CSV files (ignored in version control)
├── delta_tables/ # Output Delta tables (ignored in version control)
├── .gitignore
├── README.md
└── requirements.txt
```

> ✅ All raw and output data folders are excluded from version control.

---

## 🏗️ Layered Architecture

| Layer  | Description                                      |
|--------|--------------------------------------------------|
| Bronze | Raw sensor logs (batch + streaming support)      |
| Silver | Cleaned, deduplicated, schema-validated data     |
| Gold   | Aggregated hourly temperature + humidity metrics |

---

<details> <summary>📈 Click to expand Mermaid diagram code</summary>

```mermaid
flowchart TD
  subgraph Ingestion
    A1[Raw CSV Files<br>(sensor_logs.csv)]
    A2[Streaming CSV<br>Drop into /iot_stream]
  end

  subgraph Bronze Layer
    B1[Bronze_Ingest_Batch.py]
    B2[Bronze_Ingest_Stream.py]
    A1 --> B1
    A2 --> B2
    B1 & B2 --> BZ[Delta Table<br>bronze/sensor_logs]
  end

  subgraph Silver Layer
    C1[Silver_Transform.py]
    BZ --> C1
    C1 --> SZ[Delta Table<br>silver/sensor_logs]
  end

  subgraph Gold Layer
    D1[Gold_Aggregate.py]
    SZ --> D1
    D1 --> GZ[Delta Table<br>gold/sensor_hourly_avg]
  end

  subgraph Export
    GZ --> E1[CSV Export<br>iot_gold_export.csv]
    E1 --> EXT[Download via<br>Databricks FileStore]
  end

  classDef delta fill:#e6f7ff,stroke:#0099cc,stroke-width:2px;
  class BZ,SZ,GZ delta;
```
</details>

---

## ⚙️ How to Use

### ▶️ 1. Run in Databricks

1. Upload all `notebooks/` into your Databricks Workspace.
2. Create `/FileStore/iot_stream` to simulate streaming input.
3. Run the following notebooks in order:
   - `Bronze_Ingest_Batch`
   - `Silver_Transform`
   - `Gold_Aggregate`
   - Or use `Pipeline_Run_All` to execute them sequentially.

---

### ⚡ 2. Simulate Streaming Ingestion

- Drop a new CSV file (e.g., `batch1.csv`) into `/FileStore/iot_stream`
- It will be ingested in real time by `Bronze_Ingest_Stream`
- Rerun Silver and Gold notebooks to propagate results

---

### 📤 3. Export Gold Layer to CSV

Use the `Gold_Export_to_CSV` notebook (or `src/export_gold_csv.py`) to:
- Export the `sensor_hourly_avg` table
- Rename and download via:

👉 [iot_gold_export](https://community.cloud.databricks.com/files/gold_export/iot_gold_export.csv)

---

## 🧪 Sample Data Format

To test the pipeline locally or in Databricks, create a file named:

```text
data/sample_sensor_data.csv
```
With the following content:
```csv
sensor_id,timestamp,temperature,humidity,location
1001,2025-05-01 00:00:00,72.4,41.2,Room_A
1002,2025-05-01 00:00:05,75.1,45.9,Room_B
```
You can manually drop this into /FileStore/iot_stream to simulate a stream event.

---

## ✨ Advanced Features Implemented

- [x] Bronze/Silver/Gold data lake layers
- [x] Structured Streaming ingestion
- [x] Delta Time Travel with versioning
- [x] Z-Ordering (manual simulation)
- [x] Notebook chaining with `%run`
- [x] CSV export for BI tools
- [ ] (Optional) REST API with Flask
- [ ] (Optional) Power BI integration via Databricks SQL

---

## 👨‍💻 Author

Derek Acevedo
[GitHub](www.github.com/poloman2308) • [LinkedIn](www.linkedin.com/in/derekacevedo86)

---

## 💡 Contributions Welcome

Feel free to fork, improve, and submit pull requests to enhance streaming support, add cloud integrations, or expand use cases to real-time dashboards.
