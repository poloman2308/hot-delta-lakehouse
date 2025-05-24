# 🏗️ IoT Delta Lakehouse (Databricks + PySpark)

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
├── notebooks/ 
├── src/ 
├── data/
├── delta_tables/ # Delta output tables
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

## ⚙️ How to Use

### ▶️ 1. Run in Databricks

1. Upload all `notebooks/` into your Databricks Workspace
2. Create `/FileStore/iot_stream` for streaming simulation
3. Run notebooks in this order:

Bronze_Ingest_Batch → Silver_Transform → Gold_Aggregate

Or run them all at once via `Pipeline_Run_All`.

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

[iot_gold_export](https://community.cloud.databricks.com/files/gold_export/iot_gold_export.csv)

---

## 🧪 Sample Data Format

If you wish to test the pipeline locally, use this sample format for your CSV:

```
csv
sensor_id,timestamp,temperature,humidity,location
1001,2025-05-01 00:00:00,72.4,41.2,Room_A
1002,2025-05-01 00:00:05,75.1,45.9,Room_B
```

Save it as: data/sample_sensor_data.csv
You can manually drop this into /FileStore/iot_stream in Databricks

---

## ✨ Advanced Features Implemented

✅ Bronze/Silver/Gold data lake layers
✅ Structured Streaming ingestion
✅ Delta Time Travel with versioning
✅ Z-Ordering (manual simulation)
✅ Notebook chaining with %run
✅ CSV export for BI tools
🟨 (Optional) REST API with Flask
🟨 (Optional) Power BI integration via Databricks SQL

---

## 📝 License

This project is released under the MIT License.

---

## 👨‍💻 Author

Derek Acevedo
[GitHub](www.github.com/poloman2308)
[Linkedin](www.linkedin.com/in/derekacevedo86

---

## 💡 Contributions Welcome

Feel free to fork, improve, and submit pull requests to enhance streaming support, add cloud integrations, or expand use cases to real-time dashboards.

---

Let me know if you'd like a matching `requirements.txt`, a `LICENSE`, or help pushing this final version to GitHub.
