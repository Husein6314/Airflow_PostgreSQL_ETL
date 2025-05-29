# 🛠️ Airflow ETL Pipeline - Customer Data

![Python](https://img.shields.io/badge/Python-3.11-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Airflow-DAG-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

---

## 📋 Project Overview

This project demonstrates a complete **ETL pipeline** built with **Apache Airflow**, which:

- Extracts customer data from a CSV file.
- Transforms and cleans the data using pandas.
- Loads the transformed data into a PostgreSQL table.
- Sends email notifications on success or failure.

> Ideal for learning how to automate data pipelines with Airflow, pandas, and PostgreSQL.

---

## 📁 Folder Structure

.
├── dags/
│ └── etl_customer_data.py # The Airflow DAG
├── data/
│ └── customer_data.csv # Source data (external)
├── README.md # You're here!
---

## 🚀 Technologies Used

- **Python 3.11**
- **Apache Airflow**
- **PostgreSQL**
- **pandas**
- **Bash**
- **EmailOperator (SMTP)**

---

## 🔄 DAG Tasks Breakdown

| Task ID             | Description                                          |
|---------------------|------------------------------------------------------|
| `check_file`        | Verifies if `customer_data.csv` exists               |
| `create_table`      | Creates `customer_data` table if it doesn't exist    |
| `extract_data`      | Reads CSV and stores in temporary raw format         |
| `transform_data`    | Cleans & transforms data (valid phone, country map)  |
| `load_data`         | Inserts data into PostgreSQL using `ON CONFLICT`     |
| `send_success_email`| Sends notification if pipeline succeeds              |
| `send_failure_email`| Sends notification if any task fails                 |

---

## ⚙️ Prerequisites

Make sure you have the following installed:

- [Apache Airflow](https://airflow.apache.org/docs/)
- Python 3.11
- PostgreSQL (with a database & table setup)
- A working SMTP configuration for email alerts

---
## Activate your virtual environment and start Airflow:


source ~/airflow_venv/bin/activate
airflow db init
airflow webserver --port 8080
airflow scheduler


## Place the source CSV file at:

~/Downloads/customer_data.csv
## 📥 How to Run

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/airflow-customer-etl.git
   cd airflow-customer-etl

# 🙋 Author
Hussein
📧 HusseinHadliye@gmail.com
🔗 LinkedIn: https://www.linkedin.com/in/hussein-hadliye/
