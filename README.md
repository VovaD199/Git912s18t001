# 📦 README.md (Student Template Version — фінальний варіант)

````md
# 📊 Batch Data Pipeline Assignment

## 🎯 Goal

Your task is to complete a **batch data pipeline** for an e-commerce platform.

The pipeline should:

```text
read raw data → transform with PySpark → store in Parquet → run via Airflow
````

---

## 🧠 Business Context

An e-commerce company needs daily analytics:

```text
daily revenue by country
```

This dataset is used by:

* analysts
* dashboards
* data science teams

---

## 🧩 Your Task

You are given a **production-like pipeline with missing core logic**.

Your job is to implement the transformation so that the pipeline works correctly.

---

## 📁 Project Structure

```text
.
├── dags/                 # Airflow DAG (already implemented)
├── src/
│   ├── jobs/             # PySpark job (you must complete)
│   └── utils/            # config + validation (already implemented)
├── config/               # config.yaml
├── data/
│   ├── raw/              # input data
│   └── curated/          # output data
├── tests/                # unit tests
├── Dockerfile.airflow
├── docker-compose.yml
└── README.md
```

---

## 📥 Input Data

Located in:

```text
data/raw/
```

### users.csv

```text
user_id, country, registration_date
```

### orders.csv

```text
order_id, user_id, order_date, amount, status
```

---

## 📤 Expected Output

```text
data/curated/daily_revenue_by_country/
```

Partitioned by:

```text
order_date=YYYY-MM-DD/
```

Columns:

```text
order_date
country
total_revenue
completed_orders_count
unique_customers_count
avg_order_value
```

---

## ⚙️ What You Need to Implement

File:

```text
src/jobs/daily_revenue_by_country.py
```

### TODO

Implement:

```python
def transform(users_df, orders_df):
    """
    TODO:
    1. filter only completed orders
    2. join orders with users on user_id
    3. group by order_date and country
    4. calculate:
       - total_revenue
       - completed_orders_count
       - unique_customers_count
       - avg_order_value
    """
    pass
```

---

## 🚀 How to Run

### 1. Start the system

```bash
docker compose up --build
```

---

### 2. Open Airflow

```text
http://localhost:8080
```

Credentials:

```text
admin / admin
```

---

### 3. Run the pipeline

* Find DAG: `ecommerce_daily_revenue_pipeline`
* Trigger DAG

---

## 🧪 Run Tests

Run tests inside Docker:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow && PYTHONPATH=/opt/airflow pytest tests/"
```

---

## ✔ Success Criteria

Your solution is correct if:

```text
✔ all tests pass
✔ Airflow DAG runs successfully (all tasks are green)
✔ output folder is created
✔ Parquet files exist
✔ data is correct
```

---

## ❌ Common Mistakes

* using pandas instead of PySpark
* not filtering cancelled orders
* incorrect join
* wrong aggregations
* missing partitioning
* modifying DAG or infrastructure instead of job logic

---

## 🧠 Important Notes

* Do NOT modify:

  * Airflow DAG
  * Docker configuration
  * validation utilities

* Focus only on:

```text
transform() function
```

---

## 🟡 Bonus (Optional)

Implement a simple API:

```text
GET /revenue?date=YYYY-MM-DD
```

Requirements:

* read Parquet output
* return JSON

---

## 🏁 Final Result

You will build a **production-like batch data pipeline** using:

```text
PySpark + Airflow + Parquet
```
