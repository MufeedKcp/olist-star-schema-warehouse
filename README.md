# E-Commerce Data Warehouse ETL Pipeline

### Transforming 1M+ rows of fragmented E-commerce data into a high-performance, analytical Star Schema.

---

Data is useless if it’s disconnected. This project solves the "Data Silo" problem by architecting a production-grade ETL pipeline that ingests, cleans, and models the E-Commerce dataset. By moving data from messy CSVs into a structured **PostgreSQL Star Schema**, we enable sub-second analytical queries and business intelligence.

**Deep Dive into Engineering Excellence:**  
If you are looking for a project that goes beyond basic "data moving," this is it. We implement:

- **Idempotent Ingestion:** Pipelines that can be re-run safely without duplicating data using **SQL Upsert (ON CONFLICT)** logic.

- **Star Schema Modeling:** Transforming 3rd-normal form data into **Fact and Dimension tables** for optimized warehouse performance.

- **Production-Grade Python:** Dictionary-based orchestration, **Bulk Loading** (method='multi'), and **Atomic Transactions** (engine.begin()).

- **Data Quality Enforcement:** Handling UTF-8 normalization, **Category-Specific Median Imputation**, and rigid Primary/Foreign key constraints.


---

### 1. Pipeline Execution


<p align="center">  
<img src="C:\Users\biten\OneDrive\Documents\python_learning\olist-ETL-Warehouse\docs/code_success" width="7000">  
</p>

  
The pipeline processing ~600,000 rows across 9 tables in under 40 seconds. 

### 2. Data Warehouse Schema (Star Schema)


<p align="center">  
<img src="C:\Users\biten\OneDrive\Documents\python_learning\olist-ETL-Warehouse\docs/star_schema_architecture" width="7000">  
</p>

  
The architecture of Fact and Dimension tables designed for analytical speed.

---

### User Instructions

1. **Clone the Repository:**

```
git clone https://github.com/yourusername/olist-etl-warehouse.git
 cd olist-etl-warehouse
```

2. Create .env file:

```
 DB_USER=your_postgres_user
 DB_PASSWORD=your_password
 DB_HOST=localhost
 DB_PORT=5432
 DB_NAME=olist_warehouse
``` 

3. **Run the Pipeline:**

```
python main.py
```
 
---
### Prerequisites

- Python 3.10+

- PostgreSQL 14+

- Download the dataset from kaggle

### Setup & Data Ingestion

1. **Download the [Olist Dataset here](https://www.google.com/url?sa=E&q=https%3A%2F%2Fwww.kaggle.com%2Fdatasets%2Folistbr%2Fbrazilian-ecommerce).

2. Place all CSV files into a folder named brazil_sales_datasets/.

3. download the dependencies
```
pip install pandas sqlalchemy psycopg2-binary python-dotenv
```
 
4. **Database Creation:** Create an empty database in Postgres before running the script.

---
### Contribution Expectations

Contributions are what make the open-source community an amazing place to learn.

1. Please open an issue first.

2. I am currently looking for help implementing **Apache Airflow** for orchestration or **dbt** for the transformation layer.
 
3. Please follow PEP8 guidelines for Python.

---

### Known Issues

- The source data contains typos. These were kept for schema-matching but should be Aliased in future iterations to decouple source errors from the warehouse.

- **Memory Constraints:** The current geolocation transformation uses Pandas in-memory grouping. On machines with <8GB RAM, this may require switching to SQL-based aggregation.

- Pandas Timedeltas are cast to Integers (.dt.days) to ensure PostgreSQL compatibility, which loses minute-level precision in delivery metrics.

---

## Support the Journey
If this project helped you understand ETL architecture or provided a template for your own warehouse, consider fueling the next build!

---