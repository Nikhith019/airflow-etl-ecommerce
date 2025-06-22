# ecom_project - E-commerce Sales ETL Pipeline

This project implements an ETL pipeline for e-commerce sales data using **Apache Airflow**, **Python**, and **PostgreSQL**.

## Project Structure

- `dags/etl_pipeline_dag.py`: Airflow DAG defining ETL tasks.  
- `scripts/etl_pipeline.py`: Python ETL scripts for extract, transform, load.  
- `requirements.txt`: Python dependencies.  
- `.gitignore`: Files/folders to ignore.  
- `raw_sales.csv`: Input CSV file (add manually).  

## Setup & Usage

1. Clone the repository.

2. Set up your Python environment and install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Update PostgreSQL credentials in `scripts/etl_pipeline.py`.

4. Place your sales CSV file as `raw_sales.csv` in the project root.

5. Start Airflow components (scheduler and webserver):

   ```bash
   airflow scheduler
   airflow webserver
   ```

6. Trigger the `ecom_etl_pipeline` DAG from the Airflow UI.

## Notes

- Uses `SequentialExecutor` by default; for production, consider switching to `LocalExecutor` or `CeleryExecutor`.  
- Modify transformation logic in `scripts/etl_pipeline.py` as needed.  
- Ensure PostgreSQL is running and accessible.

---

Created by Nikhith Raju Konduru
