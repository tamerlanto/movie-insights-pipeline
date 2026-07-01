# Movie Insights ETL Pipeline

An end-to-end data engineering pipeline that orchestrates data extraction, transformation, and loading (ETL) into a relational database.

## ?? Architecture Diagram & Tech Stack
* **Orchestration:** Apache Airflow (LocalExecutor)
* **Storage:** PostgreSQL 15 (Multi-database setup)
* **Containerization:** Docker & Docker Compose
* **Language:** Python 3.10

## ?? Project Structure
```text
movie-insights-pipeline/
+-- dags/
¦   L-- movie_pipeline_dag.py  # ETL logic & DAG definition
+-- init-scripts/
¦   L-- init.sh               # Database initialization script
+-- .gitignore
+-- README.md
L-- docker-compose.yaml
```

## ??? How to Run
1. Clone the repository and navigate to the directory:
   ```bash
   cd movie-insights-pipeline
   ```
2. Start the infrastructure:
   ```bash
   docker compose up -d
   ```
3. Initialize the Airflow meta-database (if running for the first time):
   ```bash
   docker compose run --rm airflow-webserver airflow db migrate
   ```
4. Create an admin user to access the UI:
   ```bash
   docker compose run --rm airflow-webserver airflow users create --username admin --firstname Data --lastname Engineer --email admin@example.com --role Admin --password admin
   ```
5. Open your browser at `http://localhost:8080`, log in with `admin/admin`, and trigger the **`movie_insights_pipeline`** DAG.

## ?? Database Verification
To verify that the data has been successfully processed and loaded into PostgreSQL, run:
```bash
docker compose exec postgres psql -U airflow -d movies_db -c "SELECT * FROM movie_reviews LIMIT 5;"
```
