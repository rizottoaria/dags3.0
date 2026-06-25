import os
import requests
import pandas as pd
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BASE = "EUR"
OUTPUT_DIR = "tmp"
SPARK_CONTAINER = "spark-master"
SODA_CONTAINER  = "soda"
INGEST_SCRIPT   = "/tmp/ingest_currency_rates.py"
INGEST_SCRIPT_HOST = os.path.expanduser(
    "~/currency-lakehouse-v2/scripts/ingest_currency_rates.py"
)
AWS_CONN_ID = "s3_hello"

default_args = {
    "owner": "rizottoaria",
}


@dag(
    dag_id="currency_rates_etl",
    start_date=datetime(2026, 5, 15),
    schedule="0 5 * * *",
    catchup=False,
    default_args=default_args,
    tags=["currency", "lakehouse", "iceberg"],
)
def currency_rates_etl():

    @task
    def fetch_rates() -> dict:
        api_key = Variable.get("exchangerate_key")
        url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{BASE}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()

    @task
    def save_to_parquet(data: dict) -> str:
        df = pd.DataFrame(
            data["conversion_rates"].items(),
            columns=["target_currency", "rate"],
        )
        df["base_currency"] = data["base_code"]
        df["business_date"] = pd.to_datetime(data["time_last_update_utc"]).date()
        df["update_at"] = datetime.now(timezone.utc)
        df = df[["business_date", "base_currency", "target_currency", "rate", "update_at"]]
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        file_path = f"{OUTPUT_DIR}/currency_rates_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.parquet"
        df.to_parquet(file_path, index=False, engine="pyarrow", compression="snappy")
        print(f"{len(df)} строк в {file_path}")
        return file_path

    @task
    def upload_to_s3(file_path: str) -> str:
        filename = os.path.basename(file_path)
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        hook.load_file(
            filename=file_path,
            key=filename,
            bucket_name="dev",
            replace=True,
        )
        s3_path = f"s3://dev/{filename}"
        print(f"Загружено в {s3_path}")
        return s3_path

    @task
    def get_minio_creds() -> dict:
        """Читаем креды прямо из connection s3_hello."""
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(AWS_CONN_ID)
        return {
            "access_key": conn.login,
            "secret_key": conn.password,
        }

    @task
    def spark_ingest_to_iceberg(creds: dict) -> None:
        import subprocess
        result = subprocess.run(
            [
                "docker", "exec",
                "-e", f"MINIO_ACCESS_KEY={creds['access_key']}",
                "-e", f"MINIO_SECRET_KEY={creds['secret_key']}",
                SPARK_CONTAINER,
                "/opt/spark/bin/spark-submit",
                "--master", "spark://spark-master:7077",
                "--conf", "spark.sql.catalog.lakehouse.s3.region=us-east-1",
                "--conf", "spark.driver.extraJavaOptions=-Daws.region=us-east-1",
                "--conf", "spark.executor.extraJavaOptions=-Daws.region=us-east-1",
                INGEST_SCRIPT,
            ],
            capture_output=True,
            text=True,
        )
        print(result.stdout[-3000:] if result.stdout else "")
        print(result.stderr[-3000:] if result.stderr else "")
        if result.returncode != 0:
            raise Exception(f"spark-submit failed with code {result.returncode}")

    @task
    def refresh_soda_view(creds: dict) -> None:
        import subprocess
        # Пересоздаём init_duckdb.py с актуальными кредами
        init_script = f"""
import duckdb
con = duckdb.connect("/app/lakehouse.duckdb")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(\"\"\"
    CREATE OR REPLACE PERSISTENT SECRET minio_secret (
        TYPE S3,
        KEY_ID '{creds["access_key"]}',
        SECRET '{creds["secret_key"]}',
        REGION 'us-east-1',
        ENDPOINT '172.20.0.1:9000',
        URL_STYLE 'path',
        USE_SSL false
    )
\"\"\")
con.execute(\"\"\"
    CREATE OR REPLACE VIEW currency_rates AS
    SELECT * FROM read_parquet(
        's3://dev/iceberg-warehouse/raw/currency_rates/data/**/*.parquet',
        hive_partitioning=true
    )
\"\"\")
print("View updated:", con.execute("SELECT COUNT(*) FROM currency_rates").fetchone())
con.close()
"""
        # Записываем скрипт в контейнер и запускаем
        write = subprocess.run(
            ["docker", "exec", "-i", SODA_CONTAINER,
             "sh", "-c", "cat > /app/init_duckdb.py"],
            input=init_script, text=True, capture_output=True,
        )
        run = subprocess.run(
            ["docker", "exec", SODA_CONTAINER, "python3", "/app/init_duckdb.py"],
            capture_output=True, text=True,
        )
        print(run.stdout)
        if run.returncode != 0:
            raise Exception(f"DuckDB refresh failed: {run.stderr}")

    soda_scan = BashOperator(
        task_id="soda_dq_scan",
        bash_command=f"""
            docker exec {SODA_CONTAINER} \
              soda scan \
              -d trino_lakehouse \
              -c /app/configuration.yml \
              /app/checks/currency_rates.yml
        """,
    )

    # ── Pipeline ──────────────────────────────────────────────────────────────
    creds = get_minio_creds()

    copy_script = BashOperator(
        task_id="copy_ingest_script",
        bash_command=f"docker cp {INGEST_SCRIPT_HOST} {SPARK_CONTAINER}:{INGEST_SCRIPT}",
    )

    s3_path = upload_to_s3(save_to_parquet(fetch_rates()))
    s3_path >> copy_script >> spark_ingest_to_iceberg(creds) >> refresh_soda_view(creds) >> soda_scan


currency_rates_etl()