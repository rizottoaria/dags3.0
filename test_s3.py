import os
import requests
import pandas as pd
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BASE = "EUR"
OUTPUT_DIR = "tmp"
default_args = {
    "owner": "rizottoaria",
}


@dag(
    dag_id="currency_rates_etl",
    start_date=datetime(2026, 5, 1),
    schedule="0 5 * * *",
    catchup=False,
    default_args=default_args,
    tags=["currency"],
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

        # Используем S3Hook
        hook = S3Hook(aws_conn_id='minios3_conn')
        hook.load_file(
            filename=file_path,
            key=filename,
            bucket_name='prod',
            replace=True,
        )

        s3_path = f"s3://prod/{filename}"
        print(f"Загружено в {s3_path}")
        return s3_path

    file_path = save_to_parquet(fetch_rates())
    upload_to_s3(file_path)


currency_rates_etl()
