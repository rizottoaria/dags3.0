import io
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime, timezone, date as dt_date, timedelta
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

BASE              = "EUR"
OUTPUT_DIR        = "tmp"
AWS_CONN_ID       = "s3_hello"
BUCKET            = "dev"
FILE_PREFIX       = "currency_rates_"
MINIO_IP          = "91.188.214.84"
ICEBERG_REST_URI  = "http://91.188.214.84:8181"
SODA_CONTAINER    = "soda"
TG_CHAT_ID        = "219022108"



def on_failure_callback(context):
    tg_token   = Variable.get("tg_bot_token")
    tg_chat_id = TG_CHAT_ID
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exc     = context.get("exception", "unknown error")
    msg = (
        "Airflow DAG failed\n"
        "DAG: " + dag_id + "\n"
        "Task: " + task_id + "\n"
        "Error: " + str(exc)[:200]
    )
    requests.post(
        "https://api.telegram.org/bot" + tg_token + "/sendMessage",
        json={"chat_id": tg_chat_id, "text": msg},
        timeout=10,
    )

def _get_creds():
    conn = BaseHook.get_connection(AWS_CONN_ID)
    return conn.login, conn.password


def _get_catalog(access_key, secret_key):
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        "lakehouse",
        **{
            "type": "rest",
            "uri": ICEBERG_REST_URI,
            "s3.endpoint": "http://" + MINIO_IP + ":9000",
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        }
    )


default_args = {
    "owner": "rizottoaria",
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="currency_rates_etl",
    start_date=datetime(2026, 5, 15),
    schedule="0 5 * * *",
    catchup=False,
    default_args=default_args,
    tags=["currency", "lakehouse", "iceberg"],
    on_failure_callback=on_failure_callback,
)
def currency_rates_etl():

    @task
    def fetch_rates() -> dict:
        api_key = Variable.get("exchangerate_key")
        url = "https://v6.exchangerate-api.com/v6/" + api_key + "/latest/" + BASE
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
        file_path = (
            OUTPUT_DIR + "/currency_rates_"
            + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            + ".parquet"
        )
        df.to_parquet(file_path, index=False, engine="pyarrow", compression="snappy")
        print(str(len(df)) + " строк в " + file_path)
        return file_path

    @task
    def upload_to_s3(file_path: str) -> str:
        import boto3
        access_key, secret_key = _get_creds()
        filename = os.path.basename(file_path)
        s3 = boto3.client(
            "s3",
            endpoint_url="http://" + MINIO_IP + ":9000",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        s3.upload_file(file_path, BUCKET, filename)
        print("Загружено в s3://" + BUCKET + "/" + filename)
        return filename

    @task
    def ingest_to_iceberg(filename: str) -> int:
        import boto3
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            NestedField, DateType, StringType, DoubleType, TimestamptzType
        )
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import DayTransform

        access_key, secret_key = _get_creds()

        s3 = boto3.client(
            "s3",
            endpoint_url="http://" + MINIO_IP + ":9000",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        buf = io.BytesIO(s3.get_object(Bucket=BUCKET, Key=filename)["Body"].read())
        arrow_table = pq.read_table(buf)
        print("Read " + str(len(arrow_table)) + " rows from " + filename)

        ts_utc = pc.cast(arrow_table.column("update_at"), pa.timestamp("us", tz="UTC"))
        arrow_table = arrow_table.set_column(
            arrow_table.schema.get_field_index("update_at"), "update_at", ts_utc)

        date_ints = arrow_table.column("business_date").cast(pa.int32()).to_pylist()
        EPOCH = dt_date(1970, 1, 1)
        dates = [EPOCH + timedelta(days=d) if d is not None else None for d in date_ints]
        arrow_table = arrow_table.set_column(
            arrow_table.schema.get_field_index("business_date"),
            "business_date",
            pa.array(dates, type=pa.date32())
        )

        catalog = _get_catalog(access_key, secret_key)

        try:
            catalog.create_namespace("raw")
        except Exception:
            pass

        table_id = "raw.currency_rates"
        iceberg_schema = Schema(
            NestedField(1, "business_date",   DateType(),        required=False),
            NestedField(2, "base_currency",   StringType(),      required=False),
            NestedField(3, "target_currency", StringType(),      required=False),
            NestedField(4, "rate",            DoubleType(),      required=False),
            NestedField(5, "update_at",       TimestamptzType(), required=False),
        )
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=1, field_id=1000,
                transform=DayTransform(), name="business_date_day"
            )
        )

        try:
            table = catalog.load_table(table_id)
            print("Table exists, upserting...")
            table.upsert(
                arrow_table,
                join_cols=["business_date", "base_currency", "target_currency"],
            )
            print("Upsert done!")
        except Exception as e:
            if "AlreadyExists" in str(e) or "already exists" in str(e).lower():
                raise
            print("Creating table: " + str(e))
            table = catalog.create_table(
                table_id,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy",
                }
            )
            table.append(arrow_table)
            print("Created and loaded!")

        count = table.scan().to_arrow().num_rows
        print("Total rows: " + str(count))
        return count

    @task
    def soda_dq_scan(count: int) -> None:
        print("Rows ingested: " + str(count))
        print("Soda DQ checks run independently via cron in soda container (hourly)")

    file_path = save_to_parquet(fetch_rates())
    filename  = upload_to_s3(file_path)
    count     = ingest_to_iceberg(filename)
    soda_dq_scan(count)


currency_rates_etl()