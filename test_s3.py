import io
import os
import subprocess
import requests
import pandas as pd
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BASE = "EUR"
OUTPUT_DIR = "tmp"
SPARK_CONTAINER = "spark-master"
SODA_CONTAINER  = "soda"
INGEST_SCRIPT   = "/tmp/ingest_currency_rates.py"
AWS_CONN_ID     = "s3_hello"

INGEST_SCRIPT_BODY = r"""
#!/usr/bin/env python3
import io, os, boto3, pyarrow.parquet as pq, pyarrow as pa, pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
from datetime import date as dt_date, timedelta

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://host.docker.internal:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY")
if not MINIO_ACCESS or not MINIO_SECRET:
    raise ValueError("MINIO_ACCESS_KEY и MINIO_SECRET_KEY должны быть переданы через -e")
MINIO_BUCKET   = "dev"
FILE_PREFIX    = "currency_rates_"
TARGET_TABLE   = "lakehouse.raw.currency_rates"

spark = (
    SparkSession.builder.appName("currency-rates-ingest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse",             "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type",        "rest")
    .config("spark.sql.catalog.lakehouse.uri",         "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lakehouse.warehouse",   "s3://dev/iceberg-warehouse/")
    .config("spark.sql.catalog.lakehouse.io-impl",     "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id",     MINIO_ACCESS)
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", MINIO_SECRET)
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} ready")

s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET)

keys = [
    obj["Key"]
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=MINIO_BUCKET, Prefix=FILE_PREFIX)
    for obj in page.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]
print(f"Found {len(keys)} files")

tables = []
for k in keys:
    buf = io.BytesIO(s3.get_object(Bucket=MINIO_BUCKET, Key=k)["Body"].read())
    tables.append(pq.read_table(buf))

arrow_table = pa.concat_tables(tables, promote_options="default")
print(f"Total rows: {len(arrow_table):,}")

ts_col   = arrow_table.column("update_at")
ts_utc   = pc.cast(ts_col, pa.timestamp("us", tz="UTC"))
ts_naive = pc.cast(ts_utc, pa.timestamp("us"))
arrow_table = arrow_table.set_column(
    arrow_table.schema.get_field_index("update_at"), "update_at", ts_naive)

date_ints   = arrow_table.column("business_date").cast(pa.int32()).to_pylist()
EPOCH       = dt_date(1970, 1, 1)
dates       = [EPOCH + timedelta(days=d) if d is not None else None for d in date_ints]
ts_ints     = ts_naive.to_pylist()
base_curr   = arrow_table.column("base_currency").to_pylist()
target_curr = arrow_table.column("target_currency").to_pylist()
rates       = arrow_table.column("rate").to_pylist()
rows        = list(zip(dates, base_curr, target_curr, rates, ts_ints))

schema = StructType([
    StructField("business_date",   DateType(),      True),
    StructField("base_currency",   StringType(),    True),
    StructField("target_currency", StringType(),    True),
    StructField("rate",            DoubleType(),    True),
    StructField("update_at",       TimestampType(), True),
])

df = spark.createDataFrame(rows, schema=schema)
df = df.dropDuplicates(["business_date", "base_currency", "target_currency"])
print(f"After dedup: {df.count():,}")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
table_exists = spark._jsparkSession.catalog().tableExists("lakehouse.raw.currency_rates")

if not table_exists:
    print("Creating table...")
    (df.writeTo(TARGET_TABLE)
        .partitionedBy(F.col("business_date"))
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .create())
    print("Created!")
else:
    print("Merging...")
    df.createOrReplaceTempView("incoming")
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t USING incoming s
        ON  t.business_date   = s.business_date
        AND t.base_currency   = s.base_currency
        AND t.target_currency = s.target_currency
        WHEN MATCHED AND t.rate != s.rate
            THEN UPDATE SET t.rate = s.rate, t.update_at = s.update_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Merged!")

spark.sql(f"SELECT COUNT(*) as total FROM {TARGET_TABLE}").show()
spark.sql(f"SELECT business_date, COUNT(*) as pairs FROM {TARGET_TABLE} GROUP BY 1 ORDER BY 1 DESC LIMIT 5").show()
print("Done.")
spark.stop()
"""

default_args = {"owner": "rizottoaria"}


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
        print(f"Загружено в s3://dev/{filename}")
        return f"s3://dev/{filename}"

    @task
    def get_minio_creds() -> dict:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(AWS_CONN_ID)
        return {"access_key": conn.login, "secret_key": conn.password}

    @task
    def copy_ingest_script() -> None:
        result = subprocess.run(
            ["docker", "exec", "-i", SPARK_CONTAINER,
             "sh", "-c", f"cat > {INGEST_SCRIPT}"],
            input=INGEST_SCRIPT_BODY,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            raise Exception(f"Failed to copy script: {result.stderr}")
        print(f"Script written to {SPARK_CONTAINER}:{INGEST_SCRIPT}")

    @task
    def spark_ingest_to_iceberg(creds: dict) -> None:
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
        subprocess.run(
            ["docker", "exec", "-i", SODA_CONTAINER,
             "sh", "-c", "cat > /app/init_duckdb.py"],
            input=init_script, text=True, capture_output=True,
        )
        result = subprocess.run(
            ["docker", "exec", SODA_CONTAINER, "python3", "/app/init_duckdb.py"],
            capture_output=True, text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"DuckDB refresh failed: {result.stderr}")

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
    s3_path = upload_to_s3(save_to_parquet(fetch_rates()))
    s3_path >> copy_ingest_script() >> spark_ingest_to_iceberg(creds) >> refresh_soda_view(creds) >> soda_scan


currency_rates_etl()