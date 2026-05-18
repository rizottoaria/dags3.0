import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "oksana",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


class S3NewFilesSensor(BaseSensorOperator):
    """
    Ждёт появления в S3 файлов, которых ещё нет в таблице `files`.
    Список новых ключей пушит в XCom под именем 'new_files'.
    """
    template_fields = ("bucket_name", "prefix")

    def __init__(
        self,
        bucket_name: str,
        prefix: str = "",
        aws_conn_id: str = s3_hello,
        postgres_conn_id: str = "postgres_default",
        table: str = "files",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.table = table

    def poke(self, context) -> bool:
        self.log.info("Проверяю s3://%s/%s", self.bucket_name, self.prefix)

        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        keys = s3.list_keys(bucket_name=self.bucket_name, prefix=self.prefix) or []
        keys = [k for k in keys if not k.endswith("/")]  # отбрасываем "папки"

        if not keys:
            self.log.info("Бакет пуст.")
            return False

        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        known = {r[0] for r in pg.get_records(f"SELECT name FROM {self.table}")}

        new_keys = sorted(set(keys) - known)
        if not new_keys:
            self.log.info("Новых файлов нет (всего в S3: %d).", len(keys))
            return False

        self.log.info("Найдено новых файлов: %d. Примеры: %s",
                      len(new_keys), new_keys[:5])
        context["ti"].xcom_push(key="new_files", value=new_keys)
        return True


def insert_files(**context):
    new_files = context["ti"].xcom_pull(
        task_ids="wait_for_new_files", key="new_files"
    )
    if not new_files:
        return

    pg = PostgresHook(postgres_conn_id="postgres_default")
    # ON CONFLICT защитит от гонок, если файл успел появиться дважды
    sql = "INSERT INTO files (name) VALUES (%s) ON CONFLICT (name) DO NOTHING"
    with pg.get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, [(name,) for name in new_files])
        conn.commit()

with DAG(
    dag_id="s3_to_postgres_files",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["s3", "postgres", "sensor"],
) as dag:

    wait_for_new_files = S3NewFilesSensor(
        task_id="wait_for_new_files",
        bucket_name="dev",
        prefix="",
        aws_conn_id="s3_hello",
        postgres_conn_id="postgres_default",
        table="files",
        poke_interval=30,
        timeout=60 * 30,          # ждём максимум 30 минут
        mode="reschedule",        # освобождает слот между пробами
        soft_fail=True,           # если не дождались — SKIPPED, не FAILED
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=insert_files,
    )

    wait_for_new_files >> load_to_postgres
