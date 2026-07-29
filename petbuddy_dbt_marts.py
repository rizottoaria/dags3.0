"""
rizottoaria__petbuddy_dbt_marts

Ежедневная пересборка dbt-витрин в схеме petbuddy_clean (ClickHouse):
staging -> intermediate -> marts. Запускает `dbt build` (run + тесты качества)
через изолированный venv образа (/home/airflow/dbt-venv) — у dbt-core свои
пины зависимостей, конфликтующие с Airflow, поэтому он вынесен в отдельный venv.

Проект деплоится вместе с DAG'ами в /opt/airflow/dags/dbt/petbuddy.
Креды ClickHouse берутся из Airflow Variables (CH_DBT_*), не хардкодятся.
target/logs пишем в /tmp, чтобы не трогать примонтированную папку dags.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
PROJECT_DIR = "/opt/airflow/dags/dbt/petbuddy"
TARGET_DIR = "/tmp/dbt_petbuddy/target"
LOG_DIR = "/tmp/dbt_petbuddy/logs"


@dag(
    dag_id="rizottoaria__petbuddy_dbt_marts",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "clickhouse", "petbuddy", "marts"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def petbuddy_dbt_marts():

    @task(execution_timeout=timedelta(minutes=30))
    def dbt_build() -> None:
        import os
        import subprocess

        from airflow.sdk import Variable

        env = os.environ.copy()
        env["CH_HOST"] = Variable.get("CH_DBT_HOST", default="clickhouse")
        env["CH_USER"] = Variable.get("CH_DBT_USER", default="dbt")
        env["CH_PASSWORD"] = Variable.get("CH_DBT_PASSWORD")  # обязателен
        env["DBT_TARGET_PATH"] = TARGET_DIR
        env["DBT_LOG_PATH"] = LOG_DIR

        cmd = [
            DBT_BIN, "build",
            "--project-dir", PROJECT_DIR,
            "--profiles-dir", PROJECT_DIR,
            "--target", "dev",
        ]
        print("LOG === running:", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"LOG === dbt build failed (exit={result.returncode})")
        print("LOG === dbt build passed")

    dbt_build()


petbuddy_dbt_marts()
