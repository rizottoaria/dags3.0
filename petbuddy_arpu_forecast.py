"""
rizottoaria__petbuddy_arpu_forecast

Прогноз накопительного ARPU на install для версии 1.0.24 (сегменты ALL / US / PH),
двумя методами: лог-модель (в dbt) и Prophet (в Python). Порядок задач:

  1) dbt_inputs      — пересобрать входные витрины прогноза
                       (mart_cohort_daily, mart_cohort_daily_country, mart_arpu_prophet_input).
                       Апстримы (dim_players/stg_events/fct_revenue_events) должен собрать
                       основной DAG rizottoaria__petbuddy_dbt_marts — здесь они НЕ пересобираются.
  2) prophet_forecast — Prophet по mart_arpu_prophet_input -> petbuddy_clean.arpu_prophet_raw.
  3) dbt_forecast    — собрать mart_arpu_forecast (лог-модель + LEFT JOIN свежего Prophet).

Запускать после основного DAG витрин. Креды CH — из Airflow Variables (CH_DBT_*).
dbt и prophet живут в /home/airflow/dbt-venv (см. Dockerfile образа airflow-custom).
"""
from datetime import datetime, timedelta
import os
import subprocess

from airflow.sdk import dag, task, Variable

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
PY_BIN = "/home/airflow/dbt-venv/bin/python"
PROJECT_DIR = "/opt/airflow/dags/dbt/petbuddy"
SCRIPT = "/opt/airflow/dags/scripts/arpu_prophet_forecast.py"
TARGET_DIR = "/tmp/dbt_arpu_fc/target"
LOG_DIR = "/tmp/dbt_arpu_fc/logs"


def _ch_env() -> dict:
    env = os.environ.copy()
    env["CH_HOST"] = Variable.get("CH_DBT_HOST", default="clickhouse")
    env["CH_USER"] = Variable.get("CH_DBT_USER", default="dbt")
    env["CH_PASSWORD"] = Variable.get("CH_DBT_PASSWORD")  # обязателен
    return env


def _run(cmd: list[str], env: dict, label: str) -> None:
    print(f"LOG === running: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print(res.stdout)
    if res.stderr:
        print(res.stderr)
    if res.returncode != 0:
        raise RuntimeError(f"LOG === {label} failed (exit={res.returncode})")


@dag(
    dag_id="rizottoaria__petbuddy_arpu_forecast",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "prophet", "clickhouse", "petbuddy", "forecast"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def petbuddy_arpu_forecast():

    @task(execution_timeout=timedelta(minutes=15))
    def dbt_inputs() -> None:
        env = _ch_env()
        env["DBT_TARGET_PATH"] = TARGET_DIR
        env["DBT_LOG_PATH"] = LOG_DIR
        _run([DBT_BIN, "build", "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR,
              "--target", "dev", "--select",
              "mart_cohort_daily", "mart_cohort_daily_country", "mart_arpu_prophet_input"],
             env, "dbt_inputs")

    @task(execution_timeout=timedelta(minutes=20))
    def prophet_forecast() -> None:
        _run([PY_BIN, SCRIPT], _ch_env(), "prophet_forecast")

    @task(execution_timeout=timedelta(minutes=15))
    def dbt_forecast() -> None:
        env = _ch_env()
        env["DBT_TARGET_PATH"] = TARGET_DIR
        env["DBT_LOG_PATH"] = LOG_DIR
        _run([DBT_BIN, "build", "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR,
              "--target", "dev", "--select", "mart_arpu_forecast"],
             env, "dbt_forecast")

    dbt_inputs() >> prophet_forecast() >> dbt_forecast()


petbuddy_arpu_forecast()
