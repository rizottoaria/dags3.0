"""
rizottoaria__soda_check_petbuddy_events

Периодическая проверка качества данных petbuddy.events в ClickHouse:
  * soda_scan       -- Soda Core чек-лист (schema/nulls/valid values/дубли
                        после ReplacingMergeTree-мержа/будущие даты)
  * compare_source_target_counts -- сверка количества строк Postgres.events
                        (источник) vs ClickHouse petbuddy.events (таргет).
                        Postgres -- живая база под постоянной записью, так
                        что точное совпадение не гарантировано в моменте --
                        допускаем небольшой лаг (см. MAX_DRIFT).
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

MAX_DRIFT = 5000  # строк; примерно один цикл стрима с запасом


@dag(
    dag_id="rizottoaria__soda_check_petbuddy_events",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["soda", "data-quality", "clickhouse"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def soda_check():

    @task
    def soda_scan() -> None:
        import subprocess
        import tempfile
        from pathlib import Path

        from airflow.providers.mysql.hooks.mysql import MySqlHook

        conn = MySqlHook(mysql_conn_id="soda_clickhouse").get_connection("soda_clickhouse")

        config = f"""\
data_source petbuddy_clickhouse:
  type: mysql
  host: {conn.host}
  port: {conn.port}
  username: {conn.login}
  password: {conn.password}
  database: {conn.schema}
"""
        checks_path = Path(__file__).parent / "soda" / "checks.yml"

        with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as f:
            f.write(config)
            config_path = f.name

        result = subprocess.run(
            ["soda", "scan", "-d", "petbuddy_clickhouse", "-c", config_path, str(checks_path)],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"LOG === soda scan failed (exit={result.returncode})")
        print("LOG === soda scan passed")

    @task
    def compare_source_target_counts() -> None:
        import mysql.connector
        import psycopg2

        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_conn = PostgresHook(postgres_conn_id="opn_souls").get_conn()
        pg_cur = pg_conn.cursor()
        pg_cur.execute('SELECT count(*) FROM "events"')
        pg_count = pg_cur.fetchone()[0]

        ch_conn_info = MySqlHook(mysql_conn_id="soda_clickhouse").get_connection("soda_clickhouse")
        ch_conn = mysql.connector.connect(
            host=ch_conn_info.host,
            port=ch_conn_info.port,
            user=ch_conn_info.login,
            password=ch_conn_info.password,
            database=ch_conn_info.schema,
        )
        ch_cur = ch_conn.cursor()
        ch_cur.execute("SELECT count() FROM events FINAL")
        ch_count = ch_cur.fetchone()[0]

        drift = pg_count - ch_count
        print(f"LOG === postgres={pg_count} clickhouse={ch_count} drift={drift}")

        if ch_count > pg_count:
            raise RuntimeError(
                f"LOG === clickhouse ({ch_count}) has MORE rows than postgres ({pg_count}) "
                "-- likely duplication/corruption"
            )
        if drift > MAX_DRIFT:
            raise RuntimeError(
                f"LOG === drift too large: postgres={pg_count} clickhouse={ch_count} "
                f"drift={drift} > MAX_DRIFT={MAX_DRIFT}"
            )
        print("LOG === source/target counts within tolerance")

    soda_scan()
    compare_source_target_counts()


soda_check()
