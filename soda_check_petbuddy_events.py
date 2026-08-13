"""
rizottoaria__soda_check_petbuddy_events

Периодическая проверка качества данных petbuddy.events в ClickHouse:
  * soda_scan       -- Soda Core чек-лист (schema/nulls/valid values/дубли
                        после ReplacingMergeTree-мержа/будущие даты)
  * compare_source_target_counts -- сверка источник vs приёмник.

С 2026-08-13 источник events-стрима переключён с Postgres на ClickHouse Cloud
(analytics_prod.events, conn petbuddy_ch_src, HTTP). Поэтому сверка переписана:

  * ПОЛНЫЙ count источник-vs-приёмник больше не годится: новый CH-источник хранит
    только свежие события (~40K с 2026-08-11), а petbuddy.events -- всю историю
    (~13M, включая старый Postgres-бэкфилл). Поэтому сравниваем count в ОКНЕ
    последних WINDOW_HOURS часов -- там оба содержат одни и те же события.
  * плюс проверка СВЕЖЕСТИ: лаг max(received_at) источника относительно приёмника.
    Это прямой детектор "стрим залип" (см. историю этого пайплайна).

Источник читается через HTTP-интерфейс CH (requests + X-ClickHouse-User/Key),
приёмник -- через mysql-протокол (conn soda_clickhouse), как и soda_scan.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

WINDOW_HOURS = 24     # окно для сверки count; в нём источник и приёмник совпадают
MAX_DRIFT = 5000      # строк; допустимый лаг стрима в окне (стрим ~5-мин цикл)
MAX_LAG_MIN = 20      # минут; допустимое отставание приёмника по max(received_at)

SRC_DB = "analytics_prod"
SRC_TABLE = "events"
CH_CONN_ID = "petbuddy_ch_src"      # HTTP-conn к ClickHouse Cloud (источник)


def _parse_ch_ts(raw: str) -> datetime:
    """Разбирает 'YYYY-MM-DD HH:MM:SS[.sss]' из CH в aware-datetime (UTC)."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"не разобрать CH-таймстамп: {raw!r}")


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
        import requests

        from airflow.hooks.base import BaseHook
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        # общий срез окна считаем в Python (UTC), одним литералом на оба источника,
        # чтобы не зависеть от рассинхрона часов между инстансами CH.
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # ---- источник: ClickHouse Cloud analytics_prod.events (HTTP) ----
        src = BaseHook.get_connection(CH_CONN_ID)

        def _ch_src(sql: str, params: dict) -> str:
            resp = requests.post(
                f"https://{src.host}:{src.port}/",
                params={**params, "database": SRC_DB},
                data=sql.encode("utf-8"),
                headers={
                    "X-ClickHouse-User": src.login,
                    "X-ClickHouse-Key": src.password or "",
                },
                timeout=60,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"LOG === CH src HTTP {resp.status_code}: {resp.text[:500]}")
            return resp.text.strip()

        src_count = int(_ch_src(
            f"SELECT count() FROM {SRC_DB}.{SRC_TABLE} FINAL "
            f"WHERE received_at >= parseDateTime64BestEffort({{cut:String}}, 3, 'UTC') "
            f"FORMAT TabSeparated",
            {"param_cut": cutoff},
        ))
        src_max = _parse_ch_ts(_ch_src(
            f"SELECT toString(max(received_at)) FROM {SRC_DB}.{SRC_TABLE} FINAL "
            f"FORMAT TabSeparated",
            {},
        ))

        # ---- приёмник: petbuddy.events на 185 (mysql-протокол) ----
        ch_info = MySqlHook(mysql_conn_id="soda_clickhouse").get_connection("soda_clickhouse")
        ch_conn = mysql.connector.connect(
            host=ch_info.host,
            port=ch_info.port,
            user=ch_info.login,
            password=ch_info.password,
            database=ch_info.schema,
        )
        cur = ch_conn.cursor()
        cur.execute("SELECT count() FROM events FINAL WHERE received_at >= %s", (cutoff,))
        tgt_count = cur.fetchone()[0]
        cur.execute("SELECT toString(max(received_at)) FROM events FINAL")
        tgt_max = _parse_ch_ts(cur.fetchone()[0])

        drift = src_count - tgt_count
        lag_min = (src_max - tgt_max).total_seconds() / 60.0
        print(
            f"LOG === window={WINDOW_HOURS}h cutoff={cutoff} "
            f"src(ch_cloud)={src_count} tgt(petbuddy)={tgt_count} drift={drift} ; "
            f"src_max={src_max.isoformat()} tgt_max={tgt_max.isoformat()} lag_min={lag_min:.1f}"
        )

        # приёмник не может иметь СУЩЕСТВЕННО больше строк в окне, чем источник.
        if tgt_count - src_count > MAX_DRIFT:
            raise RuntimeError(
                f"LOG === приёмник ({tgt_count}) в окне {WINDOW_HOURS}h имеет БОЛЬШЕ строк, "
                f"чем источник ({src_count}) -- вероятны дубли/порча"
            )
        if drift > MAX_DRIFT:
            raise RuntimeError(
                f"LOG === дрифт великоват: src={src_count} tgt={tgt_count} "
                f"drift={drift} > MAX_DRIFT={MAX_DRIFT} (в окне {WINDOW_HOURS}h)"
            )
        if lag_min > MAX_LAG_MIN:
            raise RuntimeError(
                f"LOG === приёмник отстаёт по received_at на {lag_min:.1f} мин "
                f"> MAX_LAG_MIN={MAX_LAG_MIN} -- вероятно стрим залип"
            )
        print("LOG === source/target: count в окне и свежесть в допуске")

    soda_scan()
    compare_source_target_counts()


soda_check()
