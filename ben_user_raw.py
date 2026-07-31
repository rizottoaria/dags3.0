"""
rizottoaria__ben_user_raw

Ежедневная сырая выгрузка таблицы "User" из Postgres (AWS RDS opnsouls-prod,
база ben, Airflow-conn opn_souls, read-only логин) в ClickHouse raw.ben_user.

Схема-гибко: каждая строка пишется как JSON-текст в колонку data (String), поэтому
изменение набора полей в источнике НЕ ломает загрузку и не требует миграций.
Достать поле: JSONExtractString(data, 'имя'), JSONExtractInt(data, 'id') и т.п.

Полный рефреш каждый прогон через staging + EXCHANGE TABLES (атомарная замена,
без пустого окна). Расписание — раз в сутки. Креды ClickHouse — из Variable
CH_DBT_PASSWORD (пользователь dbt, БД raw); Postgres — из conn opn_souls.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

PG_CONN = "opn_souls"
PG_DB = "ben"
PG_TABLE = "User"
CH_URL = "http://clickhouse:8123/"
CH_DB = "raw"
CH_TABLE = "ben_user"
BATCH = 5000


@dag(
    dag_id="rizottoaria__ben_user_raw",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["raw", "postgres", "clickhouse", "ben"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def ben_user_raw():

    @task(execution_timeout=timedelta(minutes=30))
    def sync() -> None:
        import datetime as dt
        import decimal
        import json

        import psycopg2
        import requests
        from airflow.sdk import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # ---- ClickHouse HTTP (пользователь dbt) ----
        auth = ("dbt", Variable.get("CH_DBT_PASSWORD"))

        def ch(query, data=None):
            r = requests.post(CH_URL, params={"query": query}, data=data,
                              auth=auth, timeout=180)
            if r.status_code != 200:
                raise RuntimeError(f"CH error {r.status_code}: {r.text[:500]}")
            return r

        staging = f"{CH_TABLE}__staging"
        ddl = ("(data String, _synced_at DateTime DEFAULT now()) "
               "ENGINE = MergeTree ORDER BY tuple()")
        ch(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")
        ch(f"CREATE TABLE IF NOT EXISTS {CH_DB}.{CH_TABLE} {ddl}")
        ch(f"DROP TABLE IF EXISTS {CH_DB}.{staging}")
        ch(f"CREATE TABLE {CH_DB}.{staging} {ddl}")

        # ---- чтение Postgres ben."User" стримингом ----
        def jsonable(v):
            if isinstance(v, (dt.datetime, dt.date, dt.time)):
                return v.isoformat()
            if isinstance(v, decimal.Decimal):
                return float(v)
            if isinstance(v, (bytes, memoryview)):
                return bytes(v).decode("utf-8", "replace")
            return v

        c = PostgresHook(postgres_conn_id=PG_CONN).get_connection(PG_CONN)
        pg = psycopg2.connect(host=c.host, port=c.port, user=c.login,
                              password=c.password, dbname=PG_DB, connect_timeout=30)
        insert_q = f"INSERT INTO {CH_DB}.{staging} (data) FORMAT JSONEachRow"
        total = 0
        try:
            # имена колонок — быстрым обычным курсором (у named-курсора
            # cur.description = None до первого fetch)
            c0 = pg.cursor()
            c0.execute(f'SELECT * FROM "{PG_TABLE}" LIMIT 0')
            cols = [d[0] for d in c0.description]
            c0.close()

            cur = pg.cursor(name="ben_user_cur")  # server-side курсор (стриминг)
            cur.itersize = BATCH
            cur.execute(f'SELECT * FROM "{PG_TABLE}"')
            while True:
                batch = cur.fetchmany(BATCH)
                if not batch:
                    break
                body = "\n".join(
                    json.dumps(
                        {"data": json.dumps(dict(zip(cols, (jsonable(v) for v in row))),
                                            ensure_ascii=False)},
                        ensure_ascii=False,
                    )
                    for row in batch
                )
                ch(insert_q, data=body.encode("utf-8"))
                total += len(batch)
        finally:
            pg.close()
        print(f'LOG === прочитано/записано строк "{PG_TABLE}": {total}')

        # ---- атомарная замена ----
        ch(f"EXCHANGE TABLES {CH_DB}.{CH_TABLE} AND {CH_DB}.{staging}")
        ch(f"DROP TABLE IF EXISTS {CH_DB}.{staging}")
        print(f"LOG === {CH_DB}.{CH_TABLE} обновлена: {total} строк")

    sync()


ben_user_raw()
