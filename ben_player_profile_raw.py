"""
rizottoaria__ben_player_profile_raw

Ежедневная сырая выгрузка таблицы "PlayerProfile" из Postgres (AWS RDS
opnsouls-prod, база ben, Airflow-conn opn_souls, read-only логин) в ClickHouse
raw.ben_player_profile.

Зачем: PlayerProfile — мост между аналитикой и аккаунтами. Его id (cuid) совпадает
с player_id в событиях (analytics_prod.events) и с profileId в
monetization_transactions, а userId ссылается на "User".id. Через эту таблицу
атрибуты User (в т.ч. campaignName — рекламная кампания установки) прицепляются
к игроку: events.player_id = PlayerProfile.id, PlayerProfile.userId = User.id.
Без неё raw.ben_user (ключ id/externalId) не джойнится к событиям напрямую.

Тянем только лёгкие колонки-идентификаторы (без тяжёлых jsonb resources/baseStats):
id, userId, nickname, serverId, isTest, power, lastUpdate. Каждая строка пишется как
JSON-текст в колонку data (String) — схема-гибко, изменение источника не ломает
загрузку. Достать поле: JSONExtractString(data, 'userId') и т.п.

Полный рефреш каждый прогон через staging + EXCHANGE TABLES (атомарная замена).
Креды ClickHouse — из Variable CH_DBT_PASSWORD (пользователь dbt, БД raw);
Postgres — из conn opn_souls (база ben).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

PG_CONN = "opn_souls"
PG_DB = "ben"
PG_TABLE = "PlayerProfile"
# только лёгкие идентификаторы — тяжёлые jsonb (resources, baseStats) не тянем
PG_COLS = ["id", "userId", "nickname", "serverId", "isTest", "power", "lastUpdate"]
CH_URL = "http://clickhouse:8123/"
CH_DB = "raw"
CH_TABLE = "ben_player_profile"
BATCH = 5000


@dag(
    dag_id="rizottoaria__ben_player_profile_raw",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["raw", "postgres", "clickhouse", "ben"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def ben_player_profile_raw():

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

        # ---- чтение Postgres ben."PlayerProfile" стримингом ----
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
        col_sql = ", ".join(f'"{col}"' for col in PG_COLS)
        insert_q = f"INSERT INTO {CH_DB}.{staging} (data) FORMAT JSONEachRow"
        total = 0
        try:
            cur = pg.cursor(name="ben_pp_cur")  # server-side курсор (стриминг)
            cur.itersize = BATCH
            cur.execute(f'SELECT {col_sql} FROM "{PG_TABLE}"')
            while True:
                batch = cur.fetchmany(BATCH)
                if not batch:
                    break
                body = "\n".join(
                    json.dumps(
                        {"data": json.dumps(dict(zip(PG_COLS, (jsonable(v) for v in row))),
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


ben_player_profile_raw()
