"""
rizottoaria__petbuddy_events_backfill

Догоняет всю историю: всё, что <= якоря, поставленного стримовым DAG'ом.
Идёт вниз keyset-пагинацией: WHERE id < cursor ORDER BY id DESC.
Первый батч использует <=, чтобы захватить саму строку-якорь.

Схема JSON на выходе идентична стриму -- ClickHouse читает один топик
одной MV, дедуп по idempotencyKey (ReplacingMergeTree).

DAG сам себя выключает: когда упирается в начало таблицы, пишет DONE_VAR
и дальше все runs мгновенно skip'аются.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

BROKER = "185.182.9.18:9092"
TOPIC = "petbuddy.events"

PG_CONN_ID = "opn_souls"
PG_TABLE = "events"

ANCHOR_VAR = "petbuddy_events_anchor_id"
BACKFILL_VAR = "petbuddy_events_backfill_cursor"
DONE_VAR = "petbuddy_events_backfill_done"

BATCH = 20_000       # JSONB properties+context тяжёлые, 50k раздувает память
MAX_LOOPS = 100      # до 2M строк за run -> 11M за ~6 runs
STMT_TIMEOUT_MS = 120_000

TO_SNAKE = True


# ---------------------------------------------------------------- helpers

def _snake(name: str) -> str:
    import re
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _serialize(row: dict) -> bytes:
    import json
    from datetime import date, datetime as dt
    from decimal import Decimal

    def default(o):
        if isinstance(o, (dt, date)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        return str(o)

    if TO_SNAKE:
        row = {_snake(k): v for k, v in row.items()}
    return json.dumps(row, ensure_ascii=False, default=default).encode("utf-8")


def _make_producer():
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": BROKER,
        "linger.ms": 100,
        "compression.type": "lz4",
        "enable.idempotence": True,
        "acks": "all",
        "batch.size": 1 << 20,
        "message.max.bytes": 10 << 20,
        "queue.buffering.max.messages": 500_000,
        "socket.keepalive.enable": True,
    })


def _pg_cursor(conn):
    import psycopg2.extras
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"SET statement_timeout = {STMT_TIMEOUT_MS}")
    return cur


# ---------------------------------------------------------------- dag

@dag(
    dag_id="rizottoaria__petbuddy_events_backfill",
    schedule=timedelta(minutes=10),   # или None + ручные триггеры
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["petbuddy", "kafka", "backfill"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def backfill():

    @task
    def run_backfill() -> int:
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if Variable.get(DONE_VAR, default_var="") == "1":
            raise AirflowSkipException("LOG === бэкфилл завершён, skip")

        anchor = Variable.get(ANCHOR_VAR, default_var="")
        if not anchor:
            raise RuntimeError(
                "LOG === якорь не поставлен. Сначала прогони "
                "rizottoaria__petbuddy_events_stream хотя бы один раз."
            )

        cursor_id = Variable.get(BACKFILL_VAR, default_var=anchor)

        # первый батч в жизни бэкфилла: курсор всё ещё равен якорю ->
        # включаем саму строку-якорь, иначе она провалится в дыру
        op = "<=" if cursor_id == anchor else "<"
        print(f"LOG === старт: anchor={anchor} cursor={cursor_id} op={op}")

        producer = _make_producer()
        errors: list = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(str(err))

        total = 0
        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn:
            with _pg_cursor(conn) as cur:
                for loop in range(MAX_LOOPS):
                    sql = f'''
                        SELECT * FROM "{PG_TABLE}"
                        WHERE id {op} %s
                        ORDER BY id DESC
                        LIMIT %s
                    '''
                    cur.execute(sql, (cursor_id, BATCH))
                    rows = cur.fetchall()
                    if not rows:
                        Variable.set(DONE_VAR, "1")
                        print("LOG === история вычерпана до дна, DONE")
                        break

                    for row in rows:
                        producer.produce(
                            TOPIC,
                            key=str(row["id"]).encode(),
                            value=_serialize(dict(row)),
                            on_delivery=on_delivery,
                        )
                        producer.poll(0)

                    remaining = producer.flush(300)
                    if remaining or errors:
                        raise RuntimeError(
                            f"LOG === flush не прошёл: in_queue={remaining}, "
                            f"errors={errors[:5]} -- курсор НЕ двигаем"
                        )

                    cursor_id = rows[-1]["id"] 
                    Variable.set(BACKFILL_VAR, cursor_id)
                    op = "<"             
                    total += len(rows)
                    print(f"LOG === loop={loop} produced={len(rows)} cursor -> {cursor_id}")

                    if len(rows) < BATCH:
                        Variable.set(DONE_VAR, "1")
                        print("LOG === последний неполный батч, DONE")
                        break
                else:
                    print("LOG === MAX_LOOPS, продолжим на следующем run")

        print(f"LOG === backfill run done, total={total}")
        return total

    run_backfill()


backfill()