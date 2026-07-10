"""
rizottoaria__petbuddy_events_stream

Стрим текущих событий PetBuddy: Postgres.events -> Kafka topic petbuddy.events

Механика:
  * при самом первом запуске берётся MAX(id) и записывается в ANCHOR_VAR.
    Это граница: всё, что >= якоря -- зона стрима, всё что < -- зона бэкфилла.
  * дальше каждый run читает keyset-пагинацией: WHERE id > cursor ORDER BY id ASC
    -- всегда Index Scan using events_pkey, никакого Seq Scan.
  * курсор двигается ТОЛЬКО после успешного flush() в Kafka.
    Упали на середине -> следующий run переотправит батч -> дедуп в ClickHouse
    по idempotencyKey (ReplacingMergeTree).
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

BROKER = "185.182.9.18:9092"
TOPIC = "petbuddy.events"

PG_CONN_ID = "opn_souls"
PG_TABLE = "events"

ANCHOR_VAR = "petbuddy_events_anchor_id"
STREAM_VAR = "petbuddy_events_stream_cursor"
BACKFILL_VAR = "petbuddy_events_backfill_cursor"

BATCH = 5_000        # стрим лёгкий, большие батчи ему не нужны
MAX_LOOPS = 20       # потолок на run: 100k строк; остаток догонит следующий run
STMT_TIMEOUT_MS = 120_000

TO_SNAKE = True      # camelCase -> snake_case в ключах JSON


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
        "queue.buffering.max.messages": 200_000,
        "socket.keepalive.enable": True,
    })


def _pg_cursor(conn):
    import psycopg2.extras
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"SET statement_timeout = {STMT_TIMEOUT_MS}")
    return cur


# ---------------------------------------------------------------- dag

@dag(
    dag_id="rizottoaria__petbuddy_events_stream",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # критично: один писатель курсора
    tags=["petbuddy", "kafka", "stream"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
)
def stream():

    @task
    def ensure_anchor() -> str:
        """Один раз в жизни пайплайна: зафиксировать границу стрим/бэкфилл."""
        from airflow.sdk import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        anchor = Variable.get(ANCHOR_VAR, default="")
        if anchor:
            print(f"LOG === anchor уже зафиксирован: {anchor}")
            return anchor

        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn:
            with _pg_cursor(conn) as cur:
                cur.execute(f'SELECT max(id) AS m FROM "{PG_TABLE}"')
                anchor = cur.fetchone()["m"]

        if not anchor:
            raise RuntimeError("LOG === таблица events пуста")

        Variable.set(ANCHOR_VAR, anchor)
        Variable.set(STREAM_VAR, anchor)     # стрим пойдёт строго вверх: id > anchor
        Variable.set(BACKFILL_VAR, anchor)   # бэкфилл вниз, первый батч включит сам anchor
        print(f"LOG === якорь зафиксирован: {anchor}")
        print("LOG === стрим = (anchor, +inf) ; бэкфилл = (-inf, anchor]")
        return anchor

    @task
    def produce(anchor: str) -> int:
        from airflow.sdk import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        cursor_id = Variable.get(STREAM_VAR, default=anchor)
        producer = _make_producer()

        errors: list = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(str(err))

        sql = f'''
            SELECT * FROM "{PG_TABLE}"
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
        '''

        total = 0
        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn:
            with _pg_cursor(conn) as cur:
                for loop in range(MAX_LOOPS):
                    cur.execute(sql, (cursor_id, BATCH))
                    rows = cur.fetchall()
                    if not rows:
                        print(f"LOG === новых строк нет, cursor={cursor_id}")
                        break

                    for row in rows:
                        producer.produce(
                            TOPIC,
                            key=str(row["id"]).encode(),
                            value=_serialize(dict(row)),
                            on_delivery=on_delivery,
                        )
                        producer.poll(0)

                    remaining = producer.flush(120)
                    if remaining or errors:
                        raise RuntimeError(
                            f"LOG === flush не прошёл: in_queue={remaining}, "
                            f"errors={errors[:5]} -- курсор НЕ двигаем"
                        )

                    cursor_id = rows[-1]["id"]
                    Variable.set(STREAM_VAR, cursor_id)
                    total += len(rows)
                    print(f"LOG === loop={loop} produced={len(rows)} cursor -> {cursor_id}")

                    if len(rows) < BATCH:
                        print("LOG === догнали хвост таблицы")
                        break
                else:
                    print(f"LOG === достигнут MAX_LOOPS, остаток догонит следующий run")

        print(f"LOG === stream done, total={total}")
        return total

    produce(ensure_anchor())


stream()