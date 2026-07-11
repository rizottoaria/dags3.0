"""
rizottoaria__petbuddy_sessions_stream

Постгрес public.sessions -> Kafka topic petbuddy.sessions.

Отличие от events: sessions -- МУТАБЕЛЬНАЯ таблица (id -- случайный UUID, а не
инкремент; status active->paused->ended, дозаполняется endAt, растёт resumeCount).
Поэтому:
  * курсор по (updatedAt, id) -- ловит и новые сессии, и апдейты старых.
    Составной ключ, потому что updatedAt не уникален (у пачки строк один updatedAt).
  * стартуем от эпохи -> первый прогон подметает всю таблицу по возрастанию
    updatedAt (это и есть бэкфилл), дальше каждый run догоняет свежие изменения.
    Отдельный backfill-DAG не нужен: одна восходящая развёртка = бэкфилл + хвост.
  * версия дедупа в ClickHouse = updated_at (ReplacingMergeTree) -> в CH всегда
    последнее состояние сессии.

Курсор двигается ТОЛЬКО после успешного flush() в Kafka: упали на середине ->
следующий run переотправит батч -> дедуп по id в ClickHouse.

Индекса на updatedAt в источнике нет (только PK и (playerId,status)) -> запрос
идёт Seq Scan + top-N sort. На 94k строк это доли секунды, приемлемо.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

BROKER = "kafka:9092"
TOPIC = "petbuddy.sessions"

PG_CONN_ID = "opn_souls"
PG_TABLE = "sessions"

CURSOR_TS_VAR = "petbuddy_sessions_stream_cursor_ts"
CURSOR_ID_VAR = "petbuddy_sessions_stream_cursor_id"

EPOCH_TS = "1970-01-01T00:00:00+00:00"

BATCH = 5_000
MAX_LOOPS = 30       # 150k строк за run -> вся таблица (94k) уходит за первый прогон
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
    dag_id="rizottoaria__petbuddy_sessions_stream",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # критично: один писатель курсора
    tags=["petbuddy", "kafka", "stream", "sessions"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
)
def stream():

    @task
    def produce() -> int:
        from airflow.sdk import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        cursor_ts = Variable.get(CURSOR_TS_VAR, default=EPOCH_TS)
        cursor_id = Variable.get(CURSOR_ID_VAR, default="")
        print(f"LOG === старт с курсора ({cursor_ts}, {cursor_id!r})")

        producer = _make_producer()
        errors: list = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(str(err))

        # составной keyset: (updatedAt, id) строго больше курсора.
        # row-value comparison в постгресе -> лексикографический порядок.
        sql = f'''
            SELECT * FROM "{PG_TABLE}"
            WHERE ("updatedAt", id) > (%s::timestamptz, %s)
            ORDER BY "updatedAt" ASC, id ASC
            LIMIT %s
        '''

        total = 0
        with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as conn:
            with _pg_cursor(conn) as cur:
                for loop in range(MAX_LOOPS):
                    cur.execute(sql, (cursor_ts, cursor_id, BATCH))
                    rows = cur.fetchall()
                    if not rows:
                        print(f"LOG === новых/изменённых строк нет, cursor=({cursor_ts}, {cursor_id!r})")
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

                    last = rows[-1]
                    cursor_ts = last["updatedAt"].isoformat()
                    cursor_id = str(last["id"])
                    Variable.set(CURSOR_TS_VAR, cursor_ts)
                    Variable.set(CURSOR_ID_VAR, cursor_id)
                    total += len(rows)
                    print(f"LOG === loop={loop} produced={len(rows)} cursor -> ({cursor_ts}, {cursor_id})")

                    if len(rows) < BATCH:
                        print("LOG === догнали хвост таблицы")
                        break
                else:
                    print("LOG === достигнут MAX_LOOPS, остаток догонит следующий run")

        print(f"LOG === stream done, total={total}")
        return total

    produce()


stream()
