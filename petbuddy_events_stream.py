"""
rizottoaria__petbuddy_events_stream

Стрим событий PetBuddy: ClickHouse analytics_prod.events -> Kafka topic petbuddy.events

С 2026-08-13 источник переключён с Postgres (opn_souls.events) на ClickHouse Cloud
(analytics_prod.events, conn petbuddy_ch_src, HTTP-интерфейс :8443). Схема источника
уже snake_case и совпадает 1:1 с petbuddy.events_queue (Kafka JSONEachRow), поэтому:
  * TO_SNAKE выключен -- ключи уже в нужном регистре;
  * properties/context в источнике имеют тип JSON -> в топик уходят вложенными
    объектами (raw-строка JSONEachRow отправляется как есть, без ре-сериализации).

Механика keyset-пагинации по (received_at, id) сохранена без изменений:
    WHERE (received_at, id) > cursor ORDER BY received_at ASC, id ASC.
Курсор STREAM_VAR хранится строкой "<received_at>|<id>" и НЕ сбрасывался при
переключении источника: id и received_at в новом CH те же, что были в Postgres
(проверено: id курсора существует в CH с тем же received_at), поэтому стрим
продолжился с той же точки без дыр и без дублей.

Легаси-значение курсора лежит в ISO-формате Postgres ("...T..+00:00"), новые
значения -- в CH-формате ("YYYY-MM-DD HH:MM:SS.sss"); оба разбираются
parseDateTime64BestEffort, так что переходный период безопасен.

Курсор двигается ТОЛЬКО после успешного flush() в Kafka. Упали на середине ->
следующий run переотправит батч -> дедуп в ClickHouse-приёмнике
petbuddy.events (ReplacingMergeTree(received_at)).

NB: sessions-стрим (petbuddy_sessions_stream) остаётся на Postgres -- в новом CH
таблицы sessions нет.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

BROKER = "kafka:9092"
TOPIC = "petbuddy.events"

CH_CONN_ID = "petbuddy_ch_src"   # Airflow http-conn: host/port/login/password
SRC_DB = "analytics_prod"
SRC_TABLE = "events"
RECEIVED_COL = "received_at"     # server-side время приёма, монотонно, NOT NULL

STREAM_VAR = "petbuddy_events_stream_cursor"

BATCH = 5_000        # стрим лёгкий, большие батчи ему не нужны
MAX_LOOPS = 20       # потолок на run: 100k строк; остаток догонит следующий run
HTTP_TIMEOUT = 120   # секунды на один HTTP-запрос к CH

# Точный список колонок под petbuddy.events_queue (без MATERIALIZED event_date и
# DEFAULT ingested_at, которых нет в контракте топика).
COLUMNS = [
    "id", "name", "player_id", "session_id", "client_session_id",
    "event_at", "client_timestamp", "idempotency_key", "client_event_id",
    "source", "properties", "context", "ip", "user_agent", "received_at",
]


# ---------------------------------------------------------------- helpers

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


def _ch_select(sql: str, params: dict) -> str:
    """POST-запрос к HTTP-интерфейсу ClickHouse. Возвращает тело ответа (текст).

    Креды и адрес берутся из Airflow-conn CH_CONN_ID (http). Пароль в коде не лежит.
    """
    import requests
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CH_CONN_ID)
    url = f"https://{conn.host}:{conn.port}/"
    headers = {
        "X-ClickHouse-User": conn.login,
        "X-ClickHouse-Key": conn.password or "",
    }
    resp = requests.post(
        url,
        params={**params, "database": SRC_DB},
        data=sql.encode("utf-8"),
        headers=headers,
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"LOG === CH HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.text


def _split_cursor(raw: str):
    """Курсор хранится как "<received_at>|<id>". Возвращает (ts, id)."""
    ts, _, last_id = raw.partition("|")
    return ts, last_id


# ---------------------------------------------------------------- dag

@dag(
    dag_id="rizottoaria__petbuddy_events_stream",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # критично: один писатель курсора
    tags=["petbuddy", "kafka", "stream", "clickhouse"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
)
def stream():

    @task
    def produce() -> int:
        import json

        from airflow.sdk import Variable

        cursor = Variable.get(STREAM_VAR, default="")
        cur_ts, cur_id = _split_cursor(cursor)
        if not cur_ts:
            raise RuntimeError(
                f"LOG === стрим-курсор пуст/битый ({cursor!r}); "
                f"ожидается '<received_at>|<id>' -- инициализируй STREAM_VAR"
            )

        producer = _make_producer()
        errors: list = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(str(err))

        # keyset по (received_at, id): устойчив к смене формата id (cuid -> UUID).
        # parseDateTime64BestEffort разбирает и легаси-ISO Postgres, и CH-формат.
        col_list = ", ".join(COLUMNS)
        sql = f"""
            SELECT {col_list}
            FROM {SRC_DB}.{SRC_TABLE}
            WHERE ({RECEIVED_COL}, id) > (
                parseDateTime64BestEffort({{ts:String}}, 3, 'UTC'),
                {{cid:String}}
            )
            ORDER BY {RECEIVED_COL} ASC, id ASC
            LIMIT {{lim:UInt32}}
            FORMAT JSONEachRow
        """

        total = 0
        for loop in range(MAX_LOOPS):
            body = _ch_select(sql, {
                "param_ts": cur_ts,
                "param_cid": cur_id,
                "param_lim": BATCH,
            })
            lines = [ln for ln in body.splitlines() if ln.strip()]
            if not lines:
                print(f"LOG === новых строк нет, cursor={cur_ts}|{cur_id}")
                break

            for ln in lines:
                row = json.loads(ln)
                # value = raw-строка JSONEachRow: сохраняет вложенный JSON как есть.
                producer.produce(
                    TOPIC,
                    key=str(row["id"]).encode(),
                    value=ln.encode("utf-8"),
                    on_delivery=on_delivery,
                )
                producer.poll(0)

            remaining = producer.flush(120)
            if remaining or errors:
                raise RuntimeError(
                    f"LOG === flush не прошёл: in_queue={remaining}, "
                    f"errors={errors[:5]} -- курсор НЕ двигаем"
                )

            last = json.loads(lines[-1])
            cur_ts = last[RECEIVED_COL]
            cur_id = str(last["id"])
            Variable.set(STREAM_VAR, f"{cur_ts}|{cur_id}")
            total += len(lines)
            print(f"LOG === loop={loop} produced={len(lines)} cursor -> {cur_ts}|{cur_id}")

            if len(lines) < BATCH:
                print("LOG === догнали хвост таблицы")
                break
        else:
            print("LOG === достигнут MAX_LOOPS, остаток догонит следующий run")

        print(f"LOG === stream done, total={total}")
        return total

    produce()


stream()
