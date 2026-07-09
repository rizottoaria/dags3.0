# petbuddy_common.py
import json

BROKER     = "185.182.9.18:9092"
TOPIC      = "petbuddy.events"
CURSOR_VAR = "petbuddy_events_cursor_id"
PG_CONN_ID = "opn_souls"

RENAME = {
    "playerId": "player_id", "sessionId": "session_id",
    "clientSessionId": "client_session_id", "eventAt": "event_at",
    "clientTimestamp": "client_timestamp", "idempotencyKey": "idempotency_key",
    "clientEventId": "client_event_id", "userAgent": "user_agent",
    "receivedAt": "received_at",
}

SQL = 'SELECT * FROM events WHERE id > %s ORDER BY id LIMIT %s'


def drain(batch: int = 5000, max_loops: int = 20, itersize: int = 500) -> str:
    """
    Keyset-пагинация по id (CUID лексикографически монотонен по времени).
    Тянет всё, что появилось после курсора; если нового нет — молча выходит.
    Курсор двигается ТОЛЬКО после flush в Kafka → at-least-once.
    """
    import psycopg2.extras
    from confluent_kafka import Producer
    from airflow.sdk import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    last_id = Variable.get(CURSOR_VAR, default="")   # "" < любой CUID

    producer = Producer({
        "bootstrap.servers": BROKER,
        "linger.ms": 100,
        "compression.type": "lz4",
        "enable.idempotence": True,
        "batch.size": 1 << 20,
    })
    to_str = lambda o: str(o)
    norm = lambda r: {RENAME.get(k, k): v for k, v in r.items()}

    total = 0
    conn = PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()
    conn.set_session(readonly=True, autocommit=False)   # named cursor требует транзакцию

    try:
        for loop in range(max_loops):
            batch_last_id, n = last_id, 0

            # серверный курсор: сервер отдаёт порциями по itersize, а не одним куском
            with conn.cursor(name=f"drain_{loop}",
                             cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.itersize = itersize
                cur.execute(SQL, (last_id, batch))
                for row in cur:
                    r = norm(row)
                    producer.produce(
                        TOPIC,
                        key=str(r["id"]).encode(),
                        value=json.dumps(r, default=to_str).encode(),
                    )
                    producer.poll(0)
                    batch_last_id = r["id"]
                    n += 1
            conn.commit()          # закрываем read-only транзакцию порции

            if n == 0:
                break              # новых событий нет — штатный выход

            producer.flush()                       # 1) гарантируем доставку
            Variable.set(CURSOR_VAR, batch_last_id)  # 2) только потом двигаем курсор
            last_id = batch_last_id
            total += n

            if n < batch:
                break              # хвост исчерпан
    finally:
        conn.close()

    return f"produced {total} rows, cursor now {last_id or '(empty)'}"