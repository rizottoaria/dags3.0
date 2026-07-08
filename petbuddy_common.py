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

def drain(batch: int, max_loops: int) -> str:
    """Keyset-пагинация по id, льёт в Kafka, двигает курсор. Возвращает лог-строку."""
    import psycopg2.extras
    from confluent_kafka import Producer
    from airflow.sdk import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    last_id = Variable.get(CURSOR_VAR, default="")
    producer = Producer({
        "bootstrap.servers": BROKER, "linger.ms": 100,
        "compression.type": "lz4", "enable.idempotence": True,
        "batch.size": 1 << 20,
    })
    to_str = lambda o: str(o)
    norm = lambda r: {RENAME.get(k, k): v for k, v in r.items()}

    total = 0
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    conn.set_session(readonly=True, autocommit=True)
    with conn.cursor() as c:
        c.execute("SET statement_timeout = '120s'")
    try:
        for _ in range(max_loops):
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if last_id == "":
                    cur.execute('SELECT * FROM events ORDER BY id LIMIT %s', (batch,))
                else:
                    cur.execute(
                        'SELECT * FROM events WHERE id > %s ORDER BY id LIMIT %s',
                        (last_id, batch),
                    )
                rows = cur.fetchall()
            if not rows:
                break
            for row in rows:
                r = norm(row)
                producer.produce(
                    TOPIC, key=str(r["id"]).encode(),
                    value=json.dumps(r, default=to_str).encode(),
                )
            producer.poll(0)
            producer.flush()
            last_id = rows[-1]["id"]
            Variable.set(CURSOR_VAR, last_id)
            total += len(rows)
            if len(rows) < batch:
                break
    finally:
        conn.close()
    return f"produced {total} rows, cursor now {last_id}"