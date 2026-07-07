from datetime import datetime, timedelta
import json

from airflow.sdk import dag, task          # Airflow 3.x

BROKER        = "185.182.9.18:9092"
TOPIC         = "petbuddy.events"
WATERMARK_VAR = "petbuddy_events_last_received_at"
PG_CONN_ID    = "opn_souls"
BATCH         = 50000
MAX_LOOPS     = 40
OVERLAP_SEC   = 2         

@dag(
    dag_id="rizottoaria__petbuddy_events_to_kafka",
    schedule=timedelta(minutes=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["petbuddy", "kafka", "polling"],
)
def pipeline():

    @task
    def poll_and_produce():
        import psycopg2.extras
        from datetime import timezone
        from confluent_kafka import Producer
        from airflow.models import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        last_ts = Variable.get(WATERMARK_VAR, default_var="1970-01-01T00:00:00+00:00")

        producer = Producer({
            "bootstrap.servers": BROKER,
            "linger.ms": 50,
            "compression.type": "lz4",
            "enable.idempotence": True,
        })

        to_str = lambda o: str(o)

        # camelCase (в кавычках) → snake_case ключи в JSON для Kafka/ClickHouse
        RENAME = {
            "playerId": "player_id", "sessionId": "session_id",
            "clientSessionId": "client_session_id", "eventAt": "event_at",
            "clientTimestamp": "client_timestamp", "idempotencyKey": "idempotency_key",
            "clientEventId": "client_event_id", "userAgent": "user_agent",
            "receivedAt": "received_at",
        }
        norm = lambda r: {RENAME.get(k, k): v for k, v in r.items()}

        total = 0
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = hook.get_conn()
        conn.set_session(readonly=True, autocommit=True)
        try:
            for _ in range(MAX_LOOPS):
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # camelCase-колонки строго в двойных кавычках!
                    cur.execute(
                        '''
                        SELECT * FROM events
                        WHERE "receivedAt" >= %s
                        ORDER BY "receivedAt"
                        LIMIT %s
                        ''',
                        (last_ts, BATCH),
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                for row in rows:
                    r = norm(row)
                    producer.produce(
                        TOPIC,
                        key=str(r["id"]).encode(),     
                        value=json.dumps(r, default=to_str).encode(),
                    )
                producer.poll(0)
                producer.flush()

                # новый watermark минус перекрытие, чтобы не потерять строки с тем же timestamp
                max_ts = rows[-1]["receivedAt"]
                new_ts = (max_ts - timedelta(seconds=OVERLAP_SEC)).astimezone(timezone.utc).isoformat()
                Variable.set(WATERMARK_VAR, new_ts)
                last_ts = max_ts.isoformat()              
                total += len(rows)

                if len(rows) < BATCH:
                    break
        finally:
            conn.close()

        print(f"LOG === produced {total} rows, watermark now {Variable.get(WATERMARK_VAR)}")

    poll_and_produce()

pipeline()