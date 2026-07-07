from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task

BROKER        = "185.182.9.18:9092"
TOPIC         = "petbuddy.events"
WATERMARK_VAR = "petbuddy_events_last_id"
PG_CONN_ID    = "opn_souls"       
BATCH         = 50000
MAX_LOOPS     = 40                 

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
        from confluent_kafka import Producer
        from airflow.models import Variable
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        last_id = int(Variable.get(WATERMARK_VAR, default_var=0))

        producer = Producer({
            "bootstrap.servers": BROKER,
            "linger.ms": 50,
            "compression.type": "lz4",
            "enable.idempotence": True,
        })

        to_str = lambda o: str(o)   # datetime/Decimal → str; JSONB psycopg2 отдаёт dict

        total = 0
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = hook.get_conn()                       # psycopg2-соединение из connection
        conn.set_session(readonly=True, autocommit=True)
        try:
            for _ in range(MAX_LOOPS):
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM events WHERE id > %s ORDER BY id LIMIT %s",
                        (last_id, BATCH),
                    )
                    rows = cur.fetchall()
                if not rows:
                    break

                for row in rows:
                    producer.produce(
                        TOPIC,
                        key=str(row["id"]).encode(),
                        value=json.dumps(row, default=to_str).encode(),
                    )
                producer.poll(0)
                producer.flush()                              # сначала гарантируем доставку
                last_id = rows[-1]["id"]
                Variable.set(WATERMARK_VAR, str(last_id))     # потом двигаем watermark
                total += len(rows)

                if len(rows) < BATCH:
                    break
        finally:
            conn.close()

        print(f"LOG === produced {total} rows, watermark now {last_id}")

    poll_and_produce()

pipeline()