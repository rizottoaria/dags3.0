"""
rizottoaria__kafka_consumer_watchdog

Сторож Kafka-консьюмеров ClickHouse. Известный отказ: при рестарте Kafka-брокера
(смена IP) движок Kafka в ClickHouse не переподключается, консьюмер выходит из
группы и перестаёт читать -- backlog копится как LAG, а стрим-даги при этом
зелёные (они уже отправили всё в Kafka). Лечится DETACH/ATTACH queue-таблицы:
консьюмер заново входит в группу и дочитывает с закоммиченного оффсета
(дедуп по id в ReplacingMergeTree защищает от повторов).

Каждый run по каждой queue-таблице:
  * считает LAG (high watermark - committed offset) и число активных членов группы;
  * если консьюмера в группе нет и есть backlog, ЛИБО lag висит выше порога и не
    убывает между запусками -> делает DETACH/ATTACH через ClickHouse HTTP;
  * пишет текущий lag в Variable, чтобы на следующем run видеть динамику.

Момент срабатывания продьюса может дать кратковременный всплеск lag -- поэтому
для «член есть, но застрял» требуем ещё и НЕубывания lag между двумя run'ами.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

BROKER = "kafka:9092"
CH_CONN_ID = "clickhouse_http"

# порог для случая «член в группе есть, но не двигается»; чистый стойл (0 членов)
# триггерит при любом lag > 0.
LAG_THRESHOLD = 1000

QUEUES = [
    {"table": "petbuddy.events_queue",   "group": "clickhouse_petbuddy_events",   "topic": "petbuddy.events"},
    {"table": "petbuddy.sessions_queue", "group": "clickhouse_petbuddy_sessions", "topic": "petbuddy.sessions"},
]


# ---------------------------------------------------------------- helpers

def _lag(group: str, topic: str) -> int:
    """LAG = sum(high watermark - committed) по партициям. Все вызовы с таймаутом.

    committed() лишь запрашивает оффсеты у координатора и НЕ вступает в группу
    (нет JoinGroup/poll) -> ребаланс живого консьюмера CH не задевается.
    """
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": group,
        "enable.auto.commit": False,
    })
    try:
        parts = list(c.list_topics(topic, timeout=10).topics[topic].partitions.keys())
        committed = c.committed([TopicPartition(topic, p) for p in parts], timeout=10)
        lag = 0
        for tp in committed:
            _, hi = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)
            off = tp.offset if (tp.offset is not None and tp.offset >= 0) else 0
            lag += max(0, hi - off)
        return lag
    finally:
        c.close()


def _members(group: str):
    """Число активных членов группы; None если describe недоступен/завис.

    describe_consumer_groups иногда висит на медленном координаторе -> строгий
    таймаут и best-effort: при неудаче решение принимается по динамике lag.
    """
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": BROKER})
        info = admin.describe_consumer_groups([group])[group].result(timeout=15)
        return len(info.members)
    except Exception as e:
        print(f"LOG === describe_consumer_groups({group}) недоступен: {e!r}")
        return None


def _ch_ddl(sql: str) -> str:
    import base64
    import urllib.request
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(CH_CONN_ID)
    url = f"http://{conn.host}:{conn.port}/"
    req = urllib.request.Request(url, data=sql.encode())
    token = base64.b64encode(f"{conn.login}:{conn.password}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    return urllib.request.urlopen(req, timeout=60).read().decode()


# ---------------------------------------------------------------- dag

@dag(
    dag_id="rizottoaria__kafka_consumer_watchdog",
    schedule=timedelta(minutes=10),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "clickhouse", "watchdog", "ops"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def watchdog():

    @task
    def check_and_heal() -> None:
        from airflow.sdk import Variable

        failures = []
        acted = []

        for q in QUEUES:
            table, group, topic = q["table"], q["group"], q["topic"]
            var = f"watchdog_lag_{group}"
            try:
                lag = _lag(group, topic)
                members = _members(group)   # может быть None, если describe завис
                prev = int(Variable.get(var, default="0"))
                print(f"LOG === {table}: lag={lag} members={members} prev_lag={prev}")

                stalled, reason = False, ""
                if members == 0 and lag > 0:
                    stalled, reason = True, "нет активного консьюмера в группе"
                elif lag > LAG_THRESHOLD and lag >= prev:
                    stalled, reason = True, f"lag {lag} не убывает (было {prev})"

                if stalled:
                    print(f"LOG === {table}: СТОЙЛ ({reason}) -> DETACH/ATTACH")
                    _ch_ddl(f"DETACH TABLE {table}")
                    _ch_ddl(f"ATTACH TABLE {table}")
                    acted.append(f"{table} ({reason})")
                    Variable.set(var, "0")     # сбрасываем базу для следующего сравнения
                else:
                    Variable.set(var, str(lag))
            except Exception as e:
                print(f"LOG === {table}: ошибка проверки: {e!r}")
                failures.append(f"{table}: {e!r}")

        if acted:
            print("LOG === вылечено: " + "; ".join(acted))
        if failures:
            raise RuntimeError("watchdog: ошибки проверки -- " + " | ".join(failures))
        print("LOG === watchdog done")

    check_and_heal()


watchdog()
