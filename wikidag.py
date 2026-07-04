"""
DAG: rizottoaria__wiki_changes_to_kafka

Wikimedia EventStreams (recentchange, SSE) -> Kafka topic `wiki-changes`.

Паттерн: НЕ вечный стрим-таск, а батч-реплей своего интервала.
EventStreams хранит ~7 дней истории и поддерживает ?since=<ISO ts>,
поэтому каждый запуск читает строго свой data_interval
(data_interval_start -> data_interval_end) и останавливается,
когда meta.dt события достигает конца интервала.

Свойства:
  - идемпотентность: ретрай читает тот же интервал (at-least-once,
    дедупликация на консьюмере по meta.id)
  - backfill работает из коробки (в пределах ~7 дней истории стрима)
  - нет вечных тасков и щелей между запусками

Требования на воркере: pip install kafka-python requests
"""

import json
import time
from datetime import datetime, timezone

from airflow.sdk import dag, task
import pendulum

KAFKA_BOOTSTRAP = "185.182.9.18:9092"
TOPIC = "wiki-changes"
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "rizottoaria-kafka-practice/1.0 (rizottoaria@gmail.com)"

WIKI_FILTER: set[str] = set() 
MAX_RECONNECTS = 5
WALL_CLOCK_DEADLINE_SEC = 180  # предохранитель от зависания


def _parse_dt(iso_str: str) -> datetime:
    """meta.dt приходит как '2026-07-04T10:00:00Z' -> aware datetime."""
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def _iter_sse_data(response):
    """Минимальный SSE-парсер: отдаёт содержимое data:-строк по одному событию."""
    data_lines: list[str] = []
    for raw in response.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        if raw == "":  # пустая строка = конец SSE-события
            if data_lines:
                yield "\n".join(data_lines)
                data_lines = []
        elif raw.startswith("data:"):
            data_lines.append(raw[5:].lstrip())
    if data_lines:
        yield "\n".join(data_lines)


@dag(
    dag_id="rizottoaria__wiki_changes_to_kafka",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,  # можно включить: since-механика честно дозальёт пропуски
    max_active_runs=1,
    tags=["kafka", "wikipedia", "sse", "practice"],
)
def wiki_changes_to_kafka():
    @task(execution_timeout=pendulum.duration(minutes=4))
    def consume_interval(data_interval_start=None, data_interval_end=None):
        import requests
        from kafka import KafkaProducer

        interval_start = data_interval_start
        interval_end = data_interval_end
        print(f"LOG === interval {interval_start} -> {interval_end}")

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
            key_serializer=lambda v: v.encode(),
            acks="all",
            linger_ms=100,
        )

        sent = skipped = malformed = 0
        last_dt = interval_start
        deadline = time.monotonic() + WALL_CLOCK_DEADLINE_SEC
        reconnects = 0
        done = False

        while not done and reconnects <= MAX_RECONNECTS:
            since = last_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                resp = requests.get(
                    STREAM_URL,
                    params={"since": since},
                    headers={"User-Agent": USER_AGENT},
                    stream=True,
                    timeout=(5, 60),
                )
                resp.raise_for_status()

                for payload in _iter_sse_data(resp):
                    if time.monotonic() > deadline:
                        print("LOG === wall-clock deadline hit, stopping")
                        done = True
                        break
                    try:
                        event = json.loads(payload)
                        event_dt = _parse_dt(event["meta"]["dt"])
                    except (json.JSONDecodeError, KeyError, ValueError):
                        malformed += 1
                        continue

                    last_dt = max(last_dt, event_dt)

                    if event_dt >= interval_end:
                        done = True  # дочитали свой интервал
                        break
                    if event_dt < interval_start:
                        skipped += 1  # replay мог начать чуть раньше
                        continue
                    if event.get("meta", {}).get("domain") == "canary":
                        skipped += 1  # служебные события мониторинга Wikimedia
                        continue
                    wiki = event.get("wiki", "unknown")
                    if WIKI_FILTER and wiki not in WIKI_FILTER:
                        skipped += 1
                        continue

                    producer.send(TOPIC, key=wiki, value=event)
                    sent += 1

                resp.close()
                if not done:
                    reconnects += 1
                    print(f"LOG === stream ended early, reconnect {reconnects} since={last_dt}")
            except requests.RequestException as exc:
                reconnects += 1
                print(f"LOG === connection error: {exc}; reconnect {reconnects}")
                time.sleep(min(2**reconnects, 15))
            if time.monotonic() > deadline:
                print("LOG === wall-clock deadline hit, stopping")
                done = True

        producer.flush()
        producer.close()
        print(
            f"LOG === sent={sent} skipped={skipped} malformed={malformed} "
            f"last_dt={last_dt} reconnects={reconnects}"
        )
        if sent == 0 and reconnects > MAX_RECONNECTS:
            raise RuntimeError("Не удалось прочитать интервал: исчерпаны реконнекты")

    consume_interval()


wiki_changes_to_kafka()