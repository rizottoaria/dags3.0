"""
rizottoaria__alanbase_rtp

Выгрузка статистики партнёрки Alanbase (кабинет rtpbetpartners, Admin API) в Postgres
91.188.213.151, схема alanbase_rtp — источник для Tableau-дашборда rtp_bet.

Две таблицы:
- alanbase_rtp.conversions  — конверсии построчно (/v1/admin/statistic/conversions,
  timezone UTC, currency USD). Ключ conversion_id; вложенные goal/offer/partner/... развёрнуты
  в колонки, полный ответ API лежит в raw (jsonb).
- alanbase_rtp.daily_stats  — клики и конверсии по статусам за день
  (/v1/admin/statistic/common, group_by=day, timezone Europe/Belgrade, currency USD). Ключ day.

Статус конверсии меняется задним числом (HOLD -> CONFIRMED/REJECTED), поэтому каждый прогон
перечитывает скользящее окно LOOKBACK_DAYS (но не раньше START_DATE) и делает upsert.
Ретро за произвольный период: «Trigger DAG w/ config» {"since": "2026-09-01"}.

Креды: Variable ALANBASE_RTP_API_KEY, Connection alanbase_rtp_pg (postgres, schema = база).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

ALANBASE_URL = "https://rtpbetpartners.api.alanbase.com/v1/admin/statistic"
PG_CONN = "alanbase_rtp_pg"
SCHEMA = "alanbase_rtp"
START_DATE = "2026-09-01"
LOOKBACK_DAYS = 60
CHUNK_DAYS = 30
PER_PAGE = 1000
CURRENCY = "USD"
CONV_TZ = "UTC"
DAILY_TZ = "Europe/Belgrade"

CONVERSIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.conversions (
    conversion_id       bigint PRIMARY KEY,
    tid                 text,
    status              text,
    decline_reason      text,
    conversion_datetime timestamp,
    updated_at          timestamp,
    payment_model       int,
    payout              numeric(18,4),
    payout_currency     text,
    revenue             numeric(18,4),
    revenue_currency    text,
    value               numeric(18,4),
    value_currency      text,
    sub1 text, sub2 text, sub3 text, sub4 text, sub5 text,
    sub6 text, sub7 text, sub8 text, sub9 text, sub10 text,
    custom1 text, custom2 text, custom3 text, custom4 text, custom5 text,
    note                text,
    comment             text,
    comment_to_partner  text,
    edited_by_manager   boolean,
    click_id            text,
    click_datetime      timestamp,
    click_redirect_url  text,
    click_ip            text,
    browser             text,
    os                  text,
    device_type         text,
    country             text,
    referer             text,
    condition_id        bigint,
    is_qualification    boolean,
    user_agent          text,
    x_requested_with    text,
    promocode           text,
    promocode_user_id   text,
    landing_id          bigint,
    goal_id             bigint,
    goal_name           text,
    goal_key            text,
    advertiser_id       bigint,
    advertiser_name     text,
    product_id          bigint,
    product_name        text,
    offer_id            bigint,
    offer_name          text,
    partner_id          bigint,
    partner_name        text,
    raw                 jsonb,
    _loaded_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS conversions_conversion_datetime_idx
    ON {SCHEMA}.conversions (conversion_datetime);
COMMENT ON TABLE {SCHEMA}.conversions IS
    'Alanbase rtpbetpartners: конверсии (время UTC, суммы USD). DAG rizottoaria__alanbase_rtp';
"""

STATUSES = ("confirmed", "pending", "hold", "rejected", "total")
METRICS = ("count", "payout", "revenue", "value")
DAILY_METRIC_COLS = [f"{s}_{m}" for s in STATUSES for m in METRICS]

DAILY_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.daily_stats (
    day                date PRIMARY KEY,
    click_count        bigint,
    click_unique_count bigint,
    {", ".join(f"{c} {'bigint' if c.endswith('_count') else 'numeric(18,4)'}" for c in DAILY_METRIC_COLS)},
    _loaded_at         timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE {SCHEMA}.daily_stats IS
    'Alanbase rtpbetpartners: клики и конверсии по статусам за день (день по Europe/Belgrade, суммы USD). DAG rizottoaria__alanbase_rtp';
"""


def _api_get(endpoint: str, params: dict) -> dict:
    import time

    import requests
    from airflow.sdk import Variable

    headers = {
        "API-KEY": Variable.get("ALANBASE_RTP_API_KEY"),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    for attempt in range(1, 6):
        r = requests.get(f"{ALANBASE_URL}/{endpoint}", headers=headers, params=params, timeout=120)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(int(r.headers.get("Retry-After", 2 ** attempt)))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Alanbase {endpoint}: превышено число повторов")


def _window(conf: dict) -> tuple[datetime, datetime]:
    """[since 00:00, сегодня 23:59:59]; since = conf.since или max(START_DATE, сегодня - LOOKBACK)."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if conf.get("since"):
        start = datetime.strptime(conf["since"], "%Y-%m-%d")
    else:
        start = max(datetime.strptime(START_DATE, "%Y-%m-%d"), today - timedelta(days=LOOKBACK_DAYS))
    end = today + timedelta(hours=23, minutes=59, seconds=59)
    return start, end


def _pg_exec(ddl: str, sql: str, rows: list[tuple]) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import execute_values

    conn = PostgresHook(postgres_conn_id=PG_CONN).get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
            cur.execute(ddl)
            if rows:
                execute_values(cur, sql, rows, page_size=1000)
    finally:
        conn.close()


@dag(
    dag_id="rizottoaria__alanbase_rtp",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # включить после создания connection alanbase_rtp_pg
    tags=["alanbase", "rtp", "postgres", "tableau"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def alanbase_rtp():

    @task(execution_timeout=timedelta(minutes=30))
    def load_conversions(**context) -> int:
        import json

        start, end = _window((context["dag_run"].conf or {}))
        rows = {}
        cur = start
        while cur <= end:
            win_end = min(cur + timedelta(days=CHUNK_DAYS) - timedelta(seconds=1), end)
            page = 1
            while True:
                payload = _api_get("conversions", {
                    "timezone": CONV_TZ,
                    "date_from": cur.strftime("%Y-%m-%d %H:%M:%S"),
                    "date_to": win_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "currency_code": CURRENCY,
                    "page": page,
                    "per_page": PER_PAGE,
                })
                data = payload.get("data") or []
                meta = payload.get("meta") or {}
                for c in data:
                    rows[c["conversion_id"]] = c
                last_page = meta.get("last_page") or page
                print(f"conversions {cur:%Y-%m-%d}..{win_end:%Y-%m-%d} стр. {page}/{last_page} "
                      f"всего {meta.get('total_count')}")
                if page >= last_page or not data:
                    break
                page += 1
            cur = win_end + timedelta(seconds=1)

        def nz(v):  # API отдаёт "" вместо null
            return None if v == "" else v

        def sub(c, key, field):
            return (c.get(key) or {}).get(field)

        plain = ["tid", "status", "decline_reason", "conversion_datetime", "updated_at",
                 "payment_model", "payout", "payout_currency", "revenue", "revenue_currency",
                 "value", "value_currency",
                 *[f"sub{i}" for i in range(1, 11)], *[f"custom{i}" for i in range(1, 6)],
                 "note", "comment", "comment_to_partner", "edited_by_manager",
                 "click_id", "click_datetime", "click_redirect_url", "click_ip", "browser", "os",
                 "device_type", "country", "referer", "condition_id", "is_qualification",
                 "user_agent", "x_requested_with", "promocode", "promocode_user_id", "landing_id"]
        nested = [("goal_id", "goal", "id"), ("goal_name", "goal", "name"), ("goal_key", "goal", "key"),
                  ("advertiser_id", "advertiser", "id"), ("advertiser_name", "advertiser", "full_name"),
                  ("product_id", "product", "id"), ("product_name", "product", "name"),
                  ("offer_id", "offer", "id"), ("offer_name", "offer", "name"),
                  ("partner_id", "partner", "id"), ("partner_name", "partner", "full_name")]
        cols = ["conversion_id", *plain, *(n[0] for n in nested), "raw"]
        values = [
            (c["conversion_id"], *(nz(c.get(k)) for k in plain),
             *(sub(c, key, f) for _, key, f in nested), json.dumps(c, ensure_ascii=False))
            for c in rows.values()
        ]
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:])
        _pg_exec(
            CONVERSIONS_DDL,
            f"INSERT INTO {SCHEMA}.conversions ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT (conversion_id) DO UPDATE SET {updates}, _loaded_at = now()",
            values,
        )
        print(f"conversions upsert: {len(values)} строк ({start:%Y-%m-%d}..{end:%Y-%m-%d})")
        return len(values)

    @task(execution_timeout=timedelta(minutes=15))
    def load_daily_stats(**context) -> int:
        from zoneinfo import ZoneInfo

        start, _ = _window((context["dag_run"].conf or {}))
        # «сегодня» для дневной статистики — по Белграду (опережает UTC)
        end = datetime.now(ZoneInfo(DAILY_TZ)).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        values = []
        cur = start
        while cur <= end:
            win_end = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
            payload = _api_get("common", {
                "timezone": DAILY_TZ,
                "date_from": cur.strftime("%Y-%m-%d"),
                "date_to": win_end.strftime("%Y-%m-%d"),
                "currency_code": CURRENCY,
                "group_by": "day",
            })
            for item in payload.get("data") or []:
                day = next((g["id"] for g in item.get("group_fields", []) if g["group_field"] == "day"), None)
                if not day:
                    continue
                conv = item.get("conversions") or {}
                values.append((
                    day, item.get("click_count", 0), item.get("click_unique_count", 0),
                    *((conv.get(s) or {}).get(m, 0) for s in STATUSES for m in METRICS),
                ))
            cur = win_end + timedelta(days=1)

        cols = ["day", "click_count", "click_unique_count", *DAILY_METRIC_COLS]
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[1:])
        _pg_exec(
            DAILY_DDL,
            f"INSERT INTO {SCHEMA}.daily_stats ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT (day) DO UPDATE SET {updates}, _loaded_at = now()",
            values,
        )
        print(f"daily_stats upsert: {len(values)} дней ({start:%Y-%m-%d}..{end:%Y-%m-%d})")
        return len(values)

    load_conversions()
    load_daily_stats()


alanbase_rtp()
