"""
rizottoaria__alanbase_rtp

Выгрузка статистики партнёрки Alanbase (кабинет rtpbetpartners, Admin API) в ClickHouse на 185,
база alanbase_rtp — источник для Tableau-дашборда rtp_bet.

Две таблицы:
- alanbase_rtp.conversions  — конверсии построчно (/v1/admin/statistic/conversions,
  timezone UTC, currency USD). Ключ conversion_id; вложенные goal/offer/partner/... развёрнуты
  в колонки, полный ответ API лежит в raw (JSON-текст).
- alanbase_rtp.daily_stats  — клики и конверсии по статусам за день
  (/v1/admin/statistic/common, group_by=day, timezone Europe/Belgrade, currency USD). Ключ day.

Статус конверсии меняется задним числом (HOLD -> CONFIRMED/REJECTED), поэтому каждый прогон
перечитывает скользящее окно LOOKBACK_DAYS (но не раньше START_DATE) и дописывает версии строк;
таблицы ReplacingMergeTree(_loaded_at), после загрузки OPTIMIZE FINAL (объёмы маленькие) —
в таблицах всегда одна актуальная строка на ключ.
Ретро за произвольный период: «Trigger DAG w/ config» {"since": "2026-09-01"}.

Креды: Variable ALANBASE_RTP_API_KEY (API), Variable CH_DBT_PASSWORD (CH-юзер dbt).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

ALANBASE_URL = "https://rtpbetpartners.api.alanbase.com/v1/admin/statistic"
CH_URL = "http://clickhouse:8123/"
CH_DB = "alanbase_rtp"
START_DATE = "2026-09-01"
LOOKBACK_DAYS = 60
CHUNK_DAYS = 30
PER_PAGE = 1000
CURRENCY = "USD"
CONV_TZ = "UTC"
DAILY_TZ = "Europe/Belgrade"

CONV_STR = ["tid", "status", "decline_reason", "payout_currency", "revenue_currency", "value_currency",
            *[f"sub{i}" for i in range(1, 11)], *[f"custom{i}" for i in range(1, 6)],
            "note", "comment", "comment_to_partner", "click_id", "click_redirect_url", "click_ip",
            "browser", "os", "device_type", "country", "referer", "user_agent", "x_requested_with",
            "promocode", "promocode_user_id"]
CONV_NESTED = [("goal_id", "goal", "id"), ("goal_name", "goal", "name"), ("goal_key", "goal", "key"),
               ("advertiser_id", "advertiser", "id"), ("advertiser_name", "advertiser", "full_name"),
               ("product_id", "product", "id"), ("product_name", "product", "name"),
               ("offer_id", "offer", "id"), ("offer_name", "offer", "name"),
               ("partner_id", "partner", "id"), ("partner_name", "partner", "full_name")]

CONVERSIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.conversions (
    conversion_id       UInt64,
    conversion_datetime Nullable(DateTime('UTC')),
    updated_at          Nullable(DateTime('UTC')),
    click_datetime      Nullable(DateTime('UTC')),
    payment_model       Nullable(Int32),
    payout              Decimal(18, 4),
    revenue             Decimal(18, 4),
    value               Decimal(18, 4),
    edited_by_manager   Bool,
    is_qualification    Bool,
    condition_id        Nullable(UInt64),
    landing_id          Nullable(UInt64),
    {", ".join(f"{c} String" for c in CONV_STR)},
    {", ".join(f"{c} {'Nullable(UInt64)' if c.endswith('_id') else 'String'}" for c, _, _ in CONV_NESTED)},
    raw                 String,
    _loaded_at          DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY conversion_id
COMMENT 'Alanbase rtpbetpartners: конверсии (время UTC, суммы USD). DAG rizottoaria__alanbase_rtp'
"""

STATUSES = ("confirmed", "pending", "hold", "rejected", "total")
METRICS = ("count", "payout", "revenue", "value")
DAILY_METRIC_COLS = [f"{s}_{m}" for s in STATUSES for m in METRICS]

DAILY_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.daily_stats (
    day                Date,
    click_count        UInt64,
    click_unique_count UInt64,
    {", ".join(f"{c} {'UInt64' if c.endswith('_count') else 'Decimal(18, 4)'}" for c in DAILY_METRIC_COLS)},
    _loaded_at         DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(_loaded_at)
ORDER BY day
COMMENT 'Alanbase rtpbetpartners: клики и конверсии по статусам за день (день по Europe/Belgrade, суммы USD). DAG rizottoaria__alanbase_rtp'
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


def _window_start(conf: dict) -> datetime:
    """conf.since или max(START_DATE, сегодня - LOOKBACK_DAYS)."""
    if conf.get("since"):
        return datetime.strptime(conf["since"], "%Y-%m-%d")
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    return max(datetime.strptime(START_DATE, "%Y-%m-%d"), today - timedelta(days=LOOKBACK_DAYS))


def _ch_load(table: str, ddl: str, rows: list[dict]) -> None:
    import json

    import requests
    from airflow.sdk import Variable

    auth = ("dbt", Variable.get("CH_DBT_PASSWORD"))

    def ch(query, data=None):
        r = requests.post(CH_URL, params={"query": query, "date_time_input_format": "best_effort"},
                          data=data, auth=auth, timeout=180)
        if r.status_code != 200:
            raise RuntimeError(f"CH error {r.status_code}: {r.text[:500]}")

    ch(ddl)
    if rows:
        body = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows).encode("utf-8")
        ch(f"INSERT INTO {CH_DB}.{table} FORMAT JSONEachRow", body)
        ch(f"OPTIMIZE TABLE {CH_DB}.{table} FINAL")


@dag(
    dag_id="rizottoaria__alanbase_rtp",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["alanbase", "rtp", "clickhouse", "tableau"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def alanbase_rtp():

    @task(execution_timeout=timedelta(minutes=30))
    def load_conversions(**context) -> int:
        import json

        start = _window_start(context["dag_run"].conf or {})
        end = datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
        convs = {}
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
                    convs[c["conversion_id"]] = c
                last_page = meta.get("last_page") or page
                print(f"conversions {cur:%Y-%m-%d}..{win_end:%Y-%m-%d} стр. {page}/{last_page} "
                      f"всего {meta.get('total_count')}")
                if page >= last_page or not data:
                    break
                page += 1
            cur = win_end + timedelta(seconds=1)

        def nz(v):  # API отдаёт "" вместо null
            return None if v == "" else v

        rows = []
        for c in convs.values():
            row = {
                "conversion_id": c["conversion_id"],
                "conversion_datetime": nz(c.get("conversion_datetime")),
                "updated_at": nz(c.get("updated_at")),
                "click_datetime": nz(c.get("click_datetime")),
                "payment_model": c.get("payment_model"),
                "payout": c.get("payout") or 0,
                "revenue": c.get("revenue") or 0,
                "value": c.get("value") or 0,
                "edited_by_manager": bool(c.get("edited_by_manager")),
                "is_qualification": bool(c.get("is_qualification")),
                "condition_id": c.get("condition_id"),
                "landing_id": c.get("landing_id"),
                **{k: "" if c.get(k) is None else str(c.get(k)) for k in CONV_STR},
                **{col: (c.get(key) or {}).get(f) for col, key, f in CONV_NESTED},
                "raw": json.dumps(c, ensure_ascii=False),
            }
            for col, _, _ in CONV_NESTED:
                if not col.endswith("_id") and row[col] is None:
                    row[col] = ""
            rows.append(row)

        _ch_load("conversions", CONVERSIONS_DDL, rows)
        print(f"conversions: {len(rows)} строк ({start:%Y-%m-%d}..{end:%Y-%m-%d})")
        return len(rows)

    @task(execution_timeout=timedelta(minutes=15))
    def load_daily_stats(**context) -> int:
        from zoneinfo import ZoneInfo

        start = _window_start(context["dag_run"].conf or {})
        # «сегодня» для дневной статистики — по Белграду (опережает UTC)
        end = datetime.now(ZoneInfo(DAILY_TZ)).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        rows = []
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
                rows.append({
                    "day": day,
                    "click_count": item.get("click_count") or 0,
                    "click_unique_count": item.get("click_unique_count") or 0,
                    **{f"{s}_{m}": (conv.get(s) or {}).get(m) or 0 for s in STATUSES for m in METRICS},
                })
            cur = win_end + timedelta(days=1)

        _ch_load("daily_stats", DAILY_DDL, rows)
        print(f"daily_stats: {len(rows)} дней ({start:%Y-%m-%d}..{end:%Y-%m-%d})")
        return len(rows)

    load_conversions()
    load_daily_stats()


alanbase_rtp()
