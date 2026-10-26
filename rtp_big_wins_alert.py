"""
rizottoaria__rtp_big_wins_alert / rizottoaria__rtp_big_wins_session_alert

Алерты в Slack о крупных выигрышах RTP.bet по провайдерам из договоров (Juice, Slotegrator и др.):
выигрыш СТРОГО больше порога провайдера (50 000 EUR -> алерт от 50 000.01 EUR).
Выплату по таким выигрышам нужно заморозить и отправить отчёт провайдеру (Report to) — в алерте
есть все поля для отчёта: дата/время, провайдер и игра, сайт, round ID, сумма в исходной валюте,
EUR и USD, курс и его дата, ID игрока.

Два DAG'а из одного файла (общая логика, разный ключ агрегации):
- rizottoaria__rtp_big_wins_alert — выигрыш РАУНДА: сумма всех win-событий раунда
  (source_service, provider_key, user_id, round_id): у части провайдеров раунд пишется
  несколькими win-событиями (фриспины/каскады);
- rizottoaria__rtp_big_wins_session_alert — выигрыш за игровую СЕССИЮ: сумма win-событий
  (source_service, provider_key, user_id, session_id); в алерте дополнительно session_id и все
  round_id сессии через ', ' (по времени первого выигрыша в раунде).

Источник — прод-DWH (MySQL dwh.rtp.bet, база warehouse), Airflow Connection RTP_DWH_CONN.
- Каждую минуту берём раунды/сессии с win-событиями, попавшими в DWH (created_at) за последние
  LOOKBACK_MINUTES (перекрытие на поздние доезды), и пересчитываем их полную сумму выигрыша.
- Курс: последний FxRateHistory <валюта>->USD с effective_at <= времени выигрыша
  (USD 1:1; USDT/USDC 1:1, если своего курса нет); EUR = USD / курс EUR->USD на тот же момент.
  Курсы платформы (поставщик курсов DWH), а не xe.com — при отчёте провайдеру сверить с xe.com.
- Тестовые игроки (Players.is_test=1) не алертятся.

Журнал алертов (и защита от повторов) — ClickHouse 185: alanbase_rtp.big_win_alerts (раунды) и
alanbase_rtp.big_win_session_alerts (сессии); один алерт на ключ. Если Slack не принял
сообщение — ключ не пишется и повторится в следующем прогоне.

Сайт: в DWH пока нет поля с доменом/зеркалом, на котором сыгран раунд — подставляется SITE_DEFAULT
(доделать, когда поле появится в базе).

Креды/настройки:
- Connection RTP_DWH_CONN (MySQL: host dwh.rtp.bet, schema warehouse, read-only юзер);
- Variable SLACK_BIG_WINS_WEBHOOK — Slack Incoming Webhook канала алертов (канал задаётся
  при создании вебхука);
- Variable CH_DBT_PASSWORD — CH-юзер dbt (журнал).
Пока Connection/Webhook не заведены — прогон пропускается (skipped), без ошибок.

Ручной запуск с conf:
- {"since": "2026-10-20 00:00:00"} — перепроверить окно с даты (UTC, по created_at);
- {"dry_run": true} — только печать в лог, без Slack и без записи в журнал;
- {"threshold_scale": 0.0001} — умножить пороги (проверка формата сообщения; только вместе с dry_run).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

RTP_DWH_CONN = "rtp_dwh"
SLACK_WEBHOOK_VAR = "SLACK_BIG_WINS_WEBHOOK"
CH_URL = "http://clickhouse:8123/"
LOOKBACK_MINUTES = 30
FX_CUSTOMER = "rtp"
FX_LOOKBACK_DAYS = 14
STABLE_USD = ("USD", "USDT", "USDC")
SITE_DEFAULT = "rtp.bet"
SLACK_TEXT_LIMIT = 2900

# (ключ провайдера, название из договора, порог, валюта порога, кому отчёт)
# Ключ сравнивается с началом нормализованного slug/названия провайдера (нижний регистр,
# только буквы и цифры): pragmatic -> pragmatic, pragmaticplay, pragmatic-play.
THRESHOLDS = [
    ("juice", "Juice", 50000, "USD", "legal@juiceplay.com"),
    ("booming", "Booming Games", 25000, "EUR", "account@slotegrator.com"),
    ("conceptgaming", "Concept Gaming", 9999, "EUR", "account@slotegrator.com"),
    ("elagames", "Ela Games", 20000, "EUR", "account@slotegrator.com"),
    ("ggames", "G.Games", 50000, "EUR", "account@slotegrator.com"),
    ("hacksaw", "Hacksaw Gaming", 50000, "EUR", "account@slotegrator.com"),
    ("pragmatic", "Pragmatic Play", 50000, "EUR", "account@slotegrator.com"),
    ("rubyplay", "Ruby Play", 10000, "EUR", "account@slotegrator.com"),
    ("skywind", "Skywind", 50000, "EUR", "account@slotegrator.com"),
    ("zillion", "Zillion Games", 50000, "EUR", "account@slotegrator.com"),
]

# режим -> (dag_id, таблица журнала, колонка группировки, подпись)
MODES = {
    "round": ("rizottoaria__rtp_big_wins_alert", "alanbase_rtp.big_win_alerts", "round_id", "round"),
    "session": ("rizottoaria__rtp_big_wins_session_alert", "alanbase_rtp.big_win_session_alerts",
                "session_id", "session"),
}

ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    alert_key          String,
    win_at             DateTime64(3, 'UTC'),
    provider           String,
    provider_slug      String,
    game               String,
    game_id            String,
    site               String,
    session_id         String,
    round_id           String,
    round_ids          String,
    rounds             UInt32,
    player_id          String,
    player_public_id   String,
    currency           String,
    amount             Decimal(38, 8),
    amount_usd         Decimal(38, 2),
    amount_eur         Decimal(38, 2),
    rate_usd           Float64,
    rate_usd_at        Nullable(DateTime64(3, 'UTC')),
    rate_eur_usd       Float64,
    rate_eur_usd_at    Nullable(DateTime64(3, 'UTC')),
    threshold          Decimal(18, 2),
    threshold_currency String,
    report_to          String,
    win_events         UInt32,
    alerted_at         DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(alerted_at)
ORDER BY alert_key
COMMENT 'RTP.bet: крупные выигрыши ({label}) сверх порогов провайдеров, отправленные в Slack. DAG {dag_id}'
"""


def _norm(s) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def _threshold_for(*names):
    for name in names:
        n = _norm(name)
        for t in THRESHOLDS:
            if n and n.startswith(t[0]):
                return t
    return None


def _ch(query: str, data: bytes | None = None) -> str:
    import requests
    from airflow.sdk import Variable

    r = requests.post(CH_URL, params={"query": query, "date_time_input_format": "best_effort"},
                      data=data, auth=("dbt", Variable.get("CH_DBT_PASSWORD")), timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"CH error {r.status_code}: {r.text[:500]}")
    return r.text


def _mysql():
    import pymysql
    from airflow.sdk import BaseHook

    c = BaseHook.get_connection(RTP_DWH_CONN)
    return pymysql.connect(host=c.host, port=int(c.port or 3306), user=c.login, password=c.password,
                           database=c.schema or "warehouse", charset="utf8mb4", connect_timeout=15,
                           read_timeout=120, cursorclass=pymysql.cursors.DictCursor)


def _fx(cur, currencies: set, since: datetime) -> dict:
    """{(валюта_from, валюта_to): [(effective_at, rate), ...] по возрастанию}; курсы FX_CUSTOMER,
    если по паре есть, иначе любого customer."""
    pairs = {}
    if not currencies:
        return pairs
    ph = ",".join(["%s"] * len(currencies))
    cur.execute(f"""
        SELECT customer, currency_from, currency_to, effective_at, rate
        FROM FxRateHistory
        WHERE currency_to IN ('USD', 'EUR') AND currency_from IN ({ph})
          AND effective_at >= %s
        ORDER BY effective_at""", (*currencies, since))
    rows = cur.fetchall()
    # пара без курсов в окне -> добираем самый свежий курс до окна
    have = {(r["currency_from"], r["currency_to"]) for r in rows}
    for cfrom in currencies:
        for cto in ("USD", "EUR"):
            if (cfrom, cto) not in have and cfrom != cto:
                cur.execute("""
                    SELECT customer, currency_from, currency_to, effective_at, rate FROM FxRateHistory
                    WHERE currency_from = %s AND currency_to = %s AND effective_at < %s
                    ORDER BY effective_at DESC LIMIT 1""", (cfrom, cto, since))
                rows = list(cur.fetchall()) + rows
    by_pair = {}
    for r in rows:
        by_pair.setdefault((r["currency_from"], r["currency_to"]), []).append(r)
    for pair, rs in by_pair.items():
        own = [r for r in rs if r["customer"] == FX_CUSTOMER] or rs
        pairs[pair] = sorted(((r["effective_at"], float(r["rate"])) for r in own), key=lambda x: x[0])
    return pairs


def _rate_at(pairs: dict, cfrom: str, cto: str, ts: datetime):
    """Последний курс с effective_at <= ts (иначе самый ранний после) -> (rate, effective_at)."""
    if cfrom == cto:
        return 1.0, None
    hist = pairs.get((cfrom, cto))
    if not hist:
        return None, None
    best = None
    for eff, rate in hist:
        if eff <= ts:
            best = (rate, eff)
        else:
            break
    return best or (hist[0][1], hist[0][0])


def _usd_rate(pairs, currency, ts):
    rate, eff = _rate_at(pairs, currency, "USD", ts)
    if rate is None and currency in STABLE_USD:
        return 1.0, None
    return rate, eff


def _fmt_money(v, cur=""):
    s = f"{v:,.2f}" if abs(v) >= 1 else f"{v:,.8f}".rstrip("0").rstrip(".")
    return f"{s} {cur}".strip()


def _clip(s: str, limit: int = SLACK_TEXT_LIMIT) -> str:
    return s if len(s) <= limit else s[:limit - 40].rsplit(", ", 1)[0] + ", … (full list in CH log)"


def _slack_message(w: dict, mode: str) -> dict:
    thr = f"{_fmt_money(w['threshold'], w['threshold_currency'])}"
    rate_lines = []
    if w["currency"] != "USD":
        at = f" (rate as of {w['rate_usd_at']:%Y-%m-%d %H:%M} UTC)" if w["rate_usd_at"] else ""
        rate_lines.append(f"1 {w['currency']} = {w['rate_usd']:.8g} USD{at}")
    if w["currency"] != "EUR":
        at = f" (rate as of {w['rate_eur_usd_at']:%Y-%m-%d %H:%M} UTC)" if w["rate_eur_usd_at"] else ""
        rate_lines.append(f"1 EUR = {w['rate_eur_usd']:.6g} USD{at}")
    rows = [
        ("Win date and time", f"{w['win_at']:%Y-%m-%d %H:%M:%S} UTC"),
        ("Provider", w["provider"]),
        ("Game", w["game"]),
        ("Website", w["site"]),
    ]
    if mode == "session":
        rows += [("Session ID", w["session_id"]), (f"Round IDs ({w['rounds']})", _clip(w["round_ids"]))]
    else:
        rows.append(("Round ID", w["round_id"]))
    rows += [
        ("Amount (original)", _fmt_money(w["amount"], w["currency"])),
        ("Amount EUR", _fmt_money(w["amount_eur"], "EUR")),
        ("Amount USD", _fmt_money(w["amount_usd"], "USD")),
        ("Exchange rate / date", "\n".join(rate_lines)),
        ("Player ID", f"{w['player_id']}" + (f" ({w['player_public_id']})" if w["player_public_id"] else "")),
        ("Threshold", thr),
        ("Report to", w["report_to"]),
    ]
    scope = "session" if mode == "session" else "round"
    head = (f":rotating_light: *Big win ({scope}) — {w['provider']}*: {_fmt_money(w['amount_eur'], 'EUR')} "
            f"/ {_fmt_money(w['amount_usd'], 'USD')} > threshold {thr}")
    cell = lambda s: {"type": "raw_text", "text": str(s)}  # noqa: E731
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": head}},
        {"type": "table", "column_settings": [{"is_wrapped": True}, {"is_wrapped": True}],
         "rows": [[cell("Field"), cell("Value")]] + [[cell(k), cell(v)] for k, v in rows]},
        {"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"Hold the payout until approval · win events: {w['win_events']} · "
            f"rates: DWH FxRateHistory (check vs xe.com)"}]},
    ]
    ref = f"session {w['session_id']}" if mode == "session" else f"round {w['round_id']}"
    return {"text": f"Big win {w['provider']} {_fmt_money(w['amount_eur'], 'EUR')} {ref}", "blocks": blocks}


def _check(mode: str, conf: dict) -> int:
    import json

    import requests
    from airflow.exceptions import AirflowSkipException
    from airflow.sdk import BaseHook, Variable

    dag_id, table, group_col, label = MODES[mode]
    dry_run = bool(conf.get("dry_run"))
    scale = float(conf.get("threshold_scale") or 1) if dry_run else 1.0

    webhook = Variable.get(SLACK_WEBHOOK_VAR, default=None)
    try:
        BaseHook.get_connection(RTP_DWH_CONN)
    except Exception:
        raise AirflowSkipException(f"нет Connection {RTP_DWH_CONN} — заведите подключение к dwh.rtp.bet")
    if not webhook and not dry_run:
        raise AirflowSkipException(f"нет Variable {SLACK_WEBHOOK_VAR} — заведите Slack webhook")

    now = datetime.utcnow()
    since = (datetime.strptime(conf["since"], "%Y-%m-%d %H:%M:%S") if conf.get("since")
             else now - timedelta(minutes=LOOKBACK_MINUTES))

    conn = _mysql()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT e.source_service, e.provider_key, e.provider_slug, e.user_id, e.{group_col} AS grp,
                   MAX(gp.title) AS provider_title
            FROM GameFinancialEvents e
            LEFT JOIN GameProviders gp ON gp.slug = e.provider_slug
            WHERE e.event_type = 'win' AND e.amount > 0 AND e.created_at >= %s
              AND e.{group_col} IS NOT NULL AND e.{group_col} <> ''
            GROUP BY e.source_service, e.provider_key, e.provider_slug, e.user_id, e.{group_col}""", (since,))
        touched = {(r["source_service"], r["provider_key"], r["user_id"], r["grp"]): r for r in cur.fetchall()
                   if _threshold_for(r["provider_slug"], r["provider_key"].split(":")[-1], r["provider_title"])}
        print(f"[{label}] окно с {since:%Y-%m-%d %H:%M:%S} UTC: {label}s провайдеров из списка — {len(touched)}")
        if not touched:
            return 0

        # полная сумма выигрыша по раундам (все win-события, не только из окна);
        # сессии ищем через индекс user_id
        per_round = []
        grps = sorted({k[3] for k in touched})
        users = sorted({k[2] for k in touched})
        for i in range(0, len(grps), 500):
            chunk = grps[i:i + 500]
            where = f"e.round_id IN ({','.join(['%s'] * len(chunk))})"
            params = list(chunk)
            if mode == "session":
                where = f"e.user_id IN ({','.join(['%s'] * len(users))}) AND e.session_id IN " \
                        f"({','.join(['%s'] * len(chunk))})"
                params = users + list(chunk)
            cur.execute(f"""
                SELECT e.source_service, e.provider_key, e.user_id, e.round_id, e.session_id, e.currency,
                       SUM(e.amount) AS amount, MIN(e.occurred_at) AS first_at, MAX(e.occurred_at) AS win_at,
                       COUNT(*) AS n, MAX(e.game_id) AS game_id, MAX(g.title) AS game_title,
                       MAX(p.public_id) AS public_id, MAX(COALESCE(p.is_test, 0)) AS is_test
                FROM GameFinancialEvents e
                LEFT JOIN Games g ON g.uuid = e.game_id
                LEFT JOIN Players p ON p.source_entity_id = e.user_id
                WHERE e.event_type = 'win' AND {where}
                GROUP BY e.source_service, e.provider_key, e.user_id, e.round_id, e.session_id, e.currency""",
                params)
            per_round.extend(cur.fetchall())

        groups = {}
        for r in per_round:
            k = (r["source_service"], r["provider_key"], r["user_id"], r[group_col])
            if k not in touched:
                continue
            g = groups.setdefault((*k, r["currency"]), {
                "rows": [], "provider_slug": touched[k]["provider_slug"],
                "provider_title": touched[k]["provider_title"]})
            g["rows"].append(r)

        currencies = {c[4] for c in groups} | {"EUR"}
        min_ts = min(r["win_at"] for g in groups.values() for r in g["rows"])
        pairs = _fx(cur, currencies, min_ts - timedelta(days=FX_LOOKBACK_DAYS))
    finally:
        conn.close()

    big = []
    for (svc, pkey, user_id, grp, currency), g in groups.items():
        rows = sorted(g["rows"], key=lambda x: x["first_at"])
        t = _threshold_for(g["provider_slug"], pkey.split(":")[-1], g["provider_title"])
        _, contract_name, threshold, thr_cur, report_to = t
        amount = float(sum(r["amount"] for r in rows))
        win_at = max(r["win_at"] for r in rows)
        rate_usd, rate_usd_at = _usd_rate(pairs, currency, win_at)
        rate_eur, rate_eur_at = _rate_at(pairs, "EUR", "USD", win_at)
        if rate_usd is None or rate_eur is None:
            print(f"!! нет курса {currency}->USD или EUR->USD: {label} {grp} ({amount} {currency}) — пропуск")
            continue
        amount_usd = amount * rate_usd
        amount_eur = amount_usd / rate_eur
        in_thr_cur = amount_usd if thr_cur == "USD" else amount_eur
        if round(in_thr_cur, 2) <= threshold * scale:
            continue
        if any(r["is_test"] for r in rows):
            print(f"тестовый игрок {user_id}: {contract_name} {amount} {currency} {label} {grp} — без алерта")
            continue
        round_ids = [r["round_id"] for r in rows]
        games = list(dict.fromkeys(r["game_title"] or r["game_id"] or "?" for r in rows))
        sessions = list(dict.fromkeys(r["session_id"] for r in rows if r["session_id"]))
        big.append({
            "alert_key": f"{svc}|{pkey}|{user_id}|{grp}|{currency}",
            "win_at": win_at, "provider": g["provider_title"] or contract_name,
            "provider_slug": g["provider_slug"] or pkey, "game": ", ".join(games),
            "game_id": ", ".join(dict.fromkeys(r["game_id"] or "" for r in rows)), "site": SITE_DEFAULT,
            "session_id": ", ".join(sessions), "round_id": grp if mode == "round" else "",
            "round_ids": ", ".join(round_ids), "rounds": len(round_ids),
            "player_id": user_id, "player_public_id": rows[0]["public_id"] or "", "currency": currency,
            "amount": amount, "amount_usd": round(amount_usd, 2), "amount_eur": round(amount_eur, 2),
            "rate_usd": rate_usd, "rate_usd_at": rate_usd_at, "rate_eur_usd": rate_eur,
            "rate_eur_usd_at": rate_eur_at, "threshold": threshold * scale,
            "threshold_currency": thr_cur, "report_to": report_to, "win_events": int(sum(r["n"] for r in rows)),
        })
    print(f"[{label}] выше порога: {len(big)}")
    if not big:
        return 0

    if dry_run:
        for w in big:
            print("WIN", json.dumps(w, ensure_ascii=False, default=str))
            print(json.dumps(_slack_message(w, mode), ensure_ascii=False, default=str))
        return len(big)

    _ch(ALERTS_DDL.format(table=table, label=label, dag_id=dag_id))
    ph = ",".join("'" + w["alert_key"].replace("\\", "\\\\").replace("'", "\\'") + "'" for w in big)
    done = set(_ch(f"SELECT alert_key FROM {table} WHERE alert_key IN ({ph}) FORMAT TSVRaw").split("\n"))
    sent = 0
    for w in sorted(big, key=lambda x: x["win_at"]):
        if w["alert_key"] in done:
            continue
        resp = requests.post(webhook, json=_slack_message(w, mode), timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Slack {resp.status_code}: {resp.text[:300]}")
        _ch(f"INSERT INTO {table} FORMAT JSONEachRow",
            json.dumps(w, ensure_ascii=False, default=str).encode("utf-8"))
        sent += 1
        print(f"алерт: {w['provider']} {w['amount']} {w['currency']} = {w['amount_eur']} EUR, {label} {w['alert_key']}")
    print(f"[{label}] отправлено в Slack: {sent} (уже было в журнале: {len(big) - sent})")
    return sent


def _make_dag(mode: str):
    dag_id, _, _, label = MODES[mode]

    @dag(
        dag_id=dag_id,
        schedule="* * * * *",
        start_date=datetime(2026, 10, 26),
        catchup=False,
        max_active_runs=1,
        tags=["rtp", "alert", "slack", "big-wins"],
        default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
        dagrun_timeout=timedelta(minutes=5),
        doc_md=__doc__,
    )
    def big_wins_dag():

        @task(task_id=f"check_big_wins_{label}", execution_timeout=timedelta(minutes=4))
        def check_big_wins(**context) -> int:
            return _check(mode, context["dag_run"].conf or {})

        check_big_wins()

    return big_wins_dag()


rtp_big_wins_alert = _make_dag("round")
rtp_big_wins_session_alert = _make_dag("session")
