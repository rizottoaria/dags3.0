"""
rizottoaria__rtp_player_lookup_bot — Telegram-бот @rtppromobot: карточка игрока RTP.bet по player_id.

Пользователь пишет боту один или несколько player_id через любой разделитель
(«800», «800, 803 790», «800;803/790», по строке на id) — бот отвечает строками из прод-DWH:

    SELECT p.player_id, p.registration_promo_code, p.nickname, p.registered_at
    FROM Players p WHERE p.player_id IN (...)

id, которых нет в Players, перечисляются отдельно.

Как живёт бот: не отдельный сервис, а Airflow-таск с long-polling getUpdates на ~4,8 мин;
DAG запускается каждые 5 мин (max_active_runs=1 — один поллер на токен), следующий прогон
продолжает с сохранённого offset. Ответ — в пределах секунд, между прогонами пауза в секунды
(сообщения за паузу не теряются: Telegram держит их до 24 ч).

Источник — прод-DWH (MySQL dwh.rtp.bet, база warehouse), Airflow Connection rtp_dwh.
Время registered_at — UTC.

Креды/настройки (Airflow Variables на 185, не в гите):
- RTP_PROMO_BOT_TOKEN — токен бота @rtppromobot (обязательно; без него прогон skipped);
- RTP_PROMO_BOT_ALLOWED_CHATS — необязательно: chat_id через запятую, кому бот отвечает.
  Пусто/нет переменной — отвечает всем. chat_id видно в логе таска и в ответе на /id.
Служебная: RTP_PROMO_BOT_OFFSET — offset getUpdates (ведёт сам бот).
"""
import html
import re
import time
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

RTP_DWH_CONN = "rtp_dwh"
TOKEN_VAR = "RTP_PROMO_BOT_TOKEN"
ALLOWED_VAR = "RTP_PROMO_BOT_ALLOWED_CHATS"
OFFSET_VAR = "RTP_PROMO_BOT_OFFSET"

TG_API = "https://api.telegram.org/bot{token}/{method}"
RUN_SECONDS = 285            # выйти до следующего прогона (schedule 5 мин), сохранив offset
LONGPOLL_TIMEOUT = 25
MAX_IDS = 500                # защита от простыней
TG_TEXT_LIMIT = 3900         # лимит Telegram 4096, с запасом на теги

HELP = (
    "Пришлите player_id — один или несколько через запятую, пробел или любой другой знак.\n"
    "Например: <code>800, 803 790</code>\n"
    "Ответ: player_id, промокод регистрации, ник, дата регистрации (UTC)."
)


def _tg(token, method, **payload):
    import requests

    try:
        r = requests.post(TG_API.format(token=token, method=method), json=payload, timeout=30)
        return r.json()
    except Exception as exc:  # сеть/JSON — не роняем поллер
        print(f"tg {method} error: {exc!r}")
        return {"ok": False}


def _send(token, chat_id, text):
    return _tg(token, "sendMessage", chat_id=chat_id, text=text, parse_mode="HTML",
               disable_web_page_preview=True)


def _mysql():
    import pymysql
    from airflow.sdk import BaseHook

    c = BaseHook.get_connection(RTP_DWH_CONN)
    last = None
    for attempt in range(3):  # dwh.rtp.bet иногда отвечает Too many connections / рестарт
        try:
            conn = pymysql.connect(host=c.host, port=int(c.port or 3306), user=c.login, password=c.password,
                                   database=c.schema or "warehouse", charset="utf8mb4", connect_timeout=15,
                                   read_timeout=60, cursorclass=pymysql.cursors.DictCursor)
            with conn.cursor() as cur:
                cur.execute("SET time_zone = '+00:00'")
            return conn
        except Exception as exc:
            last = exc
            time.sleep(5 * (attempt + 1))
    raise last


def _lookup(ids):
    placeholders = ",".join(["%s"] * len(ids))
    sql = f"""
        SELECT p.player_id, p.registration_promo_code, p.nickname, p.registered_at
        FROM Players p
        WHERE p.player_id IN ({placeholders})
        ORDER BY p.player_id"""
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(ids))
            return cur.fetchall()
    finally:
        conn.close()


def _fmt_row(r):
    reg = r["registered_at"]
    reg = reg.strftime("%Y-%m-%d %H:%M:%S") if reg else "—"
    return (f"<b>{r['player_id']}</b> | {html.escape(r['registration_promo_code'] or '—')} | "
            f"{html.escape(r['nickname'] or '—')} | {reg}")


def _chunks(lines, limit=TG_TEXT_LIMIT):
    buf, size = [], 0
    for line in lines:
        if buf and size + len(line) + 1 > limit:
            yield "\n".join(buf)
            buf, size = [], 0
        buf.append(line)
        size += len(line) + 1
    if buf:
        yield "\n".join(buf)


def _handle(token, msg):
    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    if text.startswith("/start") or text.startswith("/help"):
        _send(token, chat_id, HELP)
        return
    if text.startswith("/id"):
        _send(token, chat_id, f"chat_id: <code>{chat_id}</code>")
        return

    ids = list(dict.fromkeys(int(x) for x in re.findall(r"\d+", text) if len(x) <= 10))
    if not ids:
        _send(token, chat_id, "Не нашла ни одного player_id.\n\n" + HELP)
        return
    if len(ids) > MAX_IDS:
        _send(token, chat_id, f"Слишком много id ({len(ids)}), максимум {MAX_IDS} за раз.")
        return

    try:
        rows = _lookup(ids)
    except Exception as exc:
        print(f"query error: {exc!r}")
        _send(token, chat_id, "Ошибка запроса к DWH, попробуйте через минуту.")
        return

    found = {r["player_id"] for r in rows}
    missing = [i for i in ids if i not in found]
    lines = [f"Найдено {len(rows)} из {len(ids)}", "<i>player_id | promo | nickname | registered_at (UTC)</i>"]
    lines += [_fmt_row(r) for r in rows]
    if missing:
        lines.append("Нет в Players: " + ", ".join(map(str, missing)))
    for part in _chunks(lines):
        _send(token, chat_id, part)


@dag(
    dag_id="rizottoaria__rtp_player_lookup_bot",
    schedule=timedelta(minutes=5),
    start_date=pendulum.datetime(2026, 11, 27, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0},
    tags=["rtp", "telegram", "bot"],
    doc_md=__doc__,
)
def rtp_player_lookup_bot():
    @task(execution_timeout=timedelta(minutes=6))
    def poll():
        import requests
        from airflow.exceptions import AirflowSkipException
        from airflow.sdk import Variable

        token = Variable.get(TOKEN_VAR, default=None)
        if not token:
            raise AirflowSkipException(f"нет Variable {TOKEN_VAR}")
        allowed = {x.strip() for x in (Variable.get(ALLOWED_VAR, default="") or "").split(",") if x.strip()}
        offset = int(Variable.get(OFFSET_VAR, default="0") or 0)

        deadline = time.monotonic() + RUN_SECONDS
        handled = 0
        while time.monotonic() < deadline - 5:
            wait = int(max(1, min(LONGPOLL_TIMEOUT, deadline - time.monotonic() - 5)))
            try:
                resp = requests.get(TG_API.format(token=token, method="getUpdates"),
                                    params={"offset": offset, "timeout": wait,
                                            "allowed_updates": '["message"]'},
                                    timeout=wait + 15).json()
            except Exception as exc:
                print(f"getUpdates error: {exc!r}")
                time.sleep(3)
                continue
            if not resp.get("ok"):
                print(f"getUpdates not ok: {str(resp)[:300]}")
                time.sleep(5)
                continue

            updates = resp.get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                msg = upd.get("message")
                if not msg or "text" not in msg:
                    continue
                chat_id = str(msg["chat"]["id"])
                who = msg.get("from", {}).get("username") or msg.get("from", {}).get("id")
                print(f"msg chat={chat_id} from={who}: {msg['text'][:200]!r}")
                if allowed and chat_id not in allowed and not msg["text"].startswith("/id"):
                    _send(token, chat_id, "Нет доступа. Ваш chat_id: <code>%s</code>" % chat_id)
                    continue
                try:
                    _handle(token, msg)
                    handled += 1
                except Exception as exc:  # один битый апдейт не роняет поллер
                    print(f"handle error: {exc!r}")
            if updates:
                Variable.set(OFFSET_VAR, str(offset))

        print(f"done: handled={handled}, offset={offset}")

    poll()


rtp_player_lookup_bot()
