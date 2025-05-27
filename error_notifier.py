# ────────────────────────────────────────────────────────────────
# error_notifier.py
# ────────────────────────────────────────────────────────────────
import os, asyncio, traceback, datetime, html, json, pathlib
from typing import Set
from dotenv import load_dotenv; load_dotenv()

ERROR_BOT_TOKEN = os.getenv("ERROR_BOT_TOKEN")
if not ERROR_BOT_TOKEN:
    raise RuntimeError("7511920158:AAFUtT9Rfce34lYWWrROwkootszXpgJj9DY")

SUB_FILE      = pathlib.Path("error_subscribers.json")   # хранит chat-id
OFFSET_FILE   = pathlib.Path("error_last_update.txt")    # хранит offset для getUpdates

# --- aiogram 3.7+ / 3.6- совместимость ----------------------------------------
from aiogram import Bot
try:
    from aiogram.enums import ParseMode
    from aiogram.client.default import DefaultBotProperties
    _bot = Bot(ERROR_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
except ImportError:
    _bot = Bot(ERROR_BOT_TOKEN, parse_mode="HTML")
# -----------------------------------------------------------------------------

# ──────────────────────────── подписчики ─────────────────────────────────────
def _load_subs() -> Set[int]:
    try:
        return set(json.loads(SUB_FILE.read_text()))
    except FileNotFoundError:
        return set()

def _save_subs(s: Set[int]):
    SUB_FILE.write_text(json.dumps(sorted(s)))

_SUBSCRIBERS: Set[int] = _load_subs()

def _load_offset() -> int:
    try:
        return int(OFFSET_FILE.read_text())
    except (FileNotFoundError, ValueError):
        return 0

def _save_offset(offset: int):
    OFFSET_FILE.write_text(str(offset))

# ────────────────────────── служебные хелперы ────────────────────────────────
def _fmt_ts() -> str:
    return datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')

def _code(block: str) -> str:
    return f"<pre>{html.escape(block)}</pre>"

async def _refresh_subscribers() -> None:
    """
    Считываем входящие апдейты, чтобы:
      • добавить новых /start
      • удалить тех, кто написал /stop
    """
    offset = _load_offset()
    updates = await _bot.get_updates(offset=offset + 1, timeout=0)
    if not updates:
        return

    global _SUBSCRIBERS
    for upd in updates:
        if not upd.message:
            continue
        chat_id = upd.message.chat.id
        text    = (upd.message.text or "").strip().lower()

        if text in ("/start", "/subscribe"):
            if chat_id not in _SUBSCRIBERS:
                _SUBSCRIBERS.add(chat_id)
                await _bot.send_message(chat_id, "✅ Подписка на ошибки включена.")
        elif text in ("/stop", "/unsubscribe"):
            if chat_id in _SUBSCRIBERS:
                _SUBSCRIBERS.discard(chat_id)
                await _bot.send_message(chat_id, "❎ Подписка отключена.")

    # сохраняем
    _save_subs(_SUBSCRIBERS)
    _save_offset(updates[-1].update_id)

async def _poll_updates_forever():
    """Отдельная корутина: каждые 2 сек. проверяем новые /start-/stop."""
    while True:
        try:
            await _refresh_subscribers()
        except Exception as e:                       # чтобы не упал навсегда
            print("poll_updates error:", e)
        await asyncio.sleep(2)


async def start_background_tasks(loop: asyncio.AbstractEventLoop):
    loop.create_task(_poll_updates_forever())

# ────────────────────────── публикация сообщений ─────────────────────────────
async def _post(msg: str):
    """Отправить сообщение всем подписчикам (HTML)."""
    # await _refresh_subscribers()                    # подхватили новых /start
    to_remove = set()

    for cid in _SUBSCRIBERS:
        try:
            await _bot.send_message(cid, msg)
        except Exception:
            # если пользователь запретил бота → убрать из списка
            to_remove.add(cid)

    if to_remove:
        _SUBSCRIBERS.difference_update(to_remove)
        _save_subs(_SUBSCRIBERS)

# ────────────────────────── публичный API ────────────────────────────────────
async def notify_startup():
    await _post(f"🟢 Бот <b>запущен</b> — {_fmt_ts()}")

async def notify_exception(exc: BaseException):
    await _post(
        f"🔴 <b>НЕОБРАБОТАННОЕ ИСКЛЮЧЕНИЕ</b> — {_fmt_ts()}\n{_code(traceback.format_exc())}"
    )

async def notify_log_record(record: str):
    await _post(f"🟠 <b>Log ERROR</b> — {_fmt_ts()}\n{_code(record)}")

async def notify_heartbeat(uptime: str):
    await _post(f"💓 <b>Heartbeat</b> — бот жив, uptime {uptime}")
