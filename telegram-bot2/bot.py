from __future__ import annotations
import os
import time
import json
import sqlite3
import re
import threading
import queue
import atexit
import telebot
import psutil
from telebot import types
from telebot import apihelper
import random
import requests
import asyncio
import re as _re
import html as _html
from datetime import datetime
from telebot.handler_backends import ContinueHandling
from typing import Any, Dict, List, Optional, Tuple
from telebot.apihelper import ApiTelegramException
from telethon import TelegramClient
from telethon.errors import UsernameNotOccupiedError
from telethon.tl.types import MessageService, PeerChannel, PeerChat
from telethon.tl.types import (
    MessageEntityBold, MessageEntityItalic, MessageEntityUnderline,
    MessageEntityStrike, MessageEntityCode, MessageEntityPre,
    MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention,
    MessageEntityCustomEmoji
)


# ==== НАСТРОЙКИ ====

def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value


TOKEN = _get_required_env("BOT_TOKEN")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "Insertq").strip().lstrip("@") or "Insertq"  # без @

DATA_DIR = os.getenv("DATA_DIR", "/data")

GROUP_STATS_FILE = os.path.join(DATA_DIR, 'group_stats.json')
GROUP_SETTINGS_FILE = os.path.join(DATA_DIR, 'group_settings.json')
PROFILES_FILE = os.path.join(DATA_DIR, 'profiles.json')
USERS_FILE = os.path.join(DATA_DIR, 'users.json')  # новая БД пользователей
VERIFY_ADMINS_FILE = os.path.join(DATA_DIR, 'verify_admins.json')
VERIFY_DEV_FILE = os.path.join(DATA_DIR, 'verify_dev.json')
GLOBAL_USERS_FILE = os.path.join(DATA_DIR, "global_users.json")
CHAT_SETTINGS_FILE = os.path.join(DATA_DIR, 'chat_settings.json')
MODERATION_FILE = os.path.join(DATA_DIR, 'moderation.json')
DEV_CONTACT_INBOX_FILE = os.path.join(DATA_DIR, 'dev_contact_inbox.json')
DEV_CONTACT_META_FILE = os.path.join(DATA_DIR, 'dev_contact_meta.json')
PENDING_GROUPS_FILE = os.path.join(DATA_DIR, 'pending_groups.json')

os.makedirs(DATA_DIR, exist_ok=True)

BOT_THREADS = max(2, int(os.getenv("BOT_THREADS", "8")))
DB_FLUSH_INTERVAL_SECONDS = max(1, int(os.getenv("DB_FLUSH_INTERVAL_SECONDS", "2")))
GLOBAL_LAST_SEEN_UPDATE_SECONDS = max(15, int(os.getenv("GLOBAL_LAST_SEEN_UPDATE_SECONDS", "60")))
TG_CACHE_MEMBER_TTL = max(1, int(os.getenv("TG_CACHE_MEMBER_TTL", "15")))
TG_CACHE_CHAT_TTL = max(5, int(os.getenv("TG_CACHE_CHAT_TTL", "60")))

bot = telebot.TeleBot(TOKEN, parse_mode='HTML', num_threads=BOT_THREADS)
bot_raw = telebot.TeleBot(TOKEN, num_threads=BOT_THREADS)  # без HTML
API_ID = int(_get_required_env("API_ID"))
API_HASH = _get_required_env("API_HASH")
TG_SESSION_NAME = os.getenv("TG_SESSION_NAME", "user_session")

tg_client = TelegramClient(TG_SESSION_NAME, API_ID, API_HASH)

async def get_user_id_by_username_mtproto(username: str) -> int | None:
    username = (username or "").strip().lstrip("@")
    if not username:
        return None

    try:
        print(f"[MTProto] Пытаюсь resolve @{username}")
        await tg_client.start()
        entity = await tg_client.get_entity(username)
        print(f"[MTProto] Нашёл @{username}: id={entity.id}")
        return int(entity.id)
    except UsernameNotOccupiedError:
        print(f"[MTProto] @{username} не занят (UsernameNotOccupiedError)")
        return None
    except Exception as e:
        print(f"[MTProto] Ошибка при resolve @{username}: {e}")
        return None

COMMAND_PREFIXES = ['/', '.', ',', '!']

MAX_MSG_LEN = 3800

# ==== ЭМОДЗИ ====

PREMIUM_PREFIX_EMOJI_ID = "5447644880824181073"
PREMIUM_STATS_EMOJI_ID  = "5431577498364158238"
PREMIUM_USER_EMOJI_ID   = "5373012449597335010"
PREMIUM_CLOSE_EMOJI_ID  = "5465665476971471368"

# профиль (обновлённые)
EMOJI_PROFILE_ID        = "5226512880362332956"  # профиль
EMOJI_MSG_COUNT_ID      = "5431577498364158238"  # статистика сообщений в профиле
EMOJI_DESC_ID = "5334673106202010226"       # описание в профиле
EMOJI_AWARDS_BLOCK_ID   = "5332547853304734597"  # блок наград

# статусы
EMOJI_OWNER_ID          = "5958376256788502078"  # не используем у разработчика
EMOJI_ADMIN_ID          = "5377754411319698237"  # админ чата
EMOJI_DEV_ID            = "5390851716520353647"  # разработчик бота
EMOJI_MEMBER_ID         = "5373012449597335010"  # участник
EMOJI_LEFT_ID           = "5906995262378741881"  # покинувший
EMOJI_PREMIUM_STATUS_ID = "5438496463044752972"  # Премиум пользователь
EMOJI_VERIFY_ADMIN_ID = "5370941588165893740"    # Верифицирован администратором
EMOJI_VERIFY_DEV_ID = "5370661904190544678"      # Верифицирован разработчиком

# роль/область для легенды и меню
EMOJI_ROLE_ALL_ID       = "5908808657700655253"
EMOJI_ROLE_ADMIN_ID     = "5364237895836120924"
EMOJI_ROLE_DEV_ID       = "5951665890079544884"
EMOJI_ROLE_OWNER_ID      = "5397796867616546218"  # Владелец чата
EMOJI_ROLE_CHIEF_ADMIN_ID = "5397754265835938409"  # Главный админ
EMOJI_ROLE_ADMIN_ID      = "5397646938898178715"  # Админ
EMOJI_ROLE_MOD_ID        = "5397653273974939567"  # Модератор
EMOJI_ROLE_TRAINEE_ID    = "5398049016556560225"  # Стажёр

EMOJI_USER_ROLE_TEXT_ID  = "5418010521309815154"  # Роль (строка в профиле)
EMOJI_ROLE_ACTION_ID = "5418010521309815154"


# Эмодзи для интерфейса прав (оставляем твои ID и добавляем "выбор должности")
EMOJI_ROLE_SETTINGS_CHAT_ID = 5287238684226104614
EMOJI_ROLE_SETTINGS_SENT_PM_ID = 5341715473882955310
EMOJI_ROLE_SETTINGS_CANCEL_ID = 5465665476971471368  # отмена
EMOJI_ROLE_SETTINGS_SAVE_ID = 5454096630372379732    # сохранить
EMOJI_ROLE_SETTINGS_OPEN_AGAIN_ID = 5264727218734524899  # открыть снова
EMOJI_ROLE_SETTINGS_CHOOSE_RANK_ID = 5472308992514464048  # текст "Выберите должность..."
EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID = 5963223853231509569


EMOJI_SCOPE_GROUP_ID    = "5942877472163892475"
EMOJI_SCOPE_PM_ID       = "5967548335542767952"
EMOJI_SCOPE_ALL_ID      = "5944940516754853337"

# доп эмодзи 
EMOJI_LIST_ID           = "5334882760735598374"
EMOJI_ADMIN_RIGHTS_ID   = "5454096630372379732"
EMOJI_BTN_UNADMIN_ID    = "5465665476971471368"
EMOJI_BTN_KICK_ID       = "5467928559664242360"
EMOJI_REASON_ID = "5465143921912846619"  # Причина
EMOJI_LIST_ID = "5334882760735598374"       # список/меню
EMOJI_PING_ID = "5472146462362048818"       # пинг
EMOJI_LOG_ID = "5433653135799228968"        # лог бота
EMOJI_LOG_PM_ID = "5427009714745517609"     # "Лог отправлен в ЛС"
EMOJI_CHAT_CLOSED_ID = "5472308992514464048"      # Чат закрыт / заголовок
EMOJI_CHAT_OPEN_BTN_ID = "5427009714745517609"    # Кнопка/действие открыть чат
EMOJI_PIN_NOTIFY_ID = 5242628160297641831      # С уведомлением
EMOJI_PIN_SILENT_ID = 5244807637157029775      # Без уведомления
EMOJI_PIN_REPIN_ID = 5264727218734524899       # Открепить и закрепить снова
EMOJI_DELETED_REASON_ID = "5467519850576354798"
EMOJI_RATE_LIMIT_ID = "5451732530048802485"

# эмодзи для настроек приветствия
EMOJI_WELCOME_TEXT_ID = "5334882760735598374"    # 📝 Текст
EMOJI_WELCOME_MEDIA_ID = "5431783411981228752"   # 🖼️ Медиа
EMOJI_WELCOME_BUTTONS_ID = "5363850326577259091" # 🔘 Кнопки

EMOJI_PUNISHMENT_ID = "5467928559664242360"       # ⚠️
EMOJI_UNPUNISH_ID = "5427009714745517609"        # ✅
EMOJI_DELETED_REASON_ID = "5467519850576354798"  # 🗑️
EMOJI_LOG_ID = "5433653135799228968"             # 📋
EMOJI_PAGINATION_NEXT_ID = "5963179889946268318" # ➡️
EMOJI_PAGINATION_PREV_ID = "5963223853231509569" # ⬅️

# связь с разработчиком
EMOJI_CONTACT_DEV_ID = "5406631276042002796"
EMOJI_SEND_TEXT_PROMPT_ID = "5334673106202010226"
EMOJI_SENT_OK_ID = "5427009714745517609"
EMOJI_NEW_MSG_OWNER_ID = "5361979468887893611"
EMOJI_REPLY_BTN_ID = "5433614747381538714"
EMOJI_IGNORE_BTN_ID = "5454096630372379732"
EMOJI_REPLY_RECEIVED_ID = "5433811242135331842"
EMOJI_BOT_VERSION_ID = "5021712394259268143"

# новая легенда стартового меню
EMOJI_LEGEND_ANYWHERE_ID = "5287238684226104614"
EMOJI_LEGEND_DEV_ONLY_ID = "5390851716520353647"
EMOJI_LEGEND_DEV_OR_VERIFIED_ID = "5370661904190544678"
EMOJI_LEGEND_GROUP_ADMIN_ID = "5377754411319698237"
EMOJI_LEGEND_PM_ONLY_ID = "5373012449597335010"
EMOJI_LEGEND_GROUP_ONLY_ID = "5372926953978341366"
EMOJI_LEGEND_ALL_USERS_ID = "5411285332668720752"

AWARD_EMOJI_IDS = [
    "5382322671679708881",
    "5381990043642502553",
    "5381879959335738545",
    "5382054253403577563",
    "5391197405553107640",
    "5390966190283694453",
    "5382132232829804982",
    "5391038994274329680",
    "5391234698754138414",
    "5382322671679708881+5393480373944459905",
]

API_BASE_URL = f"https://api.telegram.org/bot{TOKEN}"


# ==== RAW-ХЕЛПЕРЫ ====

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException

# Переиспользуем одну сессию: TCP keep-alive без создания нового
# соединения при каждом raw-запросе к Telegram Bot API.
_HTTP_SESSION = requests.Session()


def raw_request(method: str, payload: dict):
    url = f"{API_BASE_URL}/{method}"
    try:
        r = _HTTP_SESSION.post(url, data=payload, timeout=10)
        return r.json()
    except Exception as e:
        print(f"[RAW] Ошибка запроса {method}: {e}")
        return None


def raw_set_chat_member_tag(chat_id: int, user_id: int, tag: str | None) -> tuple[bool, str | None]:
    payload = {
        "chat_id": chat_id,
        "user_id": user_id,
        "tag": (tag or "").strip(),
    }

    try:
        set_tag_method = getattr(bot, "set_chat_member_tag", None)
        if callable(set_tag_method):
            set_tag_method(chat_id, user_id, payload["tag"])
        else:
            apihelper._make_request(TOKEN, "setChatMemberTag", method="post", params=payload)
        tg_invalidate_chat_member_caches(chat_id, user_id)
        return True, None
    except ApiTelegramException as e:
        return False, str(e.description or str(e) or "Неизвестная ошибка Telegram API")
    except Exception as e:
        return False, str(e)


def raw_get_chat_member(chat_id: int, user_id: int) -> dict | None:
    try:
        result = apihelper._make_request(
            TOKEN,
            "getChatMember",
            method="post",
            params={"chat_id": chat_id, "user_id": user_id},
        )
        if isinstance(result, dict):
            return result
    except Exception:
        return None
    return None


def _extract_member_tag(member_obj: Any) -> str:
    if isinstance(member_obj, dict):
        for key in ("tag", "member_tag", "custom_tag", "custom_title"):
            value = member_obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    for attr in ("tag", "member_tag", "custom_tag", "custom_title"):
        value = getattr(member_obj, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _kb_to_dict(keyboard):
    """
    Универсальная конвертация:
    - если dict или None — возвращаем как есть (для старого кода);
    - если InlineKeyboardMarkup — собираем {inline_keyboard: [[...]]},
      прокидывая style / icon_custom_emoji_id, если они заданы на кнопках.
    """
    if keyboard is None or isinstance(keyboard, dict):
        return keyboard

    if isinstance(keyboard, InlineKeyboardMarkup):
        rows = []
        for row in keyboard.keyboard:
            row_buttons = []
            for btn in row:
                btn_dict = {"text": btn.text}
                if btn.callback_data is not None:
                    btn_dict["callback_data"] = btn.callback_data
                if btn.url is not None:
                    btn_dict["url"] = btn.url

                style = getattr(btn, "style", None)
                if style is not None:
                    btn_dict["style"] = style

                icon_id = getattr(btn, "icon_custom_emoji_id", None)
                if icon_id is not None:
                    btn_dict["icon_custom_emoji_id"] = icon_id

                row_buttons.append(btn_dict)
            rows.append(row_buttons)
        return {"inline_keyboard": rows}

    return keyboard


def raw_send_with_inline_keyboard(chat_id: int, text: str, keyboard):
    kb_dict = _kb_to_dict(keyboard)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if kb_dict is not None:
        payload["reply_markup"] = json.dumps(kb_dict, ensure_ascii=False)
    return raw_request("sendMessage", payload)


def raw_edit_inline_keyboard(chat_id: int, message_id: int, keyboard):
    kb_dict = _kb_to_dict(keyboard)
    payload = {"chat_id": chat_id, "message_id": message_id}
    if kb_dict is not None:
        payload["reply_markup"] = json.dumps(kb_dict, ensure_ascii=False)
    return raw_request("editMessageReplyMarkup", payload)


def raw_edit_message_with_keyboard(chat_id: int, message_id: int, text: str, keyboard):
    kb_dict = _kb_to_dict(keyboard)
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if kb_dict is not None:
        payload["reply_markup"] = json.dumps(kb_dict, ensure_ascii=False)
    return raw_request("editMessageText", payload)


def raw_delete_message(chat_id: int, message_id: int):
    payload = {"chat_id": chat_id, "message_id": message_id}
    return raw_request("deleteMessage", payload)


# ==== JSON ====

SQLITE_DB_FILE = os.path.join(DATA_DIR, "bot_data.sqlite3")
_DB_LOCK = threading.RLock()
_DB_CONN: sqlite3.Connection | None = None
SQLITE_JSON_FALLBACK_WRITE = os.getenv("SQLITE_JSON_FALLBACK_WRITE", "0").strip().lower() in {"1", "true", "yes", "on"}


def _stats_increment(key: str, delta: int = 1) -> None:
    stats = globals().get("STATS")
    if not isinstance(stats, dict):
        return
    stats[key] = int(stats.get(key) or 0) + delta


def _db_connect() -> sqlite3.Connection:
    global _DB_CONN
    with _DB_LOCK:
        if _DB_CONN is not None:
            return _DB_CONN

        conn = sqlite3.connect(SQLITE_DB_FILE, check_same_thread=False, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
                store_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()
        _DB_CONN = conn
        return conn


def _db_key(path: str) -> str:
    return os.path.abspath(path)


def _reset_db_connection():
    global _DB_CONN
    with _DB_LOCK:
        if _DB_CONN is None:
            return
        try:
            _DB_CONN.close()
        except Exception:
            pass
        _DB_CONN = None


def _legacy_json_load(path: str, default: Any):
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _legacy_json_save(path: str, data: Any):
    """Атомарный fallback-save в legacy JSON (для аварийных случаев SQLite)."""
    try:
        tmp = path + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"Ошибка fallback-сохранения JSON {path}: {e}")


def _known_json_store_paths() -> list[str]:
    """Собирает все известные json-хранилища по *_FILE константам."""
    out: list[str] = []
    for name, value in globals().items():
        if not name.endswith("_FILE"):
            continue
        if not isinstance(value, str):
            continue
        if not value.endswith(".json"):
            continue
        out.append(value)
    # order + уникальность
    return list(dict.fromkeys(out))


def migrate_legacy_json_to_sqlite() -> dict[str, int]:
    """Принудительно переносит все legacy JSON из DATA_DIR в SQLite."""
    migrated = 0
    skipped = 0
    failed = 0

    dynamic_paths: list[str] = []
    try:
        for name in os.listdir(DATA_DIR):
            if name.endswith(".json"):
                dynamic_paths.append(os.path.join(DATA_DIR, name))
    except Exception:
        pass

    paths = list(dict.fromkeys(_known_json_store_paths() + dynamic_paths))
    for path in paths:
        if not os.path.exists(path):
            skipped += 1
            continue

        try:
            payload = _legacy_json_load(path, None)
            if payload is None:
                failed += 1
                continue
            if not save_json_file(path, payload):
                failed += 1
                continue

            # Подтверждаем, что запись действительно легла в SQLite.
            key = _db_key(path)
            conn = _db_connect()
            with _DB_LOCK:
                row = conn.execute(
                    "SELECT payload_json FROM kv_store WHERE store_key = ?",
                    (key,),
                ).fetchone()
            if not row:
                failed += 1
                continue

            migrated += 1
        except Exception:
            failed += 1

    return {"migrated": migrated, "skipped": skipped, "failed": failed, "total": len(paths)}


def get_sqlite_status() -> dict[str, Any]:
    """Возвращает краткую диагностику SQLite-хранилища."""
    status: dict[str, Any] = {
        "db_path": SQLITE_DB_FILE,
        "exists": os.path.exists(SQLITE_DB_FILE),
        "size_bytes": os.path.getsize(SQLITE_DB_FILE) if os.path.exists(SQLITE_DB_FILE) else 0,
        "rows": 0,
        "latest_updated_at": 0,
        "keys": [],
    }
    try:
        conn = _db_connect()
        with _DB_LOCK:
            status["rows"] = int(conn.execute("SELECT COUNT(*) FROM kv_store").fetchone()[0])
            latest = conn.execute("SELECT COALESCE(MAX(updated_at), 0) FROM kv_store").fetchone()[0]
            status["latest_updated_at"] = int(latest or 0)
            status["keys"] = [r[0] for r in conn.execute("SELECT store_key FROM kv_store ORDER BY updated_at DESC LIMIT 10").fetchall()]
    except Exception as e:
        _stats_increment("sqlite_errors")
        status["error"] = str(e)
    return status


def load_json_file(path, default):
    key = _db_key(path)
    for attempt in range(2):
        try:
            conn = _db_connect()
            with _DB_LOCK:
                row = conn.execute(
                    "SELECT payload_json FROM kv_store WHERE store_key = ?",
                    (key,),
                ).fetchone()
            if row:
                return json.loads(row[0])
            break
        except Exception:
            _stats_increment("sqlite_errors")
            # Одна попытка восстановить соединение и прочитать повторно.
            if attempt == 0:
                _reset_db_connection()
                continue
            break

    # Автомиграция: если в SQLite записи нет, читаем старый JSON и кладем в SQLite.
    legacy = _legacy_json_load(path, default)
    try:
        save_json_file(path, legacy)
    except Exception:
        pass
    return legacy


def save_json_file(path, data):
    key = _db_key(path)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    ts = int(time.time())
    for attempt in range(2):
        try:
            conn = _db_connect()
            with _DB_LOCK:
                conn.execute(
                    """
                    INSERT INTO kv_store (store_key, payload_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(store_key) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    (key, payload, ts),
                )
                conn.commit()
            return True
        except Exception as e:
            _stats_increment("sqlite_errors")
            print(f"Ошибка сохранения SQLite {path}: {e}")
            if attempt == 0:
                _reset_db_connection()
                continue

    if SQLITE_JSON_FALLBACK_WRITE:
        _legacy_json_save(path, data)
    return False


_SAVE_LOCK = threading.Lock()
_SAVE_LAST_TS: dict[str, float] = {}
_SAVE_REGISTRY: dict[str, tuple[str, Any]] = {}
_SAVE_DIRTY_KEYS: set[str] = set()


def throttled_save_json_file(path: str, data: Any, key: str, force: bool = False):
    now = time.monotonic()
    should_write_now = False

    with _SAVE_LOCK:
        _SAVE_REGISTRY[key] = (path, data)
        last_ts = _SAVE_LAST_TS.get(key, 0.0)
        if force or (now - last_ts) >= DB_FLUSH_INTERVAL_SECONDS:
            _SAVE_LAST_TS[key] = now
            _SAVE_DIRTY_KEYS.discard(key)
            should_write_now = True
        else:
            _SAVE_DIRTY_KEYS.add(key)

    if should_write_now:
        save_json_file(path, data)


def _flush_pending_saves(force: bool = False):
    now = time.monotonic()
    to_write: list[tuple[str, Any]] = []

    with _SAVE_LOCK:
        for key in list(_SAVE_DIRTY_KEYS):
            item = _SAVE_REGISTRY.get(key)
            if not item:
                _SAVE_DIRTY_KEYS.discard(key)
                continue

            path, data = item
            last_ts = _SAVE_LAST_TS.get(key, 0.0)
            if force or (now - last_ts) >= DB_FLUSH_INTERVAL_SECONDS:
                _SAVE_LAST_TS[key] = now
                _SAVE_DIRTY_KEYS.discard(key)
                to_write.append((path, data))

    for path, data in to_write:
        save_json_file(path, data)


def _periodic_flush_worker():
    sleep_seconds = max(1, DB_FLUSH_INTERVAL_SECONDS)
    while True:
        time.sleep(sleep_seconds)
        _flush_pending_saves(force=False)


def force_flush_all_saves():
    _flush_pending_saves(force=True)


def close_sqlite_connection():
    _reset_db_connection()


def _shutdown_persistence():
    # Важно: сначала flush, потом закрытие SQLite.
    force_flush_all_saves()
    close_sqlite_connection()


_FLUSH_THREAD = threading.Thread(target=_periodic_flush_worker, daemon=True)
_FLUSH_THREAD.start()
atexit.register(_shutdown_persistence)


_TG_CACHE_LOCK = threading.Lock()
_TG_MEMBER_CACHE: dict[tuple[int, int], tuple[float, Any]] = {}
_TG_CHAT_CACHE: dict[str, tuple[float, Any]] = {}
_CALLBACK_DEDUPE_LOCK = threading.Lock()
_CALLBACK_DEDUPE: dict[tuple[int, str, int], float] = {}


CALLBACK_DEDUPE_BUCKET_SECONDS = 5
CALLBACK_DEDUPE_KEEP_BUCKETS = 2


def _tg_chat_cache_key(chat_ref: Any) -> str:
    if isinstance(chat_ref, str):
        return f"s:{chat_ref.strip().lower()}"
    try:
        return f"i:{int(chat_ref)}"
    except Exception:
        return f"o:{str(chat_ref)}"


def tg_get_chat(chat_ref: Any):
    key = _tg_chat_cache_key(chat_ref)
    now = time.monotonic()

    with _TG_CACHE_LOCK:
        cached = _TG_CHAT_CACHE.get(key)
        if cached and (now - cached[0]) < TG_CACHE_CHAT_TTL:
            return cached[1]

    _stats_increment("tg_cache_chat_misses")
    chat = bot.get_chat(chat_ref)
    with _TG_CACHE_LOCK:
        _TG_CHAT_CACHE[key] = (now, chat)
    return chat


def tg_get_chat_member(chat_id: int, user_id: int):
    key = (int(chat_id), int(user_id))
    now = time.monotonic()

    with _TG_CACHE_LOCK:
        cached = _TG_MEMBER_CACHE.get(key)
        if cached and (now - cached[0]) < TG_CACHE_MEMBER_TTL:
            return cached[1]

    _stats_increment("tg_cache_member_misses")
    member = bot.get_chat_member(chat_id, user_id)
    with _TG_CACHE_LOCK:
        _TG_MEMBER_CACHE[key] = (now, member)
    return member


def tg_invalidate_member_cache(chat_id: int, user_id: int) -> None:
    """Точечно сбрасывает запись (chat_id, user_id) из _TG_MEMBER_CACHE."""
    key = (int(chat_id), int(user_id))
    with _TG_CACHE_LOCK:
        _TG_MEMBER_CACHE.pop(key, None)


def tg_invalidate_chat_cache(chat_ref: Any) -> None:
    """Точечно сбрасывает запись chat_ref из _TG_CHAT_CACHE."""
    key = _tg_chat_cache_key(chat_ref)
    with _TG_CACHE_LOCK:
        _TG_CHAT_CACHE.pop(key, None)


def tg_invalidate_chat_member_caches(chat_id: int, user_id: int) -> None:
    """Сбрасывает member/chat кэш после мутаций состояния участника."""
    tg_invalidate_member_cache(chat_id, user_id)
    tg_invalidate_chat_cache(chat_id)


def _is_duplicate_callback_query(call: types.CallbackQuery) -> bool:
    data = (call.data or "").strip()
    if not data:
        return False

    user_id = int(getattr(call.from_user, "id", 0) or 0)
    bucket = int(time.time() // CALLBACK_DEDUPE_BUCKET_SECONDS)
    min_bucket = bucket - CALLBACK_DEDUPE_KEEP_BUCKETS
    key = (user_id, data, bucket)

    with _CALLBACK_DEDUPE_LOCK:
        stale_keys = [existing_key for existing_key in _CALLBACK_DEDUPE if existing_key[2] < min_bucket]
        for stale_key in stale_keys:
            _CALLBACK_DEDUPE.pop(stale_key, None)

        if key in _CALLBACK_DEDUPE:
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            return True

        _CALLBACK_DEDUPE[key] = time.monotonic()

    return False


# ==== ВЕРИФИКАЦИЯ ====

VERIFY_ADMINS = load_json_file(VERIFY_ADMINS_FILE, {})  # {chat_id: [user_id, ...]}
VERIFY_DEV = set(load_json_file(VERIFY_DEV_FILE, []))

def save_verify_admins():
    throttled_save_json_file(VERIFY_ADMINS_FILE, VERIFY_ADMINS, "verify_admins")

def save_verify_dev():
    save_json_file(VERIFY_DEV_FILE, list(VERIFY_DEV))


# ==== СВЯЗЬ С РАЗРАБОТЧИКОМ ====

DEV_CONTACT_INBOX = load_json_file(DEV_CONTACT_INBOX_FILE, {"last_id": 0, "items": []})
if not isinstance(DEV_CONTACT_INBOX, dict):
    DEV_CONTACT_INBOX = {"last_id": 0, "items": []}
DEV_CONTACT_INBOX.setdefault("last_id", 0)
DEV_CONTACT_INBOX.setdefault("items", [])

DEV_CONTACT_META = load_json_file(DEV_CONTACT_META_FILE, {})
if not isinstance(DEV_CONTACT_META, dict):
    DEV_CONTACT_META = {}

PENDING_DEV_CONTACT_FROM_USER: dict[int, dict] = {}
PENDING_DEV_REPLY_FROM_OWNER: dict[int, dict] = {}
BROADCAST_DRAFTS: dict[int, dict] = {}
BROADCAST_PENDING_INPUT: dict[int, dict] = {}
_OPERATION_QUEUE: queue.Queue[dict[str, Any]] = queue.Queue()
_OPERATION_QUEUE_LOCK = threading.Lock()
_OPERATION_QUEUE_ACTIVE: dict[int, dict[str, Any]] = {}
_OPERATION_QUEUE_NEXT_ID = 0


OPERATION_QUEUE_MAX_RETRIES = 4
OPERATION_QUEUE_MAX_BACKOFF_SECONDS = 30


def save_dev_contact_inbox():
    save_json_file(DEV_CONTACT_INBOX_FILE, DEV_CONTACT_INBOX)


def save_dev_contact_meta():
    save_json_file(DEV_CONTACT_META_FILE, DEV_CONTACT_META)


def _remember_owner_user_id(user: types.User | None):
    if not user or not is_owner(user):
        return
    prev = int(DEV_CONTACT_META.get("owner_user_id") or 0)
    if prev != user.id:
        DEV_CONTACT_META["owner_user_id"] = user.id
        save_dev_contact_meta()


def _resolve_owner_user_id() -> int | None:
    owner_id = int(DEV_CONTACT_META.get("owner_user_id") or 0)
    if owner_id > 0:
        return owner_id

    owner_username = (OWNER_USERNAME or "").strip().lstrip("@").lower()
    if not owner_username:
        return None

    try:
        owner_chat = bot.get_chat(f"@{owner_username}")
        owner_id = int(getattr(owner_chat, "id", 0) or 0)
        if owner_id > 0:
            DEV_CONTACT_META["owner_user_id"] = owner_id
            save_dev_contact_meta()
            return owner_id
    except Exception:
        pass

    for rec in (GLOBAL_USERS or {}).values():
        if not isinstance(rec, dict):
            continue
        username = str(rec.get("username") or "").strip().lstrip("@").lower()
        if username != owner_username:
            continue
        uid = int(rec.get("id") or 0)
        if uid > 0:
            DEV_CONTACT_META["owner_user_id"] = uid
            save_dev_contact_meta()
            return uid

    return None


def _dev_contact_new_id() -> int:
    last_id = int(DEV_CONTACT_INBOX.get("last_id") or 0) + 1
    DEV_CONTACT_INBOX["last_id"] = last_id
    return last_id


def _dev_contact_new_items() -> list[dict]:
    items = DEV_CONTACT_INBOX.get("items") or []
    return [
        it for it in items
        if isinstance(it, dict)
        and (it.get("status") or "new") == "new"
        and int(it.get("owner_notified_at") or 0) <= 0
    ]


def _dev_contact_find_item(message_id: int) -> dict | None:
    for item in (DEV_CONTACT_INBOX.get("items") or []):
        if int(item.get("id") or 0) == int(message_id):
            return item
    return None


# ==== СОСТОЯНИЕ ЗАКРЫТИЯ ЧАТОВ (ПРОСТО ФЛАГ) ====

CLOSE_CHAT_FILE = os.path.join(DATA_DIR, 'closechat.json')
CLOSE_CHAT_STATE = load_json_file(
    CLOSE_CHAT_FILE,
    {}
)  # { chat_id(str): { "closed": bool, "until": float|0 } }


def save_close_chat_state():
    save_json_file(CLOSE_CHAT_FILE, CLOSE_CHAT_STATE)


def setclosechatstate(chatid: int, closed: bool, until_ts: float | int):
    cid = str(chatid)
    if not closed:
        CLOSE_CHAT_STATE.pop(cid, None)
    else:
        CLOSE_CHAT_STATE[cid] = {
            "closed": True,
            "until": float(until_ts) if until_ts else 0.0,
        }
    save_close_chat_state()


def getclosechatstate(chatid: int) -> dict:
    return CLOSE_CHAT_STATE.get(str(chatid)) or {}


def getcurrentdefaultpermissions(chatid: int) -> types.ChatPermissions | None:
    """
    Текущие default-permissions чата.
    """
    try:
        chat = bot.get_chat(chatid)
        perms = getattr(chat, "permissions", None)
        if isinstance(perms, types.ChatPermissions):
            return perms
    except Exception:
        pass
    return None


def build_closed_permissions() -> types.ChatPermissions:
    """
    Чат закрыт: всем нельзя ничего отправлять.
    Админские права (изменение профиля, закрепление) не трогаем, TeleBot их тут не задаёт.
    """
    return types.ChatPermissions(
        can_send_messages=False,
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_messages=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False,
    )


def build_open_permissions() -> types.ChatPermissions:
    """
    Чат открыт: всем можно отправлять всё.
    Админские права не трогаем (они отдельно задаются в ролях/админах).
    """
    return types.ChatPermissions(
        can_send_messages=True,
        can_send_audios=True,
        can_send_documents=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_video_messages=True,
        can_send_video_notes=True,
        can_send_voice_notes=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
    )


def setchatdefaultpermissions(chatid: int, perms: types.ChatPermissions) -> bool:
    try:
        bot.set_chat_permissions(chatid, perms)
        return True
    except ApiTelegramException:
        return False
    except Exception:
        return False


# ==== АВТО-ОТКРЫТИЕ ЧАТА ПО ТАЙМЕРУ ==== 

def schedulereopenchat(chatid: int, delayseconds: int):
    """
    Планирует авто-открытие чата через delayseconds.
    Просто включает все права отправки.
    """
    if delayseconds <= 0:
        return

    def worker():
        time.sleep(delayseconds)
        try:
            state = getclosechatstate(chatid)
            if not state or not state.get("closed"):
                return
            until_ts = state.get("until") or 0
            if until_ts and time.time() < until_ts:
                return

            new_perms = build_open_permissions()
            ok = setchatdefaultpermissions(chatid, new_perms)
            if not ok:
                return

            setclosechatstate(chatid, closed=False, until_ts=0)

            emoji_open = f'<tg-emoji emoji-id="{EMOJI_CHAT_OPEN_BTN_ID}">🔓</tg-emoji>'
            text = f"{emoji_open} <b>Чат снова открыт.</b>"

            try:
                bot.send_message(chatid, text, parse_mode='HTML', disable_web_page_preview=True)
            except Exception:
                pass
        except Exception:
            pass

    t = threading.Thread(target=worker, daemon=True)
    t.start()

# ==== ГРУППОВАЯ СТАТИСТИКА ====

GROUP_STATS = load_json_file(GROUP_STATS_FILE, {})

def save_group_stats():
    throttled_save_json_file(GROUP_STATS_FILE, GROUP_STATS, "group_stats")

GROUP_SETTINGS = load_json_file(GROUP_SETTINGS_FILE, {})

def save_group_settings():
    throttled_save_json_file(GROUP_SETTINGS_FILE, GROUP_SETTINGS, "group_settings")

def get_group_settings(chat_id: int):
    cid = str(chat_id)
    st = GROUP_SETTINGS.get(cid)
    if st is None:
        st = {"auto_stats": True}
        GROUP_SETTINGS[cid] = st
        save_group_settings()
    return st

def update_group_stats(message: types.Message):
    chat = message.chat
    if chat.type not in ['group', 'supergroup']:
        return
    if not is_group_approved(chat.id):
        return
    user = message.from_user
    if not user:
        return

    chat_id = str(chat.id)
    user_id = str(user.id)

    chat_stats = GROUP_STATS.get(chat_id)
    if chat_stats is None:
        chat_stats = {}
        GROUP_STATS[chat_id] = chat_stats

    user_stats = chat_stats.get(user_id)
    if user_stats is None:
        user_stats = {"count": 0, "last_msg_id": None}
        chat_stats[user_id] = user_stats

    user_stats["count"] += 1
    user_stats["last_msg_id"] = message.message_id

    save_group_stats()


# ==== НАСТРОЙКИ ЧАТА (/settings) ====

CHAT_SETTINGS = load_json_file(CHAT_SETTINGS_FILE, {})  # { chat_id_str: { ... }, "pending_*": {...} }
MODERATION = load_json_file(MODERATION_FILE, {})
PENDING_GROUPS = load_json_file(PENDING_GROUPS_FILE, {})  # { chat_id_str: { "title", "adder_id", "adder_username", "added_at", "message_id" } }


def save_chat_settings():
    throttled_save_json_file(CHAT_SETTINGS_FILE, CHAT_SETTINGS, "chat_settings")


def save_moderation():
    throttled_save_json_file(MODERATION_FILE, MODERATION, "moderation")


def save_pending_groups():
    save_json_file(PENDING_GROUPS_FILE, PENDING_GROUPS)


# ==== УПРАВЛЕНИЕ НЕПОДТВЕРЖДЕННЫМИ ГРУППАМИ ====

def is_group_approved(chat_id: int) -> bool:
    cid = str(chat_id)
    return cid not in PENDING_GROUPS


def check_group_approval(m: types.Message) -> bool:
    """Возвращает True если группа подтверждена, False иначе."""
    if m.chat.type not in ['group', 'supergroup']:
        return True  # Приватные чаты всегда OK
    
    if not is_group_approved(m.chat.id):
        emoji_wait = f'<tg-emoji emoji-id="{EMOJI_RATE_LIMIT_ID}">⏳</tg-emoji>'
        bot.reply_to(
            m,
            f"{emoji_wait} Бот находится на модерации. Ожидание подтверждения от разработчика.",
            parse_mode='HTML'
        )
        return False
    return True


def add_pending_group(chat_id: int, chat_title: str, adder_user: types.User) -> None:
    """Добавляет группу в список неподтвержденных."""
    cid = str(chat_id)
    adder_id = adder_user.id if adder_user else 0
    adder_username = (adder_user.username or "unknown") if adder_user else "unknown"
    
    PENDING_GROUPS[cid] = {
        "title": chat_title or "Unknown Group",
        "adder_id": adder_id,
        "adder_username": adder_username,
        "added_at": int(time.time()),
        "message_id": None,
    }
    save_pending_groups()


def approve_pending_group(chat_id: int) -> None:
    """Одобрит группу владельцем (удаляет из список pending)."""
    cid = str(chat_id)
    PENDING_GROUPS.pop(cid, None)
    save_pending_groups()
    get_group_settings(chat_id)  # инициализируем настройки группы
    save_group_settings()


def deny_pending_group(chat_id: int) -> None:
    """Отказывает группе в доступе (удаляет из список pending)."""
    cid = str(chat_id)
    PENDING_GROUPS.pop(cid, None)
    save_pending_groups()


def notify_dev_about_new_group(chat_id: int, chat_title: str, adder_user: types.User) -> None:
    """Отправляет сообщение разработчику при добавлении бота в новую группу."""
    owner_id = _resolve_owner_user_id()
    if not owner_id:
        return
    
    adder_name = f"@{adder_user.username}" if adder_user and adder_user.username else \
                 (f"{adder_user.first_name} {adder_user.last_name}".strip() if adder_user else "Unknown")
    
    emoji_new = f'<tg-emoji emoji-id="{EMOJI_NEW_MSG_OWNER_ID}">📨</tg-emoji>'
    
    text = (
        f"<b>{emoji_new} Новая группа для одобрения</b>\n\n"
        f"<b>Группа:</b> {_html.escape(chat_title or 'Без названия')}\n"
        f"<b>ID:</b> <code>{chat_id}</code>\n"
        f"<b>Добавил:</b> {adder_name} (ID: {adder_user.id if adder_user else 'Unknown'})\n"
        f"<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
        f"<i>Выберите действие ниже.</i>"
    )
    
    keyboard = types.InlineKeyboardMarkup()
    btn_approve = types.InlineKeyboardButton("Разрешить", callback_data=f"approve_group:{chat_id}")
    btn_approve.icon_custom_emoji_id = str(EMOJI_SENT_OK_ID)
    
    btn_deny = types.InlineKeyboardButton("Запретить", callback_data=f"deny_group:{chat_id}")
    btn_deny.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_CANCEL_ID)
    
    keyboard.add(btn_approve, btn_deny)
    
    try:
        bot.send_message(
            owner_id,
            text,
            parse_mode='HTML',
            reply_markup=keyboard
        )
    except Exception as e:
        print(f"[ERROR] Не удалось отправить сообщение разработчику: {e}")

# ==== ПАРСИНГ ВРЕМЕНИ ДЛЯ /closechat ==== 

MAX_CLOSECHAT_SECONDS = 24 * 60 * 60  # 1 день, лимит только для closechat


def parse_closechat_duration(value: str, is_russian: bool) -> int | None:
    """
    is_russian = False -> суффиксы: m, h, d, w, mou, y
    is_russian = True  -> суффиксы: м/мин, ч, д, н, мес, г

    Возвращает секунды или None, если формат неверный или > MAX_CLOSECHAT_SECONDS.
    Если указано "0" или пусто — None (будет трактоваться как 'навсегда' в хендлере).
    """
    if not value:
        return None

    value = value.strip().lower()

    # английские: m, h, d, w, mou, y
    if not is_russian:
        num_part = ''
        unit_part = ''
        for ch in value:
            if ch.isdigit():
                if unit_part:
                    # цифра после юнита -> фигня
                    return None
                num_part += ch
            else:
                unit_part += ch

        if not num_part or not unit_part:
            return None

        try:
            amount = int(num_part)
        except ValueError:
            return None

        if amount <= 0:
            return None

        unit = unit_part
        if unit == 'm':      # minute
            seconds = amount * 60
        elif unit == 'h':    # hour
            seconds = amount * 60 * 60
        elif unit == 'd':    # day
            seconds = amount * 24 * 60 * 60
        elif unit == 'w':    # week
            seconds = amount * 7 * 24 * 60 * 60
        elif unit == 'mou':  # month (условно 30 дней)
            seconds = amount * 30 * 24 * 60 * 60
        elif unit == 'y':    # year (условно 365 дней)
            seconds = amount * 365 * 24 * 60 * 60
        else:
            return None

    # русские: м/мин, ч, д, н, мес, г
    else:
        # возможные варианты "10м", "10 мин", "10мес" и т.п. не допускаем,
        # нам нужен формат точно "10м", "10ч", "10д", "10н", "10мес", "10г"
        num_part = ''
        unit_part = ''
        for ch in value:
            if ch.isdigit():
                if unit_part:
                    # цифра после юнита -> не принимаем
                    return None
                num_part += ch
            else:
                unit_part += ch

        if not num_part or not unit_part:
            return None

        try:
            amount = int(num_part)
        except ValueError:
            return None

        if amount <= 0:
            return None

        unit = unit_part

        if unit in ('м', 'мин'):
            seconds = amount * 60
        elif unit == 'ч':
            seconds = amount * 60 * 60
        elif unit == 'д':
            seconds = amount * 24 * 60 * 60
        elif unit == 'н':
            seconds = amount * 7 * 24 * 60 * 60
        elif unit == 'мес':
            seconds = amount * 30 * 24 * 60 * 60
        elif unit == 'г':
            seconds = amount * 365 * 24 * 60 * 60
        else:
            return None

    # лимит только для closechat
    if seconds > MAX_CLOSECHAT_SECONDS:
        return None

    return seconds


# ==== БД ПОЛЬЗОВАТЕЛЕЙ ПО ЧАТАМ ====

USERS = load_json_file(USERS_FILE, {})  # { chat_id: { user_id: {...} } }

@bot.message_handler(commands=['dbg_users'])
def cmd_dbg_users(m: types.Message):
    chat_id_s = str(m.chat.id)
    chat_users = USERS.get(chat_id_s) or {}
    lines = [f"USERS для чата {m.chat.id}:"]
    if not chat_users:
        lines.append("пусто")
    else:
        for uid, data in chat_users.items():
            lines.append(f"{uid}: @{data.get('username')} {data.get('first_name')}")
    bot.reply_to(m, "\n".join(lines)[:4000])

def save_users():
    throttled_save_json_file(USERS_FILE, USERS, "users")

def update_user_in_chat(chat: types.Chat, user: types.User):
    if chat.type not in ['group', 'supergroup']:
        return
    if not user:
        return

    cid = str(chat.id)
    uid = str(user.id)

    chat_users = USERS.get(cid)
    if chat_users is None:
        chat_users = {}
        USERS[cid] = chat_users

    new_data = {
        "id": user.id,
        "username": (user.username or "").lower(),
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "full_name": user.full_name or user.first_name or ""
    }
    if chat_users.get(uid) == new_data:
        return

    chat_users[uid] = new_data
    save_users()

def find_user_id_by_username_in_chat(chat_id: int, username: str) -> int | None:
    cid = str(chat_id)
    chat_users = USERS.get(cid) or {}
    uname = username.lstrip('@').lower()
    if not uname:
        return None
    for uid, data in chat_users.items():
        if (data.get("username") or "").lower() == uname:
            try:
                return int(uid)
            except ValueError:
                continue
    return None


# ==== ГЛОБАЛЬНАЯ БД ПОЛЬЗОВАТЕЛЕЙ ====
GLOBAL_USERS = load_json_file(GLOBAL_USERS_FILE, {})  # { user_id(str): { ... } }


def save_global_users():
    throttled_save_json_file(GLOBAL_USERS_FILE, GLOBAL_USERS, "global_users")


def update_global_user_from_telebot(user: types.User):
    """Обновить/создать запись в глобальной БД по объекту TeleBot User."""
    if not user:
        return
    uid = str(user.id)
    now_ts = time.time()
    prev = GLOBAL_USERS.get(uid) or {}

    next_rec = {
        "id": user.id,
        "username": (user.username or "").lower(),
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "full_name": user.full_name or user.first_name or "",
        "last_seen": prev.get("last_seen") or 0,
    }

    has_profile_changes = any([
        prev.get("username") != next_rec["username"],
        prev.get("first_name") != next_rec["first_name"],
        prev.get("last_name") != next_rec["last_name"],
        prev.get("full_name") != next_rec["full_name"],
    ])

    prev_seen = float(prev.get("last_seen") or 0)
    should_touch_last_seen = (now_ts - prev_seen) >= GLOBAL_LAST_SEEN_UPDATE_SECONDS
    if should_touch_last_seen:
        next_rec["last_seen"] = now_ts
    else:
        next_rec["last_seen"] = prev_seen

    if not has_profile_changes and not should_touch_last_seen:
        return

    GLOBAL_USERS[uid] = next_rec
    save_global_users()


def update_global_user_basic(user_id: int, username: str | None = None):
    """Обновление глобалки, когда есть только ID и username (например, из MTProto)."""
    uid = str(user_id)
    rec = GLOBAL_USERS.get(uid) or {}
    prev_username = rec.get("username") or ""
    prev_seen = float(rec.get("last_seen") or 0)
    rec["id"] = user_id
    if username:
        rec["username"] = username.lower()
    rec.setdefault("first_name", "")
    rec.setdefault("last_name", "")
    rec.setdefault("full_name", "")

    now_ts = time.time()
    if (now_ts - prev_seen) >= GLOBAL_LAST_SEEN_UPDATE_SECONDS:
        rec["last_seen"] = now_ts
    else:
        rec["last_seen"] = prev_seen

    username_changed = bool(username and prev_username != rec.get("username"))
    if not username_changed and (now_ts - prev_seen) < GLOBAL_LAST_SEEN_UPDATE_SECONDS:
        return

    GLOBAL_USERS[uid] = rec
    save_global_users()


# ==== ПРОФИЛИ ПО ЧАТАМ ====

PROFILES = load_json_file(PROFILES_FILE, {})

def get_profile(chat_id: int, user_id: int) -> dict:
    cid = str(chat_id)
    uid = str(user_id)
    chat_profiles = PROFILES.get(cid)
    if chat_profiles is None:
        chat_profiles = {}
        PROFILES[cid] = chat_profiles
    pr = chat_profiles.get(uid)
    if pr is None:
        pr = {"description": "", "awards": []}
        chat_profiles[uid] = pr
        save_profiles()
    return pr

def save_profiles():
    throttled_save_json_file(PROFILES_FILE, PROFILES, "profiles")


# ==== ДОЛЖНОСТИ И РОЛИ В ЧАТАХ ====
CHAT_ROLES_FILE = os.path.join(DATA_DIR, 'chat_roles.json')
# { chat_id: { user_id: {"rank": int, "role_text": str} } }
CHAT_ROLES = load_json_file(CHAT_ROLES_FILE, {})


def save_chat_roles():
    throttled_save_json_file(CHAT_ROLES_FILE, CHAT_ROLES, "chat_roles")


def get_user_rank(chat_id: int, user_id: int) -> int:
    """
    5 – владелец чата
    4 – главный админ
    3 – админ
    2 – модератор
    1 – стажёр
    0 – обычный участник
    Владелец бота — 999 (бог-режим, вне иерархии).
    """
    # сначала проверяем, не владелец ли это бота
    try:
        u = tg_get_chat(user_id)
    except Exception:
        u = types.User(user_id, False, first_name="", last_name=None, username=None)

    if is_owner(u):
        return 999

    # дальше — обычная логика чата
    try:
        member = tg_get_chat_member(chat_id, user_id)
        if member.status == 'creator':
            return 5
    except Exception:
        pass

    cid = str(chat_id)
    uid = str(user_id)
    chat_roles = CHAT_ROLES.get(cid, {})
    rec = chat_roles.get(uid, {})
    return int(rec.get("rank", 0))


def set_user_rank(chat_id: int, user_id: int, rank: int):
    fake_user = types.User(user_id, False, first_name="", last_name=None, username=None)
    if is_owner(fake_user):
        return

    cid = str(chat_id)
    uid = str(user_id)
    chat_roles = CHAT_ROLES.get(cid)
    if chat_roles is None:
        chat_roles = {}
        CHAT_ROLES[cid] = chat_roles

    if rank <= 0:
        if uid in chat_roles:
            del chat_roles[uid]
    else:
        rec = chat_roles.get(uid) or {}
        rec["rank"] = rank
        chat_roles[uid] = rec

    save_chat_roles()
    tg_invalidate_chat_member_caches(chat_id, user_id)


def get_user_role_text(chat_id: int, user_id: int) -> str:
    return ""


def get_user_custom_tag(chat_id: int, user_id: int) -> str:
    try:
        member = tg_get_chat_member(chat_id, user_id)
        tag = _extract_member_tag(member)
        if tag:
            return tag
    except Exception:
        pass

    lib_member = raw_get_chat_member(chat_id, user_id)
    if lib_member:
        return _extract_member_tag(lib_member)
    return ""


def set_user_role_text(chat_id: int, user_id: int, text: str | None):
    return


def set_user_custom_tag(chat_id: int, user_id: int, tag: str | None):
    return


def _parse_role_and_tag(raw_text: str) -> tuple[str, str | None]:
    text = (raw_text or "").strip()
    if not text:
        return "", None

    if "|" not in text:
        return text, None

    left, right = text.split("|", 1)
    role_text = left.strip()
    tag_text = right.strip()
    return role_text, (tag_text or None)

def get_rank_label_html(rank: int) -> str:
    """
    Возвращает HTML-строку с эмодзи и названием должности по рангу.
    999 – разработчик бота (владелец бота).
    5 – владелец чата.
    4 – главный админ.
    3 – админ.
    2 – модератор.
    1 – стажёр.
    0 – без должности.
    """
    # 999 и выше — разработчик бота (владелец бота)
    if rank >= 999:
        return f'<tg-emoji emoji-id="{EMOJI_DEV_ID}">👨‍💻</tg-emoji> Разработчик бота'

    # 5 — владелец чата
    if rank >= 5:
        return f'<tg-emoji emoji-id="{EMOJI_ROLE_OWNER_ID}">👑</tg-emoji> Владелец чата'
    elif rank == 4:
        return f'<tg-emoji emoji-id="{EMOJI_ROLE_CHIEF_ADMIN_ID}">⭐</tg-emoji> Главный админ'
    elif rank == 3:
        return f'<tg-emoji emoji-id="{EMOJI_ROLE_ADMIN_ID}">🛡️</tg-emoji> Админ'
    elif rank == 2:
        return f'<tg-emoji emoji-id="{EMOJI_ROLE_MOD_ID}">🔧</tg-emoji> Модератор'
    elif rank == 1:
        return f'<tg-emoji emoji-id="{EMOJI_ROLE_TRAINEE_ID}">🎓</tg-emoji> Стажёр'
    else:
        return ""


def get_rank_label_plain(rank: int) -> str:
    """
    Возвращает название должности БЕЗ эмодзи по рангу.
    999 – разработчик бота (владелец бота).
    5 – владелец чата.
    4 – главный админ.
    3 – админ.
    2 – модератор.
    1 – стажёр.
    0 – без должности.
    """
    if rank >= 999:
        return "Разработчик бота"
    if rank >= 5:
        return "Владелец чата"
    elif rank == 4:
        return "Главный админ"
    elif rank == 3:
        return "Админ"
    elif rank == 2:
        return "Модератор"
    elif rank == 1:
        return "Стажёр"
    else:
        return ""


def get_rank_label_instrumental(rank: int) -> str:
    if rank >= 999:
        return "Разработчиком бота"
    if rank >= 5:
        return "Владельцем чата"
    elif rank == 4:
        return "Главным админом"
    elif rank == 3:
        return "Админом"
    elif rank == 2:
        return "Модератором"
    elif rank == 1:
        return "Стажёром"
    return ""


# ==== ПРАВА ДОЛЖНОСТЕЙ (ПО ЧАТАМ) ==== 

ROLE_PERMS_FILE = os.path.join(DATA_DIR, 'role_perms.json')
ROLE_PERMS = load_json_file(ROLE_PERMS_FILE, {})  # { chat_id(str): { rank(str): {perm_name: bool, ...} } }


def save_role_perms():
    throttled_save_json_file(ROLE_PERMS_FILE, ROLE_PERMS, "role_perms")


# ключи прав
PERM_MUTE = "mute"
PERM_UNMUTE = "unmute"
PERM_BAN = "ban"
PERM_UNBAN = "unban"
PERM_WARN = "warn"
PERM_UNWARN = "unwarn"
PERM_KICK = "kick"
PERM_DEL_MSG = "del_msg"           # /del
PERM_VIEW_LISTS = "view_lists"     # warnlist/banlist/mutelist/vlist/adminstats
PERM_CLOSE_CHAT = "close_chat"
PERM_OPEN_CHAT = "open_chat"

# новые права закрепа
PERM_PIN = "pin"                   # закрепить сообщение
PERM_UNPIN = "unpin"               # открепить сообщение

# verify: единое право
PERM_MANAGE_VERIFY = "manage_verify"

# роли/описания
PERM_SET_ROLE_TEXT = "set_role_text"      # /addrole /removerole
PERM_SET_DESC_OTHERS = "set_desc_others"  # менять описание другим
PERM_PROMOTE = "promote"
PERM_DEMOTE = "demote"

# награды: единое право
PERM_MANAGE_AWARDS = "manage_awards"

# настройки чата (/settings)
PERM_SETTINGS = "settings"


ROLE_PERMS_KEYS = [
    (PERM_MUTE, "Ограничить"),
    (PERM_UNMUTE, "Снять ограничение"),
    (PERM_BAN, "Заблокировать"),
    (PERM_UNBAN, "Разблокировать"),
    (PERM_WARN, "Предупреждение"),
    (PERM_UNWARN, "Снять предупреждение"),
    (PERM_KICK, "Исключить"),
    (PERM_VIEW_LISTS, "Списки"),
    (PERM_PIN, "Закрепить"),
    (PERM_UNPIN, "Открепить"),
    (PERM_CLOSE_CHAT, "Закрыть чат"),
    (PERM_OPEN_CHAT, "Открыть чат"),

    (PERM_MANAGE_VERIFY, "Управление верификацией"),

    (PERM_SET_ROLE_TEXT, "Управление тегами"),
    (PERM_SET_DESC_OTHERS, "Управление описаниями других"),
    (PERM_PROMOTE, "Повысить"),
    (PERM_DEMOTE, "Понизить"),

    (PERM_MANAGE_AWARDS, "Управление наградами"),
    (PERM_SETTINGS, "Изменение настроек"),
]


def get_role_perms(chat_id: int, rank: int) -> dict:
    """
    Вернуть словарь прав для данной должности (ранга) в чате.
    Если записи нет — создать.
    Для ранга 5 при первом обращении включаем все права.
    """
    cid = str(chat_id)
    perms_by_rank = ROLE_PERMS.get(cid) or {}
    key = str(rank)
    perms = perms_by_rank.get(key)

    if perms is None:
        if rank == 5:
            perms = {k: True for k, _ in ROLE_PERMS_KEYS}
        else:
            perms = {}
        perms_by_rank[key] = perms
        ROLE_PERMS[cid] = perms_by_rank
        save_role_perms()

    return perms


def has_role_perm(chat_id: int, user_id: int, perm_name: str) -> bool:
    """
    Проверить, есть ли у пользователя право perm_name через его должность.
    Владелец бота и настоящий владелец чата всегда могут всё.
    """
    try:
        u = bot.get_chat(user_id)
    except Exception:
        u = types.User(user_id, False, first_name="", last_name=None, username=None)

    if is_owner(u):
        return True

    try:
        member = bot.get_chat_member(chat_id, user_id)
        if member.status == 'creator':
            return True
    except Exception:
        pass

    rank = get_user_rank(chat_id, user_id)
    if rank <= 0:
        return False

    perms = get_role_perms(chat_id, rank)
    return bool(perms.get(perm_name))


def check_role_permission(chat_id: int, user_id: int, perm_name: str):
    """
    Универсальная проверка:
    - Разработчик бота, dev и creator чата всегда имеют право.
    - Если у пользователя нет должности (rank 0) и он не спец-актер — ('no_rank', False).
    - Если должность есть (rank 1-5), но у этой должности нет perm_name — ('no_perm', False).
    - Если всё ок — ('ok', True).
    """
    try:
        user = bot.get_chat(user_id)
    except Exception:
        user = types.User(user_id, False, first_name="", last_name=None, username=None)

    if _is_special_actor(chat_id, user):
        return 'ok', True

    rank = get_user_rank(chat_id, user_id)
    if rank <= 0:
        return 'no_rank', False

    perms = get_role_perms(chat_id, rank)
    if not perms.get(perm_name):
        return 'no_perm', False

    return 'ok', True


def can_act_on(chat_id: int, actor_id: int, target_id: int) -> bool:
    """
    Нельзя трогать разработчика и фактического владельца.
    Владелец бота может трогать всех остальных.
    Остальные — только если actor_rank > target_rank.
    """
    try:
        target_user = bot.get_chat(target_id)
    except Exception:
        target_user = types.User(target_id, False, first_name="", last_name=None, username=None)

    if is_owner(target_user):
        return False

    try:
        actor_user = bot.get_chat(actor_id)
    except Exception:
        actor_user = types.User(actor_id, False, first_name="", last_name=None, username=None)

    if is_owner(actor_user):
        return True

    try:
        member_t = bot.get_chat_member(chat_id, target_id)
        if member_t.status == 'creator':
            return False
    except Exception:
        pass

    try:
        member_a = bot.get_chat_member(chat_id, actor_id)
        if member_a.status == 'creator':
            return True
    except Exception:
        pass

    actor_rank = get_user_rank(chat_id, actor_id)
    target_rank = get_user_rank(chat_id, target_id)
    return actor_rank > target_rank


def _is_dev_user(user: types.User) -> bool:
    return is_dev(user)


def _user_has_any_rank(chat_id: int, user_id: int) -> bool:
    return get_user_rank(chat_id, user_id) > 0


def _user_can_edit_now(user: types.User, chat_id: int) -> bool:
    """
    Для callback'ов rs_*: разработчик бота, dev-юзер или фактический владелец чата.
    """
    if is_owner(user):
        return True
    if _is_dev_user(user):
        return True
    try:
        member = bot.get_chat_member(chat_id, user.id)
        if member.status == 'creator':
            return True
    except Exception:
        pass
    return False


def _is_special_actor(chat_id: int, user: types.User) -> bool:
    """
    Универсальная проверка для команд:
    разработчик бота, dev-юзер, фактический владелец чата.
    """
    if is_owner(user) or _is_dev_user(user):
        return True
    try:
        member = bot.get_chat_member(chat_id, user.id)
        return member.status == 'creator'
    except Exception:
        return False


def _user_can_open_settings(chat_id: int, user: types.User) -> tuple[bool, str | None]:
    """
    Проверка доступа к /settings.

    Логика:
      - разработчик бота, dev‑юзер, фактический creator чата — всегда могут;
      - ранг 0 — молчит;
      - ранг 1–5 без PERM_SETTINGS — текст ошибки;
      - ранг 1–5 с PERM_SETTINGS — можно.
    """
    if _is_special_actor(chat_id, user):
        return True, None

    status, allowed = check_role_permission(chat_id, user.id, PERM_SETTINGS)
    if allowed:
        return True, None

    if status == 'no_rank':
        return False, None

    if status == 'no_perm':
        return False, "У вашей должности нет права открывать настройки чата."

    return False, "Вы не можете открывать настройки чата."


# ==== Интерфейс настроек прав должностей ==== 


def _build_chats_keyboard_for_owner(user: types.User) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    rows = []
    for chat_id_s, users in (USERS or {}).items():
        try:
            chat_id = int(chat_id_s)
        except ValueError:
            continue

        try:
            member = bot.get_chat_member(chat_id, user.id)
        except Exception:
            continue

        if member.status not in ('administrator', 'creator'):
            continue

        try:
            chat = bot.get_chat(chat_id)
            title = chat.title or str(chat_id)
        except Exception:
            title = str(chat_id)

        btn = InlineKeyboardButton(title[:32], callback_data=f"rs_chat:{chat_id}")
        rows.append([btn])

    if not rows:
        rows.append([InlineKeyboardButton("Нет доступных чатов", callback_data="rs_none")])

    for row in rows:
        kb.row(*row)

    # Отмена только здесь (ЛС), одна кнопка
    kb.row(InlineKeyboardButton(
        "Отмена",
        callback_data="rs_cancel",
        icon_custom_emoji_id=str(EMOJI_ROLE_SETTINGS_CANCEL_ID)
    ))
    return kb


def _build_ranks_keyboard(chat_id: int, for_pm: bool, back_callback: str | None = None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    buttons = [
        InlineKeyboardButton(
            "Владелец чата",
            callback_data=f"rs_rank:{chat_id}:5",
            icon_custom_emoji_id=EMOJI_ROLE_OWNER_ID
        ),
        InlineKeyboardButton(
            "Главный админ",
            callback_data=f"rs_rank:{chat_id}:4",
            icon_custom_emoji_id=EMOJI_ROLE_CHIEF_ADMIN_ID
        ),
        InlineKeyboardButton(
            "Админ",
            callback_data=f"rs_rank:{chat_id}:3",
            icon_custom_emoji_id=EMOJI_ROLE_ADMIN_ID
        ),
        InlineKeyboardButton(
            "Модератор",
            callback_data=f"rs_rank:{chat_id}:2",
            icon_custom_emoji_id=EMOJI_ROLE_MOD_ID
        ),
        InlineKeyboardButton(
            "Стажёр",
            callback_data=f"rs_rank:{chat_id}:1",
            icon_custom_emoji_id=EMOJI_ROLE_TRAINEE_ID
        ),
    ]
    kb.add(*buttons)

    if for_pm:
        kb.row(InlineKeyboardButton(
            "Назад",
            callback_data=(back_callback or "rs_back_chats"),
            icon_custom_emoji_id=str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        ))
    return kb


def _build_perms_keyboard_colored(chat_id: int, rank: int, in_pm: bool) -> dict:
    """
    Цветная клавиатура прав через raw JSON.
    Рядовка:
    Ограничить - Снять ограничение
    Заблокировать - Разблокировать
    Предупреждение - Снять предупреждение
    Кик - Списки
    Закрепить - Открепить
    Закрыть чат - Открыть чат
    Управление верификацией
    Повысить - Понизить
    Управление наградами
    Управление тегами
    Управление описаниями других
    Изменение настроек
    Назад - Сохранить
    """
    perms = get_role_perms(chat_id, rank)
    labels = {k: lbl for k, lbl in ROLE_PERMS_KEYS}

    ordered_keys = [
        (PERM_MUTE, PERM_UNMUTE),
        (PERM_BAN, PERM_UNBAN),
        (PERM_WARN, PERM_UNWARN),
        (PERM_KICK, PERM_VIEW_LISTS),
        (PERM_PIN, PERM_UNPIN),
        (PERM_CLOSE_CHAT, PERM_OPEN_CHAT),
        (PERM_MANAGE_VERIFY, None),
        (PERM_PROMOTE, PERM_DEMOTE),
        (PERM_MANAGE_AWARDS, None),
        (PERM_SET_ROLE_TEXT, None),
        (PERM_SET_DESC_OTHERS, None),
        (PERM_SETTINGS, None),
    ]

    rows = []

    for left_key, right_key in ordered_keys:
        row = []

        if left_key is not None:
            enabled = perms.get(left_key, False)
            style = "success" if enabled else "danger"
            row.append({
                "text": labels.get(left_key, left_key),
                "callback_data": f"rs_perm:{chat_id}:{rank}:{left_key}",
                "style": style,
            })

        if right_key is not None:
            enabled = perms.get(right_key, False)
            style = "success" if enabled else "danger"
            row.append({
                "text": labels.get(right_key, right_key),
                "callback_data": f"rs_perm:{chat_id}:{rank}:{right_key}",
                "style": style,
            })

        if row:
            rows.append(row)

    bottom = []
    if in_pm:
        bottom.append({
            "text": "Назад",
            "callback_data": f"rs_back:{chat_id}",
            "icon_custom_emoji_id": str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID),
        })
    bottom.append({
        "text": "Сохранить",
        "callback_data": f"rs_save:{chat_id}:{rank}",
        "style": "primary",
        "icon_custom_emoji_id": str(EMOJI_ROLE_SETTINGS_SAVE_ID),
    })
    rows.append(bottom)

    return {"inline_keyboard": rows}


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rs_chat:"))
def cb_rolesettings_chat(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id)
    try:
        _, chat_id_s = c.data.split(":", 1)
        chat_id = int(chat_id_s)
    except Exception:
        return

    if not _user_can_edit_now(c.from_user, chat_id):
        return

    emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
    emoji_choose = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHOOSE_RANK_ID}">🔽</tg-emoji>'

    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    kb = _build_ranks_keyboard(chat_id, for_pm=True)
    text = (
        f"{emoji_chat} <b>Настройка прав должностей для чата</b> "
        f"<b>{title}</b> (<code>{chat_id}</code>)\n"
        f"{emoji_choose} <b>Выберите должность для настройки прав:</b>"
    )
    raw_edit_message_with_keyboard(
        c.message.chat.id,
        c.message.message_id,
        text,
        kb
    )


@bot.callback_query_handler(func=lambda c: c.data == "rs_back_chats")
def cb_rolesettings_back_chats(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id)
    if not is_owner(c.from_user):
        return
    kb = _build_chats_keyboard_for_owner(c.from_user)
    emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
    text = f"{emoji_chat} <b>Выбери чат для настройки прав должностей:</b>"
    raw_edit_message_with_keyboard(
        c.message.chat.id,
        c.message.message_id,
        text,
        kb
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rs_rank:"))
def cb_rolesettings_rank(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id)
    try:
        _, chat_id_s, rank_s = c.data.split(":", 2)
        chat_id = int(chat_id_s)
        rank = int(rank_s)
    except Exception:
        return

    if not _user_can_edit_now(c.from_user, chat_id):
        return

    in_pm = (c.message.chat.type == 'private')
    kb_dict = _build_perms_keyboard_colored(chat_id, rank, in_pm=in_pm)

    rank_html = get_rank_label_html(rank) or f"Ранг {rank}"
    emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    text = (
        f"{emoji_chat} <b>Чат:</b> <b>{title}</b> (<code>{chat_id}</code>)\n"
        f"<b>Должность:</b> {rank_html}\n"
        f"Зелёный цвет — включено. Красный цвет — выключено."
    )
    raw_edit_message_with_keyboard(
        c.message.chat.id,
        c.message.message_id,
        text,
        kb_dict
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rs_perm:"))
def cb_rolesettings_perm(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id)
    try:
        _, chat_id_s, rank_s, perm_key = c.data.split(":", 3)
        chat_id = int(chat_id_s)
        rank = int(rank_s)
    except Exception:
        return

    if not _user_can_edit_now(c.from_user, chat_id):
        return

    perms = get_role_perms(chat_id, rank)
    perms[perm_key] = not perms.get(perm_key, False)
    save_role_perms()

    in_pm = (c.message.chat.type == 'private')
    kb_dict = _build_perms_keyboard_colored(chat_id, rank, in_pm=in_pm)

    rank_html = get_rank_label_html(rank) or f"Ранг {rank}"
    emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    text = (
        f"{emoji_chat} <b>Чат:</b> <b>{title}</b> (<code>{chat_id}</code>)\n"
        f"<b>Должность:</b> {rank_html}\n"
        f"Зелёный цвет — включено. Красный цвет — выключено."
    )
    raw_edit_message_with_keyboard(
        c.message.chat.id,
        c.message.message_id,
        text,
        kb_dict
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rs_back:"))
def cb_rolesettings_back_chat(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id)
    try:
        _, chat_id_s = c.data.split(":", 1)
        chat_id = int(chat_id_s)
    except Exception:
        return

    if not _user_can_edit_now(c.from_user, chat_id):
        return

    in_pm = (c.message.chat.type == 'private')
    kb = _build_ranks_keyboard(chat_id, for_pm=in_pm, back_callback=f"st_back_main:{chat_id}" if in_pm else None)

    emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
    emoji_choose = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHOOSE_RANK_ID}">🔽</tg-emoji>'

    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    text = (
        f"{emoji_chat} <b>Настройка прав должностей для чата</b> "
        f"<b>{title}</b> (<code>{chat_id}</code>)\n"
        f"{emoji_choose} <b>Выберите должность для настройки прав:</b>"
    )
    raw_edit_message_with_keyboard(
        c.message.chat.id,
        c.message.message_id,
        text,
        kb
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("rs_save:"))
def cb_rolesettings_save(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id, "Сохранено")
    try:
        _, chat_id_s, rank_s = c.data.split(":", 2)
        chat_id = int(chat_id_s)
        rank = int(rank_s)
    except Exception:
        return

    if not _user_can_edit_now(c.from_user, chat_id):
        return

    perms = get_role_perms(chat_id, rank)
    enabled_pairs = [(key, label) for key, label in ROLE_PERMS_KEYS if perms.get(key)]
    enabled_labels = [label for _, label in enabled_pairs]

    rank_html = get_rank_label_html(rank) or f"Ранг {rank}"

    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    emoji_saved = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
    emoji_enabled = '<tg-emoji emoji-id="5472308992514464048">✔️</tg-emoji>'

    if enabled_labels:
        perms_text = ", ".join(enabled_labels)
    else:
        perms_text = "нет включённых прав"

    text = (
        f"{emoji_saved} <b>Права для должности {rank_html} сохранены.</b>\n"
        f"{emoji_chat} <b>Чат:</b> <b>{title}</b> (<code>{chat_id}</code>)\n"
        f"{emoji_enabled} <b>Включено прав:</b> {perms_text}"
    )

    open_again_kb = {
        "inline_keyboard": [[{
            "text": "Открыть настройки снова",
            "callback_data": f"rs_rank:{chat_id}:{rank}",
            "icon_custom_emoji_id": str(EMOJI_ROLE_SETTINGS_OPEN_AGAIN_ID),
            "style": "primary"
        }]]
    }

    raw_edit_message_with_keyboard(
        c.message.chat.id,
        c.message.message_id,
        text,
        open_again_kb
    )


@bot.callback_query_handler(func=lambda c: c.data == "rs_cancel")
def cb_rolesettings_cancel(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    bot.answer_callback_query(c.id)
    emoji_cancel = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
    try:
        raw_edit_message_with_keyboard(
            c.message.chat.id,
            c.message.message_id,
            f"{emoji_cancel} Настройка прав отменена.",
            None
        )
    except Exception:
        pass

# ==== ВСПОМОГАТЕЛЬНЫЕ ====

def premium_prefix(text: str) -> str:
    return f'<tg-emoji emoji-id="{PREMIUM_PREFIX_EMOJI_ID}">⚠️</tg-emoji> ' + text


COMMAND_COOLDOWNS = {
    "user": {},
    "chat": {},
}

COMMAND_COOLDOWN_NOTICES = {
    "user": {},
    "chat": {},
}


def _cooldown_cache_key(scope: str, bucket: int, action: str) -> str:
    return f"{scope}:{bucket}:{action}"


def _cooldown_notice_once(scope: str, bucket: int, action: str) -> bool:
    store = COMMAND_COOLDOWNS.get(scope)
    notice_store = COMMAND_COOLDOWN_NOTICES.get(scope)
    if not isinstance(store, dict) or not isinstance(notice_store, dict):
        return True

    now_ts = time.time()
    key = _cooldown_cache_key(scope, bucket, action)
    cooldown_until_ts = float(store.get(key, 0) or 0)
    if cooldown_until_ts <= now_ts:
        notice_store.pop(key, None)
        return True

    notified_until_ts = float(notice_store.get(key, 0) or 0)
    if notified_until_ts >= cooldown_until_ts:
        return False

    notice_store[key] = cooldown_until_ts
    return True


def cooldown_hit(scope: str, bucket: int, action: str, seconds: int) -> int:
    store = COMMAND_COOLDOWNS.get(scope)
    if not isinstance(store, dict):
        return 0

    now_ts = time.time()
    key = _cooldown_cache_key(scope, bucket, action)
    until_ts = float(store.get(key, 0) or 0)

    if until_ts > now_ts:
        return max(1, int(until_ts - now_ts))

    store[key] = now_ts + int(seconds)
    return 0


def reply_cooldown_message(
    m: types.Message,
    wait_seconds: int,
    *,
    scope: Optional[str] = None,
    bucket: Optional[int] = None,
    action: Optional[str] = None,
):
    if scope and bucket is not None and action:
        if not _cooldown_notice_once(scope, bucket, action):
            return None

    text = (
        f'<tg-emoji emoji-id="{EMOJI_RATE_LIMIT_ID}">⏳</tg-emoji> '
        f"Подожди <b>{wait_seconds}</b> сек. и повтори команду."
    )
    return bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


TEXTS = {
    "group_only": "Команда доступна только в группах.",
    "user_not_found": "Не удалось определить пользователя.",
    "no_perm_verify": "У вашей должности нет права управлять верификацией.",
    "no_perm_view_verify": "У вашей должности нет права смотреть список верифицированных пользователей.",
    "verify_already": "Этот пользователь уже имеет статус верифицированного.",
    "verify_missing": "У этого пользователя нет статуса верификации.",
    "verify_list_empty": "В этом чате нет верифицированных пользователей.",
    "no_perm_del": "У вашей должности нет права удалять сообщения.",
    "bot_no_del": "У бота нет права удалять сообщения.",
}


def _t(key: str, default: str = "") -> str:
    return TEXTS.get(key, default or key)


def reply_error(m: types.Message, key_or_text: str):
    text = _t(key_or_text, key_or_text)
    return bot.reply_to(m, premium_prefix(text), parse_mode='HTML', disable_web_page_preview=True)

def mention_html_by_id(user_id: int, fallback_name: str = "Пользователь") -> str:
    return f'<a href="tg://user?id={user_id}">{fallback_name}</a>'

def mention_html(user: telebot.types.User) -> str:
    name = user.full_name or user.first_name or user.username or "Пользователь"
    return mention_html_by_id(user.id, name)


def mention_html_user(user: telebot.types.User) -> str:
    return mention_html(user)

def stats_link_for_user(chat_id: int, user_id: int) -> str:
    chat_id_s = str(chat_id)
    user_id_s = str(user_id)

    chat_users = USERS.get(chat_id_s) or {}
    data = chat_users.get(user_id_s) or {}

    username = data.get("username") or ""
    full_name = data.get("full_name") or data.get("first_name") or "Пользователь"

    if username:
        # t.me/username
        return f'[{full_name}](https://t.me/{username})'
    else:
        # fallback для Markdown-статистики, если понадобится
        return f'[{full_name}](tg://openmessage?user_id={user_id})'

def is_owner(user: types.User | None) -> bool:
    """
    Владелец бота — это пользователь с username = OWNER_USERNAME.
    """
    if not user:
        return False
    uname = (user.username or "").lower()
    return uname == (OWNER_USERNAME or "").lower()

def is_dev(user) -> bool:
    return is_owner(user) or (user.id in VERIFY_DEV)

def is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = tg_get_chat_member(chat_id, user_id)
        return member.status in ["administrator", "creator"]
    except Exception:
        return False

def deny_access(chat_id):
    bot.send_message(chat_id, premium_prefix("Бот недоступен для вашего аккаунта."))

def is_exact_stat(text: str) -> bool:
    return bool(text) and text.strip().lower() == 'статистика'

def text_starts_with_ci(text: str, prefix: str) -> bool:
    return bool(text) and text.strip().lower().startswith(prefix.lower())

def format_bytes_mb(num_bytes: int) -> str:
    return f"{num_bytes / (1024 * 1024):.1f} MB"

def match_command(text: str, name: str) -> bool:
    if not text:
        return False
    t = text.strip()
    first = t.split(maxsplit=1)[0].lower()
    for prefix in COMMAND_PREFIXES:
        if first.startswith(prefix):
            body = first[len(prefix):]
            body = body.split('@', 1)[0]
            return body == name.lower()
    return False


def match_command_aliases(text: str, names: list[str]) -> bool:
    if not text:
        return False

    first = text.strip().split(maxsplit=1)[0].lower()
    normalized = {str(n).lower() for n in (names or []) if str(n).strip()}
    if not normalized:
        return False

    for prefix in COMMAND_PREFIXES:
        if first.startswith(prefix):
            body = first[len(prefix):]
            body = body.split('@', 1)[0]
            return body in normalized

    return first in normalized

def parse_target_user(m: types.Message, args: list[str]) -> int | None:
    # 1) если есть reply — всегда берем из reply
    if m.reply_to_message:
        update_user_in_chat(m.chat, m.reply_to_message.from_user)
        return m.reply_to_message.from_user.id

    if not args:
        return None

    text = args[0].strip()

    # 2) ID
    if text.isdigit():
        try:
            return int(text)
        except ValueError:
            return None

    # 3) t.me/ссылка (любой формат t.me/...)
    if text.startswith("https://t.me/") or text.startswith("http://t.me/") or text.startswith("t.me/"):
        link = text.split("t.me/")[1]
        if "/" in link:
            uname = link.split("/")[0]
        else:
            uname = link
        uname = uname.lstrip("@")
        uname_low = uname.lower()

        chat_id_s = str(m.chat.id)
        chat_users = USERS.get(chat_id_s) or {}

        # сначала ищем в локальной USERS по username
        for uid, data in chat_users.items():
            un = (data.get("username") or "").lower()
            if un == uname_low:
                return int(uid)

        # потом пробуем Bot API
        try:
            ch = bot.get_chat(f"@{uname}")
            if isinstance(ch, types.User):
                return ch.id
            return ch.id
        except Exception:
            # если Bot API не смог — MTProto
            loop = asyncio.get_event_loop()
            try:
                mt_id = loop.run_until_complete(get_user_id_by_username_mtproto(uname))
            except RuntimeError:
                mt_id = asyncio.run(get_user_id_by_username_mtproto(uname))
            return mt_id

    # 4) @username
    if text.startswith("@"):
        uname = text.lstrip("@")
        uname_low = uname.lower()

        chat_id_s = str(m.chat.id)
        chat_users = USERS.get(chat_id_s) or {}

        # сначала ищем в локальной USERS
        for uid, data in chat_users.items():
            un = (data.get("username") or "").lower()
            if un == uname_low:
                return int(uid)

        # потом Bot API
        try:
            ch = bot.get_chat(f"@{uname}")
            if isinstance(ch, types.User):
                return ch.id
            return ch.id
        except Exception:
            # и снова MTProto
            loop = asyncio.get_event_loop()
            try:
                mt_id = loop.run_until_complete(get_user_id_by_username_mtproto(uname))
            except RuntimeError:
                mt_id = asyncio.run(get_user_id_by_username_mtproto(uname))
            return mt_id

    # если ничего не подошло
    return None

def link_for_user(chat_id: int, user_id: int) -> str:
    chat_id_s = str(chat_id)
    user_id_s = str(user_id)
    chat_users = USERS.get(chat_id_s) or {}
    data = chat_users.get(user_id_s) or {}

    username = (data.get("username") or "").lower()
    full_name = data.get("full_name") or data.get("first_name") or "Без имени"

    if username:
        url = f"https://t.me/{username}"
    else:
        url = f"tg://openmessage?user_id={user_id}"

    return f'<a href="{url}">{full_name}</a>'


def resolve_target_for_dev(m: types.Message) -> int | None:
    """
    Резолв цели для /devverify и /devunverify в ЛС.
    Поддерживает: reply, ID, t.me/username, @username.
    """
    # 1) reply
    if m.reply_to_message and m.reply_to_message.from_user:
        return m.reply_to_message.from_user.id

    # 2) аргументы
    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    if not raw_after:
        return None
    text = raw_after.strip()

    # 3) числовой ID
    if text.isdigit():
        try:
            return int(text)
        except ValueError:
            return None

    # 4) t.me/username или @username
    if text.startswith("https://t.me/") or text.startswith("http://t.me/") or text.startswith("t.me/"):
        link = text.split("t.me/")[1]
        if "/" in link:
            username = link.split("/")[0]
        else:
            username = link
        uname = username.lstrip("@").strip().lower()
    else:
        uname = text.lstrip("@").strip().lower()

    if not uname:
        return None

    # 5) поиск в GLOBAL_USERS по username
    for uid, data in (GLOBAL_USERS or {}).items():
        un = (data.get("username") or "").lower()
        if un and un == uname:
            try:
                return int(uid)
            except ValueError:
                continue

    # 6) MTProto, если в глобалке нет
    try:
        loop = asyncio.get_event_loop()
        mtid = loop.run_until_complete(get_user_id_by_username_mtproto(uname))
    except RuntimeError:
        mtid = asyncio.run(get_user_id_by_username_mtproto(uname))

    if mtid:
        # обновим глобалку минимальным набором
        update_global_user_basic(mtid, uname)
        return mtid

    return None


def resolve_target_like_profile(m: types.Message) -> int | None:
    """
    reply / id / @username / t.me → target_id.
    Для verify/unverify без указателя/ответа возвращает None.
    """
    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    if m.reply_to_message or args:
        return parse_target_user(m, args)
    return None


def is_verified_admin(chat: types.Chat, user_id: int) -> bool:
    chat_id_s = str(chat.id)
    return user_id in (VERIFY_ADMINS.get(chat_id_s) or [])


@bot.message_handler(commands=['verify'])
def cmd_verify(m: types.Message):
    add_stat_message(m)
    add_stat_command('verify')

    if m.chat.type not in ['group', 'supergroup']:
        return reply_error(m, "group_only")

    target_id = resolve_target_like_profile(m)

    if isinstance(target_id, int) and target_id < 1000:
        return reply_error(m, "user_not_found")

    if target_id is None:
        return reply_error(m, "user_not_found")

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_MANAGE_VERIFY)
    if not allowed:
        if status == 'no_perm':
            return reply_error(m, "no_perm_verify")
        # rank 0 — молчание
        return

    chat_id_s = str(m.chat.id)
    users = VERIFY_ADMINS.get(chat_id_s) or []

    if target_id in users:
        return reply_error(m, "verify_already")

    users.append(target_id)
    VERIFY_ADMINS[chat_id_s] = users
    save_verify_admins()

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_VERIFY_ADMIN_ID}">✔️</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>получил статус верифицированного пользователя.</b>"
    )
    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(commands=['unverify'])
def cmd_unverify(m: types.Message):
    add_stat_message(m)
    add_stat_command('unverify')

    if m.chat.type not in ['group', 'supergroup']:
        return reply_error(m, "group_only")

    target_id = resolve_target_like_profile(m)

    if isinstance(target_id, int) and target_id < 1000:
        return reply_error(m, "user_not_found")

    if target_id is None:
        return reply_error(m, "user_not_found")

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_MANAGE_VERIFY)
    if not allowed:
        if status == 'no_perm':
            return reply_error(m, "no_perm_verify")
        return

    chat_id_s = str(m.chat.id)
    users = VERIFY_ADMINS.get(chat_id_s) or []

    if target_id not in users:
        return reply_error(m, "verify_missing")

    users.remove(target_id)
    VERIFY_ADMINS[chat_id_s] = users
    save_verify_admins()

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_VERIFY_ADMIN_ID}">✔️</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>лишён статуса верифицированного пользователя.</b>"
    )
    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(commands=['vlist'])
def cmd_vlist(m: types.Message):
    add_stat_message(m)
    add_stat_command('vlist')

    wait_seconds = cooldown_hit('chat', int(m.chat.id), 'vlist', 15)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='vlist')

    if m.chat.type not in ['group', 'supergroup']:
        return reply_error(m, "group_only")

    # спец-актеры всегда могут, остальные — через PERM_VIEW_LISTS
    if not _is_special_actor(m.chat.id, m.from_user):
        status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_VIEW_LISTS)
        if not allowed:
            if status == 'no_perm':
                return reply_error(m, "no_perm_view_verify")
            # rank 0 — молчание
            return

    chat_id_s = str(m.chat.id)
    users = VERIFY_ADMINS.get(chat_id_s) or []

    if not users:
        return reply_error(m, "verify_list_empty")

    lines = []
    for uid in users:
        name = link_for_user(m.chat.id, uid)
        lines.append(f"•  {name} [<code>{uid}</code>]")

    text = (
        f'<tg-emoji emoji-id="{EMOJI_VERIFY_ADMIN_ID}">✔️</tg-emoji> '
        f"<b>Верифицированные пользователи:</b>\n" +
        "\n".join(lines)
    )
    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(commands=['devverify'])
def cmd_devverify(m: types.Message):
    add_stat_message(m)
    add_stat_command('devverify')

    # Только владелец и только ЛС, для остальных — тихо выходим
    if m.chat.type != 'private':
        return
    if not is_owner(m.from_user):
        return

    target_id = resolve_target_for_dev(m)
    if not isinstance(target_id, int) or target_id < 1:
        return bot.reply_to(
            m,
            premium_prefix("Пользователь не найден. Укажи ID, @username или ответь на его сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if target_id in VERIFY_DEV:
        return bot.reply_to(
            m,
            premium_prefix("У пользователя уже есть статус глобального разработчика."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    VERIFY_DEV.add(target_id)
    save_verify_dev()

    # Попробуем получить инфу о юзере и обновить глобалку
    name_html = f"ID <code>{target_id}</code>"
    try:
        u = bot.get_chat(target_id)
        if isinstance(u, types.User):
            update_global_user_from_telebot(u)
            name_html = mention_html_user(u)
    except Exception:
        pass

    text = (
        f'<tg-emoji emoji-id="{EMOJI_VERIFY_DEV_ID}">✔️</tg-emoji> '
        f"{name_html} получил статус глобального разработчика."
    )
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)

@bot.message_handler(commands=['devunverify'])
def cmd_devunverify(m: types.Message):
    add_stat_message(m)
    add_stat_command('devunverify')

    # Только владелец и только ЛС
    if m.chat.type != 'private':
        return
    if not is_owner(m.from_user):
        return

    target_id = resolve_target_for_dev(m)
    if not isinstance(target_id, int) or target_id < 1:
        return bot.reply_to(
            m,
            premium_prefix("Пользователь не найден. Укажи ID, @username или ответь на его сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if target_id not in VERIFY_DEV:
        return bot.reply_to(
            m,
            premium_prefix("У пользователя нет статуса глобального разработчика."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    VERIFY_DEV.remove(target_id)
    save_verify_dev()

    name_html = f"ID <code>{target_id}</code>"
    try:
        u = bot.get_chat(target_id)
        if isinstance(u, types.User):
            update_global_user_from_telebot(u)
            name_html = mention_html_user(u)
    except Exception:
        pass

    text = (
        f'<tg-emoji emoji-id="{EMOJI_VERIFY_DEV_ID}">✔️</tg-emoji> '
        f"С пользователя {name_html} снят статус глобального разработчика."
    )
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(commands=['devvlist'])
def cmd_devvlist(m: types.Message):
    add_stat_message(m)
    add_stat_command('devvlist')

    # Только ЛС
    if m.chat.type != 'private':
        return

    # Только владелец и dev-юзеры
    if not is_dev(m.from_user):
        return

    if not VERIFY_DEV:
        return bot.reply_to(
            m,
            premium_prefix("Нет пользователей с глобальной верификацией разработчика."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    lines = []
    for uid in sorted(VERIFY_DEV):
        name_html = f"ID <code>{uid}</code>"

        # сначала пробуем GLOBAL_USERS
        gu = GLOBAL_USERS.get(str(uid)) if 'GLOBAL_USERS' in globals() else None
        if gu:
            username = gu.get("username") or ""
            full_name = gu.get("full_name") or gu.get("first_name") or ""
            if username:
                if full_name:
                    name_html = f"@{username} ({full_name}) [<code>{uid}</code>]"
                else:
                    name_html = f"@{username} [<code>{uid}</code>]"
            elif full_name:
                name_html = f"{full_name} [<code>{uid}</code>]"

        # если в глобалке нет — пробуем Bot API и сразу обновляем GLOBAL_USERS
        if not gu:
            try:
                u = bot.get_chat(uid)
                if isinstance(u, types.User):
                    update_global_user_from_telebot(u)
                    name_html = f"{mention_html_user(u)} [<code>{uid}</code>]"
            except Exception:
                pass

        lines.append(
            f'<tg-emoji emoji-id="{EMOJI_VERIFY_DEV_ID}">✔️</tg-emoji> {name_html}'
        )

    text = (
        "<b>Глобально верифицированные разработчики:</b>\n" +
        "\n".join(lines)
    )

    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(commands=['dbg_global_users'])
def cmd_dbg_global_users(m: types.Message):
    add_stat_message(m)
    add_stat_command('dbg_global_users')

    if not is_owner(m.from_user):
        return  # тихо игнорируем

    total = len(GLOBAL_USERS or {})
    lines = [f"GLOBAL_USERS: всего {total} пользователей."]

    # покажем до 50 строк для примера
    max_show = 50
    count = 0
    for uid, data in (GLOBAL_USERS or {}).items():
        username = data.get("username") or "-"
        full_name = data.get("full_name") or (data.get("first_name") or "")
        lines.append(f"{uid}: @{username} {full_name}")
        count += 1
        if count >= max_show:
            break

    if total > max_show:
        lines.append(f"... и ещё {total - max_show} записей")

    text = "\n".join(lines)
    bot.reply_to(m, text[:4000])

@bot.message_handler(commands=['migrate_users_to_global'])
def cmd_migrate_users_to_global(m: types.Message):
    add_stat_message(m)
    add_stat_command('migrate_users_to_global')

    # Только владелец и только ЛС
    if m.chat.type != 'private':
        return
    if not is_owner(m.from_user):
        return

    migrated = 0
    seen = set()

    # USERS: { chat_id: { user_id: {...} } }
    for chat_id_s, chat_users in (USERS or {}).items():
        for uid, data in (chat_users or {}).items():
            try:
                user_id = int(uid)
            except ValueError:
                continue
            if user_id in seen:
                continue
            seen.add(user_id)

            username = (data.get("username") or "").lower() or None
            full_name = data.get("full_name") or ""
            first_name = data.get("first_name") or ""
            last_name = data.get("last_name") or ""

            # базовая запись
            rec = GLOBAL_USERS.get(uid) or {}
            rec["id"] = user_id
            if username:
                rec["username"] = username
            if full_name:
                rec["full_name"] = full_name
            if first_name:
                rec["first_name"] = first_name
            if last_name:
                rec["last_name"] = last_name
            rec["last_seen"] = time.time()
            GLOBAL_USERS[uid] = rec
            migrated += 1

    save_global_users()

    text = premium_prefix(f"Миграция завершена. Добавлено/обновлено {migrated} пользователей в GLOBAL_USERS.")
    bot.reply_to(m, text)


@bot.message_handler(commands=['dbmigrate', 'sqlite_migrate'])
def cmd_dbmigrate(m: types.Message):
    add_stat_message(m)
    add_stat_command('dbmigrate')

    if m.chat.type != 'private':
        return
    if not is_owner(m.from_user):
        return

    stats = migrate_legacy_json_to_sqlite()
    text = (
        "<b>Миграция JSON → SQLite завершена</b>\n"
        f"Перенесено: <code>{stats['migrated']}</code>\n"
        f"Пропущено (файл отсутствует): <code>{stats['skipped']}</code>\n"
        f"Ошибок: <code>{stats['failed']}</code>\n"
        f"Всего путей проверено: <code>{stats['total']}</code>"
    )
    bot.reply_to(m, text, parse_mode='HTML')


@bot.message_handler(commands=['dbstatus', 'sqlite_status'])
def cmd_dbstatus(m: types.Message):
    add_stat_message(m)
    add_stat_command('dbstatus')

    if m.chat.type != 'private':
        return
    if not is_owner(m.from_user):
        return

    st = get_sqlite_status()
    latest = int(st.get('latest_updated_at') or 0)
    latest_h = datetime.fromtimestamp(latest).strftime('%d.%m.%Y %H:%M:%S') if latest > 0 else "-"
    keys = st.get('keys') or []
    keys_preview = "\n".join(f"• <code>{_html.escape(str(k))}</code>" for k in keys) if keys else "• -"

    text = (
        "<b>SQLite статус</b>\n"
        f"Файл: <code>{_html.escape(str(st.get('db_path') or '-'))}</code>\n"
        f"Существует: <code>{'да' if st.get('exists') else 'нет'}</code>\n"
        f"Размер: <code>{int(st.get('size_bytes') or 0)}</code> bytes\n"
        f"Записей kv_store: <code>{int(st.get('rows') or 0)}</code>\n"
        f"Последнее обновление: <code>{latest_h}</code>\n"
        f"Fallback JSON при ошибке SQLite: <code>{'on' if SQLITE_JSON_FALLBACK_WRITE else 'off'}</code>\n"
        "\n"
        "<b>Последние ключи:</b>\n"
        f"{keys_preview}"
    )
    if st.get('error'):
        text += f"\n\n<b>Ошибка:</b> <code>{_html.escape(str(st['error']))}</code>"

    bot.reply_to(m, text, parse_mode='HTML')


@bot.message_handler(commands=['botstatus'])
def cmd_botstatus(m: types.Message):
    add_stat_message(m)
    add_stat_command('botstatus')

    if m.chat.type != 'private':
        return
    if not is_owner(m.from_user):
        return

    sqlite_status = get_sqlite_status()
    cache_stats = get_tg_cache_stats()
    queue_stats = get_operation_queue_stats()
    latest = int(sqlite_status.get('latest_updated_at') or 0)
    latest_h = datetime.fromtimestamp(latest).strftime('%d.%m.%Y %H:%M:%S') if latest > 0 else '-'

    try:
        proc = psutil.Process(os.getpid())
        process_ram = format_bytes_mb(proc.memory_info().rss)
    except Exception:
        process_ram = 'n/a'

    text = (
        '<b>Состояние бота</b>\n'
        f"<b>Uptime:</b> <code>{get_uptime_text()}</code>\n"
        f"<b>Обработано сообщений:</b> <code>{int(STATS.get('messages') or 0)}</code>\n"
        f"<b>Уникальных пользователей:</b> <code>{len(STATS.get('users') or set())}</code>\n"
        f"<b>Чатов runtime:</b> <code>{len(STATS.get('chats') or set())}</code>\n"
        f"<b>RAM процесса:</b> <code>{process_ram}</code>\n"
        '\n'
        '<b>TG кэш</b>\n'
        f"<b>Member cache:</b> <code>{cache_stats['member_size']}</code>\n"
        f"<b>Chat cache:</b> <code>{cache_stats['chat_size']}</code>\n"
        f"<b>Всего записей кэша:</b> <code>{cache_stats['total_size']}</code>\n"
        f"<b>Cache misses:</b> <code>{cache_stats['total_misses']}</code>"
        f" (member: <code>{cache_stats['member_misses']}</code>, chat: <code>{cache_stats['chat_misses']}</code>)\n"
        '\n'
        '<b>SQLite</b>\n'
        f"<b>Файл:</b> <code>{_html.escape(str(sqlite_status.get('db_path') or '-'))}</code>\n"
        f"<b>Записей kv_store:</b> <code>{int(sqlite_status.get('rows') or 0)}</code>\n"
        f"<b>Размер файла:</b> <code>{int(sqlite_status.get('size_bytes') or 0)}</code> bytes\n"
        f"<b>Последнее обновление:</b> <code>{latest_h}</code>\n"
        f"<b>Ошибок SQLite:</b> <code>{int(STATS.get('sqlite_errors') or 0)}</code>\n"
        '\n'
        '<b>Очередь операций</b>\n'
        f"<b>В работе:</b> <code>{queue_stats['active']}</code>\n"
        f"<b>В очереди:</b> <code>{queue_stats['queued']}</code>\n"
        f"<b>Всего задач:</b> <code>{queue_stats['total']}</code>"
    )

    if sqlite_status.get('error'):
        text += f"\n\n<b>Ошибка SQLite:</b> <code>{_html.escape(str(sqlite_status['error']))}</code>"

    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)

# ==== СТАТИСТИКА БОТА ====

STATS = {
    'users': set(),
    'chats': set(),
    'messages': 0,
    'commands_used': {},
    'start_time': time.time(),
    'sqlite_errors': 0,
    'tg_cache_chat_misses': 0,
    'tg_cache_member_misses': 0,
}

def add_stat_message(message: types.Message):
    STATS['messages'] += 1
    STATS['users'].add(message.from_user.id)
    STATS['chats'].add(message.chat.id)
    _remember_owner_user_id(message.from_user)
    update_group_stats(message)
    # обновляем БД пользователей по чатам
    update_user_in_chat(message.chat, message.from_user)
    # обновляем глобальную БД пользователей
    update_global_user_from_telebot(message.from_user)

def add_stat_command(cmd: str):
    STATS['commands_used'][cmd] = STATS['commands_used'].get(cmd, 0) + 1


def get_tg_cache_stats() -> dict[str, int]:
    with _TG_CACHE_LOCK:
        member_size = len(_TG_MEMBER_CACHE)
        chat_size = len(_TG_CHAT_CACHE)
    return {
        'member_size': member_size,
        'chat_size': chat_size,
        'total_size': member_size + chat_size,
        'member_misses': int(STATS.get('tg_cache_member_misses') or 0),
        'chat_misses': int(STATS.get('tg_cache_chat_misses') or 0),
        'total_misses': int(STATS.get('tg_cache_member_misses') or 0) + int(STATS.get('tg_cache_chat_misses') or 0),
    }

def get_uptime_text() -> str:
    sec = int(time.time() - STATS['start_time'])
    d, sec = divmod(sec, 86400)
    h, sec = divmod(sec, 3600)
    m, sec = divmod(sec, 60)
    parts = []
    if d: parts.append(f"{d}д")
    if h: parts.append(f"{h}ч")
    if m: parts.append(f"{m}м")
    parts.append(f"{sec}с")
    return " ".join(parts)

def get_top_commands(limit=5):
    if not STATS['commands_used']:
        return "нет данных"
    sorted_cmds = sorted(STATS['commands_used'].items(), key=lambda x: x[1], reverse=True)
    return "\n".join(f"/{cmd} — <code>{cnt}</code>" for cmd, cnt in sorted_cmds[:limit])


# ==== NEW CHAT OR MEMBERS ====

@bot.message_handler(content_types=['new_chat_members'])
def on_new_members(message: types.Message):
    add_stat_message(message)

    me = bot.get_me()
    bot_added = False

    for member in (message.new_chat_members or []):
        # сохраняем юзера в БД (только если группа подтверждена)
        if is_group_approved(message.chat.id):
            update_user_in_chat(message.chat, member)

        # если добавили самого бота
        if member.id == me.id:
            bot_added = True

    # если добавили бота — отправляем сообщение о ожидании подтверждения
    if bot_added:
        adder = message.from_user
        chat_title = message.chat.title or "Группа"
        
        # Добавляем группу в список неподтвержденных
        add_pending_group(message.chat.id, chat_title, adder)
        
        # Отправляем сообщение в чат
        emoji_wait = f'<tg-emoji emoji-id="{EMOJI_RATE_LIMIT_ID}">⏳</tg-emoji>'
        text = (
              f"{emoji_wait} <b>Бот добавлен в чат!</b>\n\n"
              f"Чат <b>{_html.escape(chat_title)}</b> ожидает подтверждения от разработчика.\n"
              f"До подтверждения бот не будет реагировать на команды.\n\n"
              f"⏳ <i>Ожидайте проверки...</i>"
        )
        try:
            bot.send_message(
                message.chat.id,
                text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        except Exception:
            pass

        # Отправляем уведомление разработчику
        notify_dev_about_new_group(message.chat.id, chat_title, adder)

    # ВАЖНО: не блокируем другие хендлеры new_chat_members (welcome + cleanup system)
    return ContinueHandling()

# ==== БАЗОВЫЕ КОМАНДЫ ====

START_MENU_STATE: dict[tuple[int, int], dict] = {}


def _build_start_home_keyboard(show_owner_button: bool = False) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("Команды", callback_data="start:commands"),
        types.InlineKeyboardButton("О боте", callback_data="start:about"),
    )
    kb.row(types.InlineKeyboardButton("Связь с разработчиком", callback_data="start:contact"))
    if show_owner_button:
        btn_new = types.InlineKeyboardButton("Новые сообщения", callback_data="start:newmsgs")
        btn_new.icon_custom_emoji_id = EMOJI_NEW_MSG_OWNER_ID
        kb.row(btn_new)
    return kb


def _build_start_back_keyboard(back_to: str = "start:home") -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    btn_back = types.InlineKeyboardButton("Назад", callback_data=back_to)
    btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
    kb.row(btn_back)
    return kb


def _build_start_commands_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("Инструкции применения команд", callback_data="start:usage"))
    btn_back = types.InlineKeyboardButton("Назад", callback_data="start:home")
    btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
    kb.row(btn_back)
    return kb


def _user_has_admin_section(user_id: int) -> bool:
    if user_id in VERIFY_DEV:
        return True

    for chat_id_s, chat_users in (USERS or {}).items():
        if str(user_id) not in (chat_users or {}):
            continue
        try:
            chat_id = int(chat_id_s)
        except Exception:
            continue
        if get_user_rank(chat_id, user_id) > 0:
            return True

    for users in (VERIFY_ADMINS or {}).values():
        if user_id in (users or []):
            return True

    return False


def _dev_contact_intro_text() -> str:
    emoji_contact = f'<tg-emoji emoji-id="{EMOJI_CONTACT_DEV_ID}">💬</tg-emoji>'
    return (
        f"{emoji_contact} <b>Связь с разработчиком</b>\n\n"
        "Здесь можно отправить вопрос, идею или жалобу разработчику бота.\n"
        "Пожалуйста, пишите по делу — пустые и мусорные сообщения игнорируются."
    )


def _dev_contact_prompt_text() -> str:
    emoji_send = f'<tg-emoji emoji-id="{EMOJI_SEND_TEXT_PROMPT_ID}">✉️</tg-emoji>'
    return (
        f"{emoji_send} <b>Отправьте текстовое сообщение...</b>\n\n"
        "<i>Если вы отправите несколько сообщений, будет обработано только первое.</i>"
    )


def _dev_contact_prompt_kb() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    btn_cancel = types.InlineKeyboardButton("Отменить", callback_data="devcontact:cancel")
    btn_cancel.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_CANCEL_ID)
    kb.row(btn_cancel)
    return kb


def _dev_contact_owner_new_text(item: dict) -> str:
    emoji_new = f'<tg-emoji emoji-id="{EMOJI_NEW_MSG_OWNER_ID}">📨</tg-emoji>'
    user_id = int(item.get("user_id") or 0)
    name = _html.escape(item.get("user_full_name") or "Пользователь")
    username = _html.escape(item.get("user_username") or "")
    username_part = f" (@{username})" if username else ""
    body = _html.escape(item.get("text") or "")
    mention = mention_html_by_id(user_id, name)
    return (
        f"{emoji_new} <b>Получено новое сообщение от пользователя!</b>\n\n"
        f"<b>Пользователь:</b> {mention}{username_part}\n\n"
        "<b>Текст сообщения:</b>\n"
        f"<blockquote>{body}</blockquote>"
    )


def _dev_contact_owner_item_kb(item_id: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()

    btn_reply = types.InlineKeyboardButton("Ответить", callback_data=f"devmsg:reply:{item_id}")
    btn_reply.icon_custom_emoji_id = EMOJI_REPLY_BTN_ID

    btn_ignore = types.InlineKeyboardButton("Проигнорировать", callback_data=f"devmsg:ignore:{item_id}")
    btn_ignore.icon_custom_emoji_id = EMOJI_IGNORE_BTN_ID

    kb.row(btn_reply, btn_ignore)
    return kb


def _dev_contact_intro_kb() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("Отправить сообщение", callback_data="devcontact:send"))

    btn_back = types.InlineKeyboardButton("Назад", callback_data="devcontact:back")
    btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
    kb.row(btn_back)
    return kb


def _append_dev_contact_item(user: types.User, text: str) -> dict:
    item = {
        "id": _dev_contact_new_id(),
        "status": "new",
        "created_at": int(time.time()),
        "user_id": int(user.id),
        "user_full_name": user.full_name or user.first_name or "Пользователь",
        "user_username": (user.username or ""),
        "text": text,
        "owner_notified_at": 0,
        "replied_at": 0,
        "reply_text": "",
    }
    DEV_CONTACT_INBOX.setdefault("items", []).append(item)
    save_dev_contact_inbox()
    return item


def _send_dev_contact_item_to_owner(item: dict) -> bool:
    owner_id = _resolve_owner_user_id()
    if not owner_id:
        return False

    try:
        bot.send_message(
            owner_id,
            _dev_contact_owner_new_text(item),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=_dev_contact_owner_item_kb(int(item.get("id") or 0)),
        )
        item["owner_notified_at"] = int(time.time())
        save_dev_contact_inbox()
        return True
    except Exception:
        return False


def _show_dev_contact_new_messages(owner_id: int):
    items = _dev_contact_new_items()
    if not items:
        emoji_ok = f'<tg-emoji emoji-id="{EMOJI_SENT_OK_ID}">✅</tg-emoji>'
        bot.send_message(owner_id, f"{emoji_ok} <i>Новых сообщений нет.</i>", parse_mode="HTML")
        return

    changed = False
    for item in items:
        try:
            bot.send_message(
                owner_id,
                _dev_contact_owner_new_text(item),
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_dev_contact_owner_item_kb(int(item.get("id") or 0)),
            )
            item["owner_notified_at"] = int(time.time())
            changed = True
            time.sleep(0.15)
        except Exception:
            continue

    if changed:
        save_dev_contact_inbox()


def _bot_link_title_html() -> str:
    try:
        me = bot.get_me()
        uname = (getattr(me, "username", "") or "").strip()
        title = (getattr(me, "first_name", "") or uname or "бот").strip()
        if uname:
            return f'<a href="https://t.me/{uname}">{_html.escape(title)}</a>'
        return _html.escape(title)
    except Exception:
        return "бот"


def _build_start_home_text(user: telebot.types.User) -> str:
    mention = mention_html(user)
    bot_title = _bot_link_title_html()

    lines = [
        f"<b>Привет, </b>{mention}<b>!</b>",
        "",
        f"Я {bot_title} бот для помощи в модерации группы. На данный момент меня может добавлять только мой разработчик.",
        "",
        "Если хотите сообщить о баге, предложить идею для бота или добавить бота в свою группу — свяжитесь с разработчиком, написав команду <code>Связь</code>.",
    ]

    if is_dev(user):
        emoji_verified = f'<tg-emoji emoji-id="{EMOJI_VERIFY_DEV_ID}">✅</tg-emoji>'
        emoji_version = f'<tg-emoji emoji-id="{EMOJI_BOT_VERSION_ID}">🏷️</tg-emoji>'
        lines.extend([
            "",
            f"{emoji_verified} Вы верифицированный разработчиком.",
            f"<i>{emoji_version} Версия бота <b>0.91.2</b></i>",
        ])

    return "\n".join(lines)


def _build_start_commands_text(user: telebot.types.User) -> str:
    is_owner_user = is_owner(user)
    is_dev_user = user.id in VERIFY_DEV

    legend_anywhere = f'<tg-emoji emoji-id="{EMOJI_LEGEND_ANYWHERE_ID}">🌐</tg-emoji>'
    legend_dev_only = f'<tg-emoji emoji-id="{EMOJI_LEGEND_DEV_ONLY_ID}">💻</tg-emoji>'
    legend_dev_or_verified = f'<tg-emoji emoji-id="{EMOJI_LEGEND_DEV_OR_VERIFIED_ID}">✅</tg-emoji>'
    legend_group_admin = f'<tg-emoji emoji-id="{EMOJI_LEGEND_GROUP_ADMIN_ID}">🛡️</tg-emoji>'
    legend_pm_only = f'<tg-emoji emoji-id="{EMOJI_LEGEND_PM_ONLY_ID}">💬</tg-emoji>'
    legend_group_only = f'<tg-emoji emoji-id="{EMOJI_LEGEND_GROUP_ONLY_ID}">👥</tg-emoji>'
    legend_all_users = f'<tg-emoji emoji-id="{EMOJI_LEGEND_ALL_USERS_ID}">👤</tg-emoji>'
    parts = [
        "<b>Пользовательские команды</b>",
        f"• /start ({legend_all_users} | {legend_pm_only})",
        f"• /ping, /пинг, ping, пинг ({legend_all_users} | {legend_anywhere})",
        f"• Профиль ({legend_all_users} | {legend_group_only})",
        f"• Награды ({legend_all_users} | {legend_group_only})",
        f"• Описание ({legend_all_users} | {legend_group_only})",
        f"• Статистика ({legend_all_users} | {legend_group_only})",
        f"• Связь / Связь с разработчиком ({legend_all_users} | {legend_pm_only})",
        "",
        "<b>Команды модерации</b>",
        f"• /settings ({legend_group_admin} | {legend_anywhere})",
        f"• /verify, /unverify, /vlist ({legend_group_admin} | {legend_group_only})",
        f"• /taglist, /теглист, теглист ({legend_group_admin} | {legend_group_only})",
        f"• /settag, /removetag, /taglist ({legend_group_admin} | {legend_group_only})",
        f"• Список тегов, Выдать тег, Снять тег ({legend_group_admin} | {legend_group_only})",
        f"• /staff, /ranks, /myrank, /promote, /demote ({legend_group_admin} | {legend_group_only})",
        f"• Повысить, Понизить, Админы ({legend_group_admin} | {legend_group_only})",
        f"• Наградить, Снять награду, Снять все награды ({legend_group_admin} | {legend_group_only})",
        f"• +Описание, -Описание ({legend_group_admin} | {legend_group_only})",
        f"• /closechat, /openchat, Закрыть чат, Открыть чат ({legend_group_admin} | {legend_group_only})",
        f"• /kick, Кик, Исключить ({legend_group_admin} | {legend_group_only})",
        f"• /pin, /spin, /npin, /unpin ({legend_group_admin} | {legend_group_only})",
        f"• Пин/Закреп/Закрепить, Анпин/Откреп/Открепить ({legend_group_admin} | {legend_group_only})",
        f"• /mute, /ban, /warn, /del ({legend_group_admin} | {legend_group_only})",
        f"• /delmute, /delban, /delwarn ({legend_group_admin} | {legend_group_only})",
        f"• /warnlist, /mutelist, /banlist ({legend_group_admin} | {legend_group_only})",
        f"• /варнлист, /мутлист, /банлист, варнлист, мутлист, банлист ({legend_group_admin} | {legend_group_only})",
        f"• /adminstats, /adminstat, /админстата ({legend_group_admin} | {legend_group_only})",
        f"• /unmute, /unban, /unwarn ({legend_group_admin} | {legend_group_only})",
        f"• Предупреждение, Ограничить, Заблокировать, Дел, Снять ограничение, Разблокировать, Снять предупреждение ({legend_group_admin} | {legend_group_only})",
    ]

    if is_dev_user:
        parts.extend([
            "",
            "<b>Команды dev-пользователей</b>",
            f"• /devverify, /devunverify, /devvlist ({legend_dev_or_verified} | {legend_pm_only})",
        ])

    if is_owner_user:
        parts.extend([
            "",
            "<b>Команды разработчика</b>",
            f"• /log, /broadcast ({legend_dev_only} | {legend_pm_only})",
            f"• Новые сообщения ({legend_dev_only} | {legend_pm_only})",
            f"• /testuser, /dbg_users, /dbg_global_users, /migrate_users_to_global ({legend_dev_only} | {legend_pm_only})",
        ])

    parts.extend([
        "",
        "<b>Легенда эмодзи</b>",
        f"• {legend_anywhere} — работает везде (в группе и в ЛС)",
        f"• {legend_dev_only} — только для разработчика",
        f"• {legend_dev_or_verified} — только для dev-пользователя или разработчика",
        f"• {legend_group_admin} — только для администратора группы",
        f"• {legend_pm_only} — работает только в ЛС",
        f"• {legend_group_only} — работает только в группе",
        f"• {legend_all_users} — работает для всех пользователей",
        "",
        "<i>Если у команды несколько ограничений, они показываются так: (эмодзи | эмодзи).</i>",
    ])

    return "\n".join(parts)


def _build_start_usage_text() -> str:
    return (
        "<b>Как пользоваться командами</b>\n\n"
        "1. Пишите команду с префиксом <code>/</code>, <code>.</code>, <code>,</code> или <code>!</code>.\n"
        "2. Для действий над участником используйте reply, @username, ссылку <code>t.me/...</code> или ID.\n"
        "3. Примеры наказаний: <code>/mute @username 1h причина</code>, <code>/mute @username причина</code>, <code>/ban @username 3д флуд</code>, <code>/ban @username</code>, <code>/warn @username причина</code>, <code>/kick @username причина</code>.\n"
        "4. Временные интервалы для наказаний:\n"
        "<blockquote expandable=\"true\"><b>Единицы времени:</b>\n"
        "RU: <code>м</code>/<code>мин</code>, <code>ч</code>, <code>д</code>, <code>н</code>, <code>мес</code>, <code>г</code>\n"
        "EN: <code>m</code>, <code>h</code>, <code>d</code>, <code>w</code>, <code>mou</code>, <code>y</code></blockquote>\n"
        "5. Команды модерации работают только в группе и требуют прав у вашей должности.\n"
        "6. Приветствия, правила, очистка, лимит предупреждений и права ролей настраиваются через <code>/settings</code>.\n"
        "7. Если команда не сработала, проверьте права должности и права бота в чате."
    )


def _build_start_about_text() -> str:
    return (
        "<b>О боте</b>\n\n"
        "Бот создан для помощи администраторам в группах.\n\n"
        "<b>Что умеет:</b>\n"
        "• Профили участников, описания, награды и теги.\n"
        "• Гибкие роли с правами по действиям.\n"
        "• Ограничения, блокировки, предупреждения, снятие наказаний и списки по каждому типу.\n"
        "• Закрытие/открытие чата и работа с закрепами.\n"
        "• Настраиваемые приветствия/сообщения о выходах/правила.\n"
        "• Очистка системных сообщений и команд.\n"
        "• Статистика сообщений по группам.\n\n"
        "<i>Нужна доработка под ваш формат сообщества: используйте раздел связи с разработчиком.</i>"
    )


def _broadcast_new_draft() -> dict:
    return {
        "id": random.randint(100000, 999999),
        "text_custom": "",
        "source": "plain",
        "entities": [],
        "media": [],
        "buttons": {"rows": [], "popups": []},
        "created_at": int(time.time()),
        "updated_at": int(time.time()),
    }


def _broadcast_get_or_create_draft(user_id: int) -> dict:
    draft = BROADCAST_DRAFTS.get(user_id)
    if not isinstance(draft, dict):
        draft = _broadcast_new_draft()
        BROADCAST_DRAFTS[user_id] = draft
    return draft


def _broadcast_render_panel_text(user_id: int) -> str:
    draft = _broadcast_get_or_create_draft(user_id)
    has_text = bool((draft.get("text_custom") or "").strip())
    has_media = bool(draft.get("media") or [])
    has_buttons = bool(((draft.get("buttons") or {}).get("rows") or []))

    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    emoji_x = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
    emoji_broadcast = f'<tg-emoji emoji-id="{EMOJI_LOG_PM_ID}">📢</tg-emoji>'

    text_flag = emoji_ok if has_text else emoji_x
    media_flag = emoji_ok if has_media else emoji_x
    btn_flag = emoji_ok if has_buttons else emoji_x

    return (
        f"{emoji_broadcast} <b>Рассылка</b>\n\n"
        "Настройте черновик и отправьте его всем пользователям из базы.\n"
        "<blockquote expandable=\"true\">"
        "Поддерживается: текст, медиа, кнопки (URL/Popup/Rules/Del).\n"
        "Формат кнопок: <code>Текст - ссылка</code> или <code>Текст - popup: сообщение</code>."
        "</blockquote>\n"
        f"<b>Текст:</b> {text_flag}\n"
        f"<b>Медиа:</b> {media_flag}\n"
        f"<b>Кнопки:</b> {btn_flag}"
    )


def _build_broadcast_panel_keyboard(draft_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)

    kb.row(
        InlineKeyboardButton("Текст", callback_data=f"bc2:text:{draft_id}", icon_custom_emoji_id=str(EMOJI_WELCOME_TEXT_ID)),
        InlineKeyboardButton("Медиа", callback_data=f"bc2:media:{draft_id}", icon_custom_emoji_id=str(EMOJI_WELCOME_MEDIA_ID)),
    )
    kb.row(InlineKeyboardButton("Кнопки", callback_data=f"bc2:buttons:{draft_id}", icon_custom_emoji_id=str(EMOJI_WELCOME_BUTTONS_ID)))
    kb.row(
        InlineKeyboardButton("Текущее сообщение", callback_data=f"bc2:preview:{draft_id}"),
        InlineKeyboardButton("Сбросить", callback_data=f"bc2:reset:{draft_id}", icon_custom_emoji_id=str(EMOJI_ROLE_SETTINGS_CANCEL_ID)),
    )
    kb.row(
        InlineKeyboardButton("Подтвердить отправку", callback_data=f"bc2:send:{draft_id}", icon_custom_emoji_id=str(EMOJI_ROLE_SETTINGS_SAVE_ID)),
        InlineKeyboardButton("Закрыть", callback_data=f"bc2:cancel:{draft_id}", icon_custom_emoji_id=str(PREMIUM_CLOSE_EMOJI_ID)),
    )
    return kb


def _broadcast_collect_targets() -> list[int]:
    targets: set[int] = set()

    for uid in STATS.get('users') or set():
        try:
            value = int(uid)
            if value > 0:
                targets.add(value)
        except Exception:
            continue

    for uid in (GLOBAL_USERS or {}).keys():
        try:
            value = int(uid)
            if value > 0:
                targets.add(value)
        except Exception:
            continue

    return sorted(targets)


def _broadcast_send_payload_once(user_id: int, html_text: str, media: list, buttons: dict) -> None:
    try:
        rows = (buttons or {}).get("rows") or []
        popups = (buttons or {}).get("popups") or []
        kb = build_inline_keyboard_for_payload("broadcast", user_id, rows, popups, user_id)
        _send_payload(user_id, html_text, media or [], reply_markup=kb)
    except Exception as send_err:
        plain = _html.unescape(_re.sub(r"<[^>]+>", "", html_text or ""))
        if plain.strip():
            bot.send_message(user_id, plain, disable_web_page_preview=True)
            return
        bot.send_message(user_id, "Рассылка", disable_web_page_preview=True)


def _broadcast_send_payload(user_id: int, html_text: str, media: list, buttons: dict) -> tuple[bool, str | None]:
    try:
        _broadcast_send_payload_once(user_id, html_text, media, buttons)
        return True, None
    except Exception as send_err:
        return False, str(send_err)


def _send_start_menu(chat_id: int, user: telebot.types.User):
    show_owner_button = is_owner(user)

    sent = bot.send_message(
        chat_id,
        _build_start_home_text(user),
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_markup=_build_start_home_keyboard(show_owner_button=show_owner_button),
    )

    START_MENU_STATE[(chat_id, sent.message_id)] = {
        "user_id": user.id,
        "show_owner_button": show_owner_button,
    }

@bot.message_handler(func=lambda m: match_command(m.text, 'start'))
def cmd_start(m: types.Message):
    add_stat_message(m)
    add_stat_command('start')
    _remember_owner_user_id(m.from_user)

    if m.chat.type != 'private':
        return

    _send_start_menu(m.chat.id, m.from_user)


@bot.message_handler(func=lambda m: m.chat.type == 'private' and (
    text_starts_with_ci(m.text, 'связь с разработчиком') or text_starts_with_ci(m.text, 'связь')
))
def cmd_contact_developer(m: types.Message):
    add_stat_message(m)
    if is_owner(m.from_user):
        _remember_owner_user_id(m.from_user)

    bot.send_message(
        m.chat.id,
        _dev_contact_intro_text(),
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_markup=_dev_contact_intro_kb(),
    )


@bot.message_handler(func=lambda m: m.chat.type == 'private' and text_starts_with_ci(m.text, 'новые сообщения'))
def cmd_dev_new_messages(m: types.Message):
    add_stat_message(m)

    if not is_owner(m.from_user):
        return deny_access(m.chat.id)

    _remember_owner_user_id(m.from_user)
    _show_dev_contact_new_messages(m.chat.id)


@bot.message_handler(func=lambda m: m.chat.type == 'private', content_types=['text'])
def handle_dev_contact_text_input(m: types.Message):
    if is_owner(m.from_user):
        _remember_owner_user_id(m.from_user)

    user_id = m.from_user.id
    text = (m.text or '').strip()

    pending_user = PENDING_DEV_CONTACT_FROM_USER.get(user_id)
    if pending_user is not None:
        if not text:
            bot.send_message(
                m.chat.id,
                premium_prefix("Сообщение не должно быть пустым. Отправьте текст."),
                parse_mode='HTML',
                disable_web_page_preview=True,
            )
            return

        PENDING_DEV_CONTACT_FROM_USER.pop(user_id, None)

        prompt_msg_id = int(pending_user.get('prompt_message_id') or 0)
        if prompt_msg_id > 0:
            try:
                bot.delete_message(m.chat.id, prompt_msg_id)
            except Exception:
                pass

        item = _append_dev_contact_item(m.from_user, text)
        _send_dev_contact_item_to_owner(item)

        emoji_ok = f'<tg-emoji emoji-id="{EMOJI_SENT_OK_ID}">✅</tg-emoji>'
        bot.send_message(
            m.chat.id,
            f"<i>{emoji_ok} Сообщение отправлено!</i>",
            parse_mode='HTML',
            disable_web_page_preview=True,
        )
        return

    pending_owner = PENDING_DEV_REPLY_FROM_OWNER.get(user_id)
    if pending_owner is not None and is_owner(m.from_user):
        if not text:
            bot.send_message(
                m.chat.id,
                premium_prefix("Ответ не должен быть пустым. Отправьте текст."),
                parse_mode='HTML',
                disable_web_page_preview=True,
            )
            return

        PENDING_DEV_REPLY_FROM_OWNER.pop(user_id, None)

        prompt_msg_id = int(pending_owner.get('prompt_message_id') or 0)
        if prompt_msg_id > 0:
            try:
                bot.delete_message(m.chat.id, prompt_msg_id)
            except Exception:
                pass

        item_id = int(pending_owner.get('item_id') or 0)
        target_user_id = int(pending_owner.get('target_user_id') or 0)
        item = _dev_contact_find_item(item_id)

        emoji_reply = f'<tg-emoji emoji-id="{EMOJI_REPLY_RECEIVED_ID}">💬</tg-emoji>'
        payload = (
            f"{emoji_reply} <b>Получен ответ от разработчика...</b>\n\n"
            "<b>Текст сообщения:</b>\n"
            f"<blockquote>{_html.escape(text)}</blockquote>"
        )

        sent_ok = False
        if target_user_id > 0:
            try:
                bot.send_message(
                    target_user_id,
                    payload,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                )
                sent_ok = True
            except Exception:
                sent_ok = False

        if item is not None:
            item['status'] = 'replied' if sent_ok else 'new'
            item['reply_text'] = text if sent_ok else ''
            item['replied_at'] = int(time.time()) if sent_ok else 0
            save_dev_contact_inbox()

        if sent_ok:
            emoji_ok = f'<tg-emoji emoji-id="{EMOJI_SENT_OK_ID}">✅</tg-emoji>'
            bot.send_message(m.chat.id, f"<i>{emoji_ok} Ответ отправлен.</i>", parse_mode='HTML')
        else:
            bot.send_message(m.chat.id, premium_prefix("Не удалось отправить ответ пользователю."), parse_mode='HTML')
        return

    return ContinueHandling()


@bot.message_handler(func=lambda m: m.chat.type == 'private', content_types=['photo', 'video', 'document', 'audio', 'animation', 'voice', 'video_note', 'sticker'])
def handle_dev_contact_non_text_input(m: types.Message):
    if is_owner(m.from_user):
        _remember_owner_user_id(m.from_user)

    user_id = m.from_user.id

    if user_id in PENDING_DEV_CONTACT_FROM_USER:
        bot.send_message(
            m.chat.id,
            premium_prefix("Нужно отправить именно текстовое сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True,
        )
        return

    if user_id in PENDING_DEV_REPLY_FROM_OWNER and is_owner(m.from_user):
        bot.send_message(
            m.chat.id,
            premium_prefix("Нужно отправить именно текстовое сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True,
        )
        return

    return ContinueHandling()


@bot.message_handler(func=lambda m: match_command_aliases(m.text, ['ping', 'пинг']))
def cmd_ping(m: types.Message):
    add_stat_message(m)
    add_stat_command('ping')

    # Проверка одобрения группы
    if m.chat.type in ['group', 'supergroup']:
        if not is_group_approved(m.chat.id):
            return bot.reply_to(
                m,
                "Бот находится на модерации. Ожидание подтверждения от разработчика.",
                parse_mode='HTML'
            )

    # Кулдаун: 30 сек для всех, кроме разработчика
    if not is_dev(m.from_user):
        wait_seconds = cooldown_hit('user', int(m.from_user.id), 'ping', 30)
        if wait_seconds > 0:
            return reply_cooldown_message(m, wait_seconds, scope='user', bucket=int(m.from_user.id), action='ping')

    t_start = time.perf_counter()
    sent = bot.send_message(
        m.chat.id,
        f'<tg-emoji emoji-id="{EMOJI_PING_ID}">🏓</tg-emoji> Проверка пинга…',
        parse_mode='HTML'
    )
    t_send_end = time.perf_counter()
    send_latency_ms = int((t_send_end - t_start) * 1000)

    t_edit_start = time.perf_counter()
    try:
        bot.edit_message_text(
            chat_id=sent.chat.id,
            message_id=sent.message_id,
            text=f'<tg-emoji emoji-id="{EMOJI_PING_ID}">🏓</tg-emoji> Проверка пинга…',
            parse_mode='HTML'
        )
    except Exception:
        pass
    t_edit_end = time.perf_counter()
    edit_latency_ms = int((t_edit_end - t_edit_start) * 1000)

    total_ms = send_latency_ms + edit_latency_ms
    emoji_ping = f'<tg-emoji emoji-id="{EMOJI_PING_ID}">🏓</tg-emoji>'

    if not is_dev(m.from_user):
        text = f"{emoji_ping} <b>Пинг:</b> <code>{total_ms} мс</code>"
    else:
        uptime_text = get_uptime_text()
        lines = [
            f"{emoji_ping} <b>Пинг</b>",
            f"<b>Отправка сообщения:</b> <code>{send_latency_ms} мс</code>",
            f"<b>Изменение сообщения:</b> <code>{edit_latency_ms} мс</code>",
            f"<b>Итого:</b> <code>{total_ms} мс</code>",
            f"<b>Uptime:</b> <code>{uptime_text}</code>",
        ]

        if is_owner(m.from_user):
            try:
                proc = psutil.Process(os.getpid())
                mem_text = format_bytes_mb(proc.memory_info().rss)
                vm = psutil.virtual_memory()
                ram_system_text = f"{int(vm.used / (1024 * 1024))}/{int(vm.total / (1024 * 1024))} MB"
            except Exception:
                mem_text = "не удалось узнать"
                ram_system_text = "не удалось узнать"

            lines.extend([
                f"<b>RAM процесса:</b> <code>{mem_text}</code>",
                f"<b>RAM системы:</b> <code>{ram_system_text}</code>",
            ])

        text = "\n".join(lines)

    bot.edit_message_text(
        chat_id=sent.chat.id,
        message_id=sent.message_id,
        text=text,
        parse_mode='HTML'
    )


@bot.message_handler(func=lambda m: match_command(m.text, 'log'))
def cmd_log(m: types.Message):
    add_stat_message(m)
    add_stat_command('log')

    if not is_owner(m.from_user):
        if m.chat.type == 'private':
            return deny_access(m.chat.id)
        return

    try:
        proc = psutil.Process(os.getpid())
        proc_ram = format_bytes_mb(proc.memory_info().rss)
        proc_cpu = proc.cpu_percent(interval=0.1)
        proc_threads = proc.num_threads()
        system_cpu = psutil.cpu_percent(interval=0.1)
        vm = psutil.virtual_memory()
        system_ram = f"{int(vm.used / (1024 * 1024))}/{int(vm.total / (1024 * 1024))} MB"
    except Exception:
        proc_ram = "n/a"
        proc_cpu = -1
        proc_threads = -1
        system_cpu = -1
        system_ram = "n/a"

    sorted_cmds = sorted((STATS.get('commands_used') or {}).items(), key=lambda x: x[1], reverse=True)
    top_lines = [f"/{cmd}: <code>{cnt}</code>" for cmd, cnt in sorted_cmds[:20]]
    if not top_lines:
        top_lines = ["Нет данных."]

    now_dt = datetime.now()
    started_dt = datetime.fromtimestamp(float(STATS.get('start_time') or time.time()))

    # Считаем только группы/супергруппы (id < 0) из известных БД бота.
    known_group_ids: set[int] = set()
    for store in (CHAT_SETTINGS, GROUP_SETTINGS, GROUP_STATS, MODERATION, PENDING_GROUPS):
        for cid_raw in (store or {}).keys():
            try:
                cid = int(str(cid_raw))
            except Exception:
                continue
            if cid < 0:
                known_group_ids.add(cid)

    users_in_db = len(GLOBAL_USERS or {})

    text = (
        f'<tg-emoji emoji-id="{EMOJI_LOG_ID}">📋</tg-emoji> <b>Лог бота</b>\n'
        f"<i>{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</i>\n\n"
        "<b>Основное:</b>\n"
        f"• Время работы: <code>{get_uptime_text()}</code>\n"
        f"• Нынешнее время: <code>{now_dt.strftime('%d.%m.%Y %H:%M:%S')}</code>\n"
        f"• Время запуска: <code>{started_dt.strftime('%d.%m.%Y %H:%M:%S')}</code>\n"
        f"• Пользователей в БД: <code>{users_in_db}</code>\n"
        f"• Групп где есть бот: <code>{len(known_group_ids)}</code>\n"
        f"• Уникальных пользователей (runtime): <code>{len(STATS.get('users') or set())}</code>\n"
        f"• Чатов (runtime): <code>{len(STATS.get('chats') or set())}</code>\n"
        f"• Сообщений: <code>{STATS.get('messages', 0)}</code>\n\n"
        "<b>VPS:</b>\n"
        f"• CPU процесса: <code>{proc_cpu:.1f}%</code>\n"
        f"• CPU системы: <code>{system_cpu:.1f}%</code>\n"
        f"• RAM процесса: <code>{proc_ram}</code>\n"
        f"• RAM системы: <code>{system_ram}</code>\n"
        f"• Потоков процесса: <code>{proc_threads}</code>\n\n"
        "<b>Топ команд:</b>\n"
        + "\n".join(top_lines)
    )

    try:
        bot.send_message(m.from_user.id, text, parse_mode='HTML', disable_web_page_preview=True)
        if m.chat.id != m.from_user.id:
            bot.reply_to(m, "Лог отправлен тебе в личку.")
    except Exception as e:
        bot.reply_to(m, premium_prefix(f"Не удалось отправить лог в ЛС: <code>{e}</code>"))


@bot.message_handler(func=lambda m: match_command(m.text, 'broadcast'))
def cmd_broadcast(m: types.Message):
    add_stat_message(m)
    add_stat_command('broadcast')

    if not is_owner(m.from_user):
        if m.chat.type == 'private':
            return deny_access(m.chat.id)
        return
    if m.chat.type != 'private':
        return

    pending = BROADCAST_PENDING_INPUT.pop(m.from_user.id, None)
    if pending:
        try:
            old_prompt_id = int(pending.get("prompt_message_id") or 0)
            if old_prompt_id > 0:
                bot.delete_message(m.chat.id, old_prompt_id)
        except Exception:
            pass

    draft = _broadcast_get_or_create_draft(m.from_user.id)

    parts = (m.text or "").split(' ', 1)
    if len(parts) > 1 and parts[1].strip():
        text_custom, source, entities_ser = convert_section_text_from_message(m)
        draft["text_custom"] = text_custom
        draft["source"] = source
        draft["entities"] = entities_ser
        draft["updated_at"] = int(time.time())
        BROADCAST_DRAFTS[m.from_user.id] = draft

    panel_text = _broadcast_render_panel_text(m.from_user.id)
    kb = _build_broadcast_panel_keyboard(int(draft.get("id") or 0))
    bot.send_message(
        m.chat.id,
        panel_text,
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_markup=kb,
    )

# ==== ПРОФИЛИ / НАГРАДЫ ====

def get_user_msg_count_in_chat(chat: types.Chat, user_id: int) -> int:
    chat_id = str(chat.id)
    chat_stats = GROUP_STATS.get(chat_id, {})
    u = chat_stats.get(str(user_id), {})
    return int(u.get("count", 0))


def get_user_photo_file_id(user_id: int) -> str | None:
    try:
        # Текущее "главное" фото пользователя.
        chat_obj = bot.get_chat(user_id)
        chat_photo = getattr(chat_obj, "photo", None)
        big_file_id = getattr(chat_photo, "big_file_id", None) if chat_photo else None
        if big_file_id:
            return big_file_id
    except Exception:
        pass

    try:
        # Fallback: первая фотография из списка доступных profile photos.
        photos = bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count == 0:
            return None
        sizes = photos.photos[0]
        largest = max(sizes, key=lambda s: s.file_size or 0)
        return largest.file_id
    except Exception as e:
        print(f"Не удалось получить фото профиля {user_id}: {e}")
        return None


def _get_user_profile_gallery_photo_file_id(user_id: int) -> str | None:
    try:
        photos = bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count == 0:
            return None
        sizes = photos.photos[0]
        largest = max(sizes, key=lambda s: s.file_size or 0)
        return largest.file_id
    except Exception:
        return None


def _mod_has_active_punishment(chat_id: int, user_id: int, kind: str) -> bool:
    _mod_cleanup_expired(chat_id)
    ch = _mod_get_chat(chat_id)
    active_map = (ch.get("active") or {}).get(kind) or {}
    return str(user_id) in active_map


def build_profile_text(chat: types.Chat, target: telebot.types.User) -> str:
    uid = target.id
    profile = get_profile(chat.id, uid)
    msg_count = get_user_msg_count_in_chat(chat, uid)

    name = link_for_user(chat.id, uid)

    text = (
        f'<tg-emoji emoji-id="{EMOJI_PROFILE_ID}">👤</tg-emoji> '
        f"<b>Профиль пользователя</b> {name} [<code>{uid}</code>]\n\n"
    )

    text += (
        f'<tg-emoji emoji-id="{EMOJI_MSG_COUNT_ID}">💬</tg-emoji> '
        f"<b>Количество сообщений:</b> <code>{msg_count}</code>\n\n"
    )

    # === Статус пользователя ===
    status_lines: list[str] = []
    punish_emoji = f'<tg-emoji emoji-id="{EMOJI_PUNISHMENT_ID}">⚠️</tg-emoji>'
    member_status = None

    if is_owner(target):
        status_lines.append(
            f'<tg-emoji emoji-id="{EMOJI_DEV_ID}">💻</tg-emoji> Разработчик бота'
        )

    if chat.type in ['group', 'supergroup']:
        try:
            member = bot.get_chat_member(chat.id, uid)
            member_status = member.status
            if member_status in ("administrator", "creator"):
                status_lines.append(
                    f'<tg-emoji emoji-id="{EMOJI_ADMIN_ID}">⭐</tg-emoji> Администратор чата'
                )
            elif member_status in ("member", "restricted"):
                status_lines.append(
                    f'<tg-emoji emoji-id="{EMOJI_MEMBER_ID}">⭐</tg-emoji> Участник чата'
                )
        except Exception:
            pass

        if _mod_has_active_punishment(chat.id, uid, "mute"):
            status_lines.append(f"{punish_emoji} Ограничен")
        if _mod_has_active_punishment(chat.id, uid, "ban"):
            status_lines.append(f"{punish_emoji} Заблокирован")
        elif member_status in ("left", "kicked"):
            status_lines.append(f"{punish_emoji} Не участник группы")

    if not status_lines:
        status_lines.append("Неизвестен")

    # Премиум
    is_premium = getattr(target, "is_premium", False)
    if is_premium:
        status_lines.append(
            f'<tg-emoji emoji-id="{EMOJI_PREMIUM_STATUS_ID}">⭐</tg-emoji> Премиум пользователь'
        )

    # Верификация разработчиком (глобальная)
    if uid in VERIFY_DEV:
        status_lines.append(
            f'<tg-emoji emoji-id="{EMOJI_VERIFY_DEV_ID}">✅</tg-emoji> Верифицирован разработчиком'
        )

    # Верификация админом (локальная для этого чата)
    chat_id_s = str(chat.id)
    if uid in (VERIFY_ADMINS.get(chat_id_s) or []):
        status_lines.append(
            f'<tg-emoji emoji-id="{EMOJI_VERIFY_ADMIN_ID}">✅</tg-emoji> Верифицирован администратором'
        )

    text += "<b>Статус пользователя:</b>\n" + "\n".join(status_lines) + "\n\n"

    rank = get_user_rank(chat.id, uid)
    rank_html = get_rank_label_html(rank)
    if rank_html:
        text += f"<b>Должность:</b>\n{rank_html}\n\n"

    user_tag = get_user_custom_tag(chat.id, uid)
    if user_tag:
        text += (
            f'<tg-emoji emoji-id="{EMOJI_USER_ROLE_TEXT_ID}">🏷</tg-emoji> '
            f"<b>Тег:</b> <code>{_html.escape(user_tag)}</code>\n\n"
        )

    return text


def build_profile_awards_text(chat_id: int, target_id: int) -> str:
    profile = get_profile(chat_id, target_id)
    awards = profile["awards"]
    name = link_for_user(chat_id, target_id)

    lines = [
        f'<tg-emoji emoji-id="{EMOJI_AWARDS_BLOCK_ID}">🏅</tg-emoji> '
        f"<b>Награды пользователя</b> {name} [<code>{target_id}</code>]",
    ]

    if not awards:
        lines.extend([
            "",
            '<blockquote expandable="true">Наград нет.</blockquote>',
        ])
    else:
        quote_lines: list[str] = []
        for idx, a in enumerate(awards[:10], start=1):
            quote_lines.append(f"<b>[{idx}] Награда</b>")
            quote_lines.append(f"{a}")
            if idx < len(awards[:10]):
                quote_lines.append("")
        quote_body = "\n".join(quote_lines)
        lines.extend([
            "",
            f'<blockquote expandable="true">{quote_body}</blockquote>',
        ])

    return "\n".join(lines)


def build_profile_description_text(chat_id: int, target_id: int) -> str:
    profile = get_profile(chat_id, target_id)
    name = link_for_user(chat_id, target_id)
    desc = profile.get("description") or "Описание не задано."

    return (
        f'<tg-emoji emoji-id="{EMOJI_DESC_ID}">📝</tg-emoji> '
        f"<b>Описание пользователя</b> {name} [<code>{target_id}</code>]\n\n"
        f'<blockquote expandable="true">{_html.escape(desc)}</blockquote>'
    )


def build_profile_keyboard(chat_id: int, target_id: int, viewer_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton(
            "Награды",
            callback_data=f"profile:awards:{chat_id}:{target_id}:{viewer_id}",
            icon_custom_emoji_id=str(EMOJI_AWARDS_BLOCK_ID),
        ),
        InlineKeyboardButton(
            "Описание",
            callback_data=f"profile:description:{chat_id}:{target_id}:{viewer_id}",
            icon_custom_emoji_id=str(EMOJI_DESC_ID),
        ),
    )
    kb.row(
        InlineKeyboardButton(
            "Закрыть",
            callback_data=f"profile:close:{chat_id}:{target_id}:{viewer_id}",
            icon_custom_emoji_id=str(PREMIUM_CLOSE_EMOJI_ID),
        )
    )
    return kb

@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "профиль"))
def cmd_profile(m: types.Message):
    add_stat_message(m)
    add_stat_command('profile')

    wait_seconds = cooldown_hit('user', int(m.from_user.id), 'profile', 5)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='user', bucket=int(m.from_user.id), action='profile')

    if m.chat.type == 'private':
        return bot.reply_to(m, premium_prefix("Эта команда работает только в группах."))

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    # если есть reply + что-то дописано рядом с командой — игнорируем
    if m.reply_to_message and args:
        return

    # если есть reply → берём пользователя из reply
    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
    else:
        # нет аргументов → профиль самого отправителя
        if not args:
            target_id = m.from_user.id
        else:
            first = args[0]
            # аргумент должен быть id / @ / t.me
            if first.isdigit() or first.startswith("@") or first.startswith("t.me/") or "t.me/" in first:
                target_id = parse_target_user(m, args)
                if target_id is None:
                    return bot.reply_to(
                        m,
                        premium_prefix(
                            "Не удалось определить пользователя для профиля.\n"
                            "Если используется @username, убедись, что пользователь уже писал в этот чат или боту в ЛС."
                        )
                    )
            else:
                # после команды текст, но не указатель — молчим
                return

    try:
        member = bot.get_chat_member(m.chat.id, target_id)
        target_user = member.user
    except Exception:
        try:
            target_user = bot.get_chat(target_id)
        except Exception:
            return bot.reply_to(m, premium_prefix("Не удалось получить информацию о пользователе."))

    caption = build_profile_text(m.chat, target_user)
    photo_id = get_user_photo_file_id(target_user.id)
    kb = build_profile_keyboard(m.chat.id, target_user.id, m.from_user.id)

    if photo_id:
        try:
            bot.send_photo(
                m.chat.id,
                photo_id,
                caption=caption,
                parse_mode='HTML',
                reply_to_message_id=m.message_id,
                reply_markup=kb,
            )
            return
        except Exception:
            fallback_photo_id = _get_user_profile_gallery_photo_file_id(target_user.id)
            if fallback_photo_id and fallback_photo_id != photo_id:
                try:
                    bot.send_photo(
                        m.chat.id,
                        fallback_photo_id,
                        caption=caption,
                        parse_mode='HTML',
                        reply_to_message_id=m.message_id,
                        reply_markup=kb,
                    )
                    return
                except Exception:
                    pass

    bot.send_message(
        m.chat.id,
        caption,
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_to_message_id=m.message_id,
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("profile:"))
def cb_profile_view(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    try:
        _, action, chat_s, target_s, viewer_s = c.data.split(":", 4)
        chat_id = int(chat_s)
        target_id = int(target_s)
        viewer_id = int(viewer_s)
    except Exception:
        return bot.answer_callback_query(c.id)

    if c.from_user.id != viewer_id:
        return bot.answer_callback_query(c.id, "Эти кнопки доступны только тому, кто вызвал профиль.", show_alert=True)

    if action == "close":
        try:
            bot.delete_message(c.message.chat.id, c.message.message_id)
        except Exception:
            pass
        return bot.answer_callback_query(c.id)

    if action == "awards":
        text = build_profile_awards_text(chat_id, target_id)
    elif action == "description":
        text = build_profile_description_text(chat_id, target_id)
    else:
        return bot.answer_callback_query(c.id)

    try:
        kb = build_profile_keyboard(chat_id, target_id, viewer_id)
        if (c.message.content_type or "") == "photo":
            bot.edit_message_caption(
                caption=text,
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                parse_mode='HTML',
                reply_markup=kb,
            )
        else:
            bot.edit_message_text(
                text,
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_markup=kb,
            )
    except Exception:
        pass

    return bot.answer_callback_query(c.id)

def ensure_user_in_users(m: types.Message):
    chat_id = str(m.chat.id)
    u = m.from_user
    if chat_id not in USERS:
        USERS[chat_id] = {}
    chat_users = USERS[chat_id]
    uid = str(u.id)
    chat_users[uid] = {
        "id": u.id,
        "username": u.username,
        "first_name": u.first_name,
        "last_name": u.last_name,
        "is_bot": u.is_bot,
    }
    save_users()
    
def find_user_id_by_username_in_chat(chat_id: int, username: str) -> int | None:
    chat_id_s = str(chat_id)
    chat_users = USERS.get(chat_id_s) or {}
    uname = username.lstrip("@").lower()
    for uid, data in chat_users.items():
        un = (data.get("username") or "").lower()
        if un == uname:
            return int(uid)
    return None

# ==== НАГРАДЫ ==== 

def _to_utf16_units(s: str):
    b = s.encode('utf-16-le')
    return [int.from_bytes(b[i:i+2], 'little') for i in range(0, len(b), 2)]


def _from_utf16_units(units):
    b = b''.join((u.to_bytes(2, 'little') for u in units))
    return b.decode('utf-16-le')


def build_award_html_from_message(message: types.Message, award_text_plain: str) -> str:
    text_full = message.text or ""
    entities = message.entities or []

    try:
        start_py = text_full.index(award_text_plain)
    except ValueError:
        return award_text_plain

    end_py = start_py + len(award_text_plain)

    full_units = _to_utf16_units(text_full)
    prefix_units = _to_utf16_units(text_full[:start_py])
    award_units = _to_utf16_units(award_text_plain)
    start_u16 = len(prefix_units)
    end_u16 = start_u16 + len(award_units)

    ce_entities = []
    for e in entities:
        if getattr(e, "type", None) != "custom_emoji":
            continue
        off = e.offset
        length = e.length
        seg_start = off
        seg_end = off + length
        if seg_end <= start_u16 or seg_start >= end_u16:
            continue
        ce_entities.append({
            "offset": seg_start,
            "length": length,
            "custom_emoji_id": getattr(e, "custom_emoji_id", None),
        })

    result_units = []
    cur = start_u16
    ce_entities.sort(key=lambda x: x["offset"])

    for ent in ce_entities:
        e_off = ent["offset"]
        e_len = ent["length"]
        e_id = ent["custom_emoji_id"]

        if e_off > cur:
            result_units.extend(full_units[cur:e_off])

        emoji_units = full_units[e_off:e_off + e_len]
        emoji_char = _from_utf16_units(emoji_units)

        emoji_html = f'<tg-emoji emoji-id="{e_id}">{emoji_char}</tg-emoji>'
        result_units.extend(_to_utf16_units(emoji_html))

        cur = e_off + e_len

    if cur < end_u16:
        result_units.extend(full_units[cur:end_u16])

    award_html = _from_utf16_units(result_units).strip()
    return award_html or award_text_plain


def _is_target_owner(target_id: int) -> bool:
    try:
        user = bot.get_chat(target_id)
        return (user.username or "").lower() == OWNER_USERNAME.lower()
    except Exception:
        return False


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "наградить"))
def cmd_award(m: types.Message):
    add_stat_message(m)
    add_stat_command('award')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_MANAGE_AWARDS)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права управлять наградами."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        award_text = raw_after.strip()
    else:
        if not args:
            return bot.reply_to(
                m,
                premium_prefix("Укажи пользователя и название награды."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

        first = args[0]
        if (first.startswith('@') or first.startswith("t.me/") or "t.me/" in first
                or (first.isdigit() and len(first) >= 4)):
            target_id = parse_target_user(m, [first])
            if target_id is None or not isinstance(target_id, int) or target_id < 1:
                return bot.reply_to(
                    m,
                    premium_prefix("Не удалось определить пользователя."),
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            award_text = " ".join(args[1:]).strip()
        else:
            return bot.reply_to(
                m,
                premium_prefix("Укажи пользователя корректно."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

    if _is_target_owner(target_id) and not is_owner(m.from_user):
        return bot.reply_to(
            m,
            premium_prefix("Профиль разработчика нельзя изменять."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    award_text = award_text.replace("\n", " ").strip()
    if not award_text:
        return bot.reply_to(
            m,
            premium_prefix("Укажи название награды."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if len(award_text) > 50:
        return bot.reply_to(
            m,
            premium_prefix("Текст награды не должен превышать 50 символов."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    profile = get_profile(m.chat.id, target_id)
    awards = profile["awards"]
    if len(awards) >= 10:
        return bot.reply_to(
            m,
            premium_prefix("У пользователя уже максимум наград."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    award_html = build_award_html_from_message(m, award_text)
    awards.append(award_html)
    save_profiles()

    award_idx = len(awards)

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_AWARDS_BLOCK_ID}">🏅</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>получил новую награду [{award_idx}].</b>"
    )

    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "снять награду"))
def cmd_remove_award(m: types.Message):
    add_stat_message(m)
    add_stat_command('remove_award')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_MANAGE_AWARDS)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права управлять наградами."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    text_full = m.text or ""
    raw_after = text_full[len("Снять награду"):].strip()
    args = raw_after.split() if raw_after else []

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        after_target = raw_after.strip()
    else:
        if not args:
            return bot.reply_to(
                m,
                premium_prefix("Укажи пользователя и номер награды."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

        first = args[0]
        if first.isdigit() and len(first) < 4:
            return bot.reply_to(
                m,
                premium_prefix("Не удалось определить пользователя."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

        if first.startswith("@") or first.startswith("t.me/") or "t.me/" in first or (first.isdigit() and len(first) >= 4):
            target_id = parse_target_user(m, [first])
            if target_id is None or not isinstance(target_id, int) or target_id < 1:
                return bot.reply_to(
                    m,
                    premium_prefix("Не удалось определить пользователя."),
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            after_target = " ".join(args[1:]).strip()
        else:
            return bot.reply_to(
                m,
                premium_prefix("Укажи пользователя корректно."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

    if not after_target:
        return bot.reply_to(
            m,
            premium_prefix("Укажи номер награды."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    try:
        idx = int(after_target)
    except ValueError:
        return bot.reply_to(
            m,
            premium_prefix("Номер награды должен быть числом."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    profile = get_profile(m.chat.id, target_id)
    awards = profile.get("awards") or []

    if idx < 1 or idx > len(awards):
        return bot.reply_to(
            m,
            premium_prefix("Награда с таким номером не найдена."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    awards.pop(idx - 1)
    save_profiles()

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_AWARDS_BLOCK_ID}">🏅</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>снята награда [{idx}].</b>"
    )

    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "снять все награды"))
def cmd_remove_all_awards(m: types.Message):
    add_stat_message(m)
    add_stat_command('remove_all_awards')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_MANAGE_AWARDS)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права управлять наградами."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    text_full = m.text or ""
    raw_after = text_full[len("Снять все награды"):].strip()
    args = raw_after.split() if raw_after else []

    if m.reply_to_message or args:
        target_id = parse_target_user(m, args)
    else:
        return bot.reply_to(
            m,
            premium_prefix("Укажи пользователя, у которого нужно снять все награды."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if target_id is None or not isinstance(target_id, int) or target_id < 1:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось определить пользователя."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if _is_target_owner(target_id) and not is_owner(m.from_user):
        return bot.reply_to(
            m,
            premium_prefix("Профиль разработчика нельзя изменять."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    profile = get_profile(m.chat.id, target_id)
    if not profile["awards"]:
        return bot.reply_to(
            m,
            premium_prefix("У пользователя нет наград."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    profile["awards"].clear()
    save_profiles()

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_AWARDS_BLOCK_ID}">🏅</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>все награды были сняты.</b>"
    )

    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )

# ==== ТЕГИ TELEGRAM ==== 

MAX_USER_TAG_LEN = 32


def _resolve_tag_target_and_text(m: types.Message, require_text: bool) -> tuple[int | None, str]:
    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        tag_text = raw_after.strip()
        return target_id, tag_text

    if not args:
        return None, ""

    first = args[0]
    target_id = parse_target_user(m, [first])
    if not isinstance(target_id, int) or target_id < 1:
        return None, ""

    tag_text = " ".join(args[1:]).strip() if len(args) > 1 else ""
    if require_text:
        return target_id, tag_text
    return target_id, ""


def _normalize_tag_text(tag_text: str) -> str:
    text = (tag_text or "").strip()
    if "|" in text:
        left, right = text.split("|", 1)
        text = (right.strip() or left.strip())
    return text


@bot.message_handler(commands=['settag'])
def cmd_settag(m: types.Message):
    add_stat_message(m)
    add_stat_command('settag')

    if m.chat.type not in ['group', 'supergroup']:
        return reply_error(m, "group_only")

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_SET_ROLE_TEXT)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(m, premium_prefix("У вашей должности нет права управлять тегами."), parse_mode='HTML', disable_web_page_preview=True)
        return

    target_id, raw_tag = _resolve_tag_target_and_text(m, require_text=True)
    if not target_id:
        return bot.reply_to(m, premium_prefix("Укажи пользователя и текст тега."), parse_mode='HTML', disable_web_page_preview=True)

    tag_text = _normalize_tag_text(raw_tag)
    if not tag_text:
        return bot.reply_to(m, premium_prefix("Укажи текст тега."), parse_mode='HTML', disable_web_page_preview=True)
    if len(tag_text) > MAX_USER_TAG_LEN:
        return bot.reply_to(m, premium_prefix(f"Тег не должен превышать {MAX_USER_TAG_LEN} символов."), parse_mode='HTML', disable_web_page_preview=True)

    ok_tag, err_tag = raw_set_chat_member_tag(m.chat.id, target_id, tag_text)

    name = link_for_user(m.chat.id, target_id)
    warn = ""
    if not ok_tag:
        warn = f"\n<i>Telegram-тег не применён через API: {_html.escape(err_tag or 'неизвестная ошибка')}.</i>"
    text = (
        f'<tg-emoji emoji-id="{EMOJI_USER_ROLE_TEXT_ID}">🏷</tg-emoji> '
        f"{name} [<code>{target_id}</code>] <b>получил тег:</b> <code>{_html.escape(tag_text)}</code>{warn}"
    )
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(commands=['removetag'])
def cmd_removetag(m: types.Message):
    add_stat_message(m)
    add_stat_command('removetag')

    if m.chat.type not in ['group', 'supergroup']:
        return reply_error(m, "group_only")

    status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_SET_ROLE_TEXT)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(m, premium_prefix("У вашей должности нет права управлять тегами."), parse_mode='HTML', disable_web_page_preview=True)
        return

    target_id, _ = _resolve_tag_target_and_text(m, require_text=False)
    if not target_id:
        return bot.reply_to(m, premium_prefix("Укажи пользователя, у которого нужно снять тег."), parse_mode='HTML', disable_web_page_preview=True)

    old_tag = get_user_custom_tag(m.chat.id, target_id)
    if not old_tag:
        return bot.reply_to(m, premium_prefix("У пользователя нет тега."), parse_mode='HTML', disable_web_page_preview=True)

    ok_tag, err_tag = raw_set_chat_member_tag(m.chat.id, target_id, "")

    name = link_for_user(m.chat.id, target_id)
    warn = ""
    if not ok_tag:
        warn = f"\n<i>Telegram-тег не снят через API: {_html.escape(err_tag or 'неизвестная ошибка')}.</i>"
    text = (
        f'<tg-emoji emoji-id="{EMOJI_USER_ROLE_TEXT_ID}">🏷</tg-emoji> '
        f"{name} [<code>{target_id}</code>] <b>тег снят.</b>{warn}"
    )
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(commands=['taglist'])
def cmd_taglist(m: types.Message):
    add_stat_message(m)
    add_stat_command('taglist')

    wait_seconds = cooldown_hit('chat', int(m.chat.id), 'taglist', 15)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='taglist')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if not (is_owner(m.from_user) or is_chat_admin(m.chat.id, m.from_user.id) or is_dev(m.from_user)):
        return bot.reply_to(
            m,
            premium_prefix("Только администраторы чата могут смотреть список тегов."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    candidate_ids: set[int] = set()
    for uid_s in (USERS.get(str(m.chat.id)) or {}).keys():
        try:
            candidate_ids.add(int(uid_s))
        except Exception:
            continue

    try:
        for adm in bot.get_chat_administrators(m.chat.id):
            if adm and getattr(adm, "user", None):
                candidate_ids.add(adm.user.id)
    except Exception:
        pass

    items: list[tuple[int, str]] = []
    for uid in sorted(candidate_ids):
        tag = get_user_custom_tag(m.chat.id, uid)
        if tag:
            items.append((uid, tag))

    if not items:
        return bot.reply_to(
            m,
            premium_prefix("В этом чате нет пользователей с тегами Telegram."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    lines = []
    for uid_int, user_tag in items[:50]:
        name = link_for_user(m.chat.id, uid_int)
        lines.append(f"• {name} [<code>{uid_int}</code>] — <code>{_html.escape(user_tag)}</code>")

    text = (
        f'<tg-emoji emoji-id="{EMOJI_USER_ROLE_TEXT_ID}">🏷</tg-emoji> '
        f"<b>Telegram-теги пользователей в этом чате:</b>\n" +
        "\n".join(lines)
    )

    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


def _is_ru_taglist_alias(m: types.Message) -> bool:
    prefix, cmd, rest = _extract_command_info(m)
    if cmd != "теглист":
        return False
    # По требованию: если к команде что-то дописано/передано аргументом — игнорируем.
    if rest:
        return False
    if prefix in COMMAND_PREFIXES:
        return True
    return (m.text or "").strip().lower() == "теглист"


@bot.message_handler(func=lambda m: _is_ru_taglist_alias(m))
def cmd_taglist_ru_alias(m: types.Message):
    fake = m
    fake.text = "/taglist"
    cmd_taglist(fake)


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "список тегов"))
def cmd_taglist_text(m: types.Message):
    fake = m
    fake.text = "/taglist"
    cmd_taglist(fake)


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "выдать тег"))
def cmd_settag_text(m: types.Message):
    parts = m.text.split(maxsplit=2)
    rest = parts[2] if len(parts) >= 3 else ""
    fake = m
    fake.text = f"/settag {rest}".strip()
    cmd_settag(fake)


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "снять тег"))
def cmd_removetag_text(m: types.Message):
    parts = m.text.split(maxsplit=2)
    rest = parts[2] if len(parts) >= 3 else ""
    fake = m
    fake.text = f"/removetag {rest}".strip()
    cmd_removetag(fake)


# ==== ДОЛЖНОСТИ: ПОВЫСИТЬ / ПОНИЗИТЬ ====

def _change_rank(chat_id: int, actor: types.User, target_id: int, delta: int) -> str:
    """
    Внутренний хелпер: изменить ранг target_id на +1 или -1
    с учётом ограничений.
    Возвращает текст ошибки (str) или пустую строку, если всё ок.
    """
    fake_target = types.User(target_id, False, first_name="", last_name=None, username=None)
    actor_rank = get_user_rank(chat_id, actor.id)
    target_rank = get_user_rank(chat_id, target_id)

    is_actor_owner_bot = is_owner(actor)
    is_target_owner_bot = is_owner(fake_target)

    # 1) разработчика бота трогать нельзя
    if is_target_owner_bot:
        return "Нельзя изменять должность разработчика бота."

    # 2) нельзя выдавать должность разработчику
    if is_target_owner_bot:
        return "Нельзя выдавать должность разработчику бота."

    # 3) определяем настоящего владельца чата
    is_real_owner_chat_actor = False
    is_real_owner_chat_target = False
    try:
        m_actor = bot.get_chat_member(chat_id, actor.id)
        if m_actor.status == 'creator':
            is_real_owner_chat_actor = True
    except Exception:
        pass
    try:
        m_target = bot.get_chat_member(chat_id, target_id)
        if m_target.status == 'creator':
            is_real_owner_chat_target = True
    except Exception:
        pass

    if is_real_owner_chat_target:
        return "Вы не можете менять должность настоящего владельца чата."

    # 4) иерархия: нельзя трогать равных и выше себя (кроме владельца бота)
    if not is_actor_owner_bot:
        if actor_rank <= target_rank:
            return "Вы не можете изменить должность этого пользователя (его должность не ниже вашей)."

    # 5) доп. проверки по текущему рангу цели
    if delta > 0:
        # повышаем
        if target_rank >= 5:
            return "У этого пользователя уже максимальная должность."
    else:
        # понижаем
        if target_rank <= 0:
            return "У этого пользователя нет должности, которую можно понизить."

    # 6) считаем новый ранг
    new_rank = target_rank + delta

    if is_real_owner_chat_actor:
        # настоящий владелец чата: может довести до 5, ниже 0 не опускаем
        if new_rank > 5:
            new_rank = 5
        if new_rank < 0:
            new_rank = 0
    else:
        # НЕ настоящий владелец чата: максимум 4
        if new_rank > 4:
            return "Вы не можете назначить должность выше ранга Главного админа."
        if new_rank < 0:
            new_rank = 0
        # нельзя назначать ранг выше собственного, кроме владельца бота
        if (not is_actor_owner_bot) and new_rank > actor_rank:
            return "Вы не можете назначить должность выше своей собственной."

    set_user_rank(chat_id, target_id, new_rank)
    return ""


def _resolve_target_for_rank(m: types.Message) -> int | None:
    """
    Общий парсер цели для повышения/понижения: reply / @username / ID / t.me.
    """
    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    # reply приоритетнее
    if m.reply_to_message:
        return m.reply_to_message.from_user.id

    if not args:
        return None

    first = args[0]
    if (first.startswith("@")
            or first.startswith("t.me/")
            or "t.me/" in first
            or (first.isdigit() and len(first) >= 4)):
        return parse_target_user(m, [first])

    return None


@bot.message_handler(commands=['promote'])
def cmd_promote(m: types.Message):
    add_stat_message(m)
    add_stat_command('promote')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    # ПРАВА: должность должна иметь PERM_PROMOTE
    if not has_role_perm(m.chat.id, m.from_user.id, PERM_PROMOTE):
        return bot.reply_to(
            m,
            premium_prefix("У вашей должности нет права повышать пользователей."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    target_id = _resolve_target_for_rank(m)
    if not isinstance(target_id, int):
        return bot.reply_to(
            m,
            premium_prefix("Укажи пользователя (ответом, @username, ID или ссылкой)."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    err = _change_rank(m.chat.id, m.from_user, target_id, +1)
    if err:
        return bot.reply_to(
            m,
            premium_prefix(err),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    new_rank = get_user_rank(m.chat.id, target_id)
    rank_html = get_rank_label_html(new_rank) or "без должности"
    rank_instr = get_rank_label_instrumental(new_rank)
    name = link_for_user(m.chat.id, target_id)
    if rank_instr:
        text = (
            f"{name} [<code>{target_id}</code>] <b>назначен</b> {rank_instr}.\n"
            f"{rank_html}"
        )
    else:
        text = (
            f"{name} [<code>{target_id}</code>] теперь имеет должность:\n"
            f"{rank_html}"
        )
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(commands=['demote'])
def cmd_demote(m: types.Message):
    add_stat_message(m)
    add_stat_command('demote')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    # ПРАВА: должность должна иметь PERM_DEMOTE
    if not has_role_perm(m.chat.id, m.from_user.id, PERM_DEMOTE):
        return bot.reply_to(
            m,
            premium_prefix("У вашей должности нет права понижать пользователей."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    target_id = _resolve_target_for_rank(m)
    if not isinstance(target_id, int):
        return bot.reply_to(
            m,
            premium_prefix("Укажи пользователя (ответом, @username, ID или ссылкой)."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    err = _change_rank(m.chat.id, m.from_user, target_id, -1)
    if err:
        return bot.reply_to(
            m,
            premium_prefix(err),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    new_rank = get_user_rank(m.chat.id, target_id)
    name = link_for_user(m.chat.id, target_id)

    if new_rank <= 0:
        text = (
            f"{name} [<code>{target_id}</code>] больше не имеет никакой должности."
        )
    else:
        rank_html = get_rank_label_html(new_rank) or "без должности"
        rank_instr = get_rank_label_instrumental(new_rank)
        if rank_instr:
            text = (
                f"{name} [<code>{target_id}</code>] <b>назначен</b> {rank_instr}.\n"
                f"{rank_html}"
            )
        else:
            text = (
                f"{name} [<code>{target_id}</code>] теперь имеет должность:\n"
                f"{rank_html}"
            )

    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "повысить"))
def cmd_promote_text(m: types.Message):
    # "Повысить @user" или reply + "Повысить"
    parts = m.text.split(maxsplit=1)
    rest = parts[1] if len(parts) > 1 else ""
    fake = m
    fake.text = f"/promote {rest}".strip()
    cmd_promote(fake)


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "понизить"))
def cmd_demote_text(m: types.Message):
    parts = m.text.split(maxsplit=1)
    rest = parts[1] if len(parts) > 1 else ""
    fake = m
    fake.text = f"/demote {rest}".strip()
    cmd_demote(fake)

@bot.message_handler(commands=['staff', 'ranks'])
def cmd_staff(m: types.Message):
    add_stat_message(m)
    add_stat_command('staff')

    wait_seconds = cooldown_hit('chat', int(m.chat.id), 'staff', 10)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='staff')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    cid = str(m.chat.id)
    chat_users = USERS.get(cid) or {}

    # {rank: [user_id, ...]}
    by_rank: dict[int, list[int]] = {}

    for uid_str in chat_users.keys():
        try:
            uid_int = int(uid_str)
        except ValueError:
            continue

        rank = get_user_rank(m.chat.id, uid_int)

        # ранг 999 — разработчик бота
        if rank >= 999:
            by_rank.setdefault(999, []).append(uid_int)

            # если разработчик одновременно настоящий владелец чата — добавим его и как 5
            try:
                member = bot.get_chat_member(m.chat.id, uid_int)
                if member.status == 'creator':
                    by_rank.setdefault(5, []).append(uid_int)
            except Exception:
                pass

            continue  # основной ранг уже обработали

        # обычные должности
        if rank <= 0:
            continue

        by_rank.setdefault(rank, []).append(uid_int)

    if not by_rank:
        return bot.reply_to(
            m,
            premium_prefix("В этом чате нет пользователей с должностями."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    priority_order = [999, 5, 4, 3, 2, 1]
    ranks_sorted = [r for r in priority_order if r in by_rank]

    lines = ["<b>Должности в этом чате:</b>"]
    first_block = True

    for rank in ranks_sorted:
        users = sorted(set(by_rank[rank]))
        if not users:
            continue

        header = get_rank_label_html(rank)
        if not header:
            continue

        if not first_block:
            lines.append("")
        first_block = False

        lines.append(header)

        for uid_int in users:
            name = link_for_user(m.chat.id, uid_int)
            lines.append(f"• {name} [<code>{uid_int}</code>]")

    text = "\n".join(lines)
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


@bot.message_handler(func=lambda m: (m.text or "").strip().lower() == "админы")
def cmd_staff_text(m: types.Message):
    # просто прокинуть в /staff
    fake = m
    fake.text = "/staff"
    cmd_staff(fake)

@bot.message_handler(commands=['myrank'])
def cmd_myrank(m: types.Message):
    wait_seconds = cooldown_hit('user', int(m.from_user.id), 'myrank', 10)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='user', bucket=int(m.from_user.id), action='myrank')

    r = get_user_rank(m.chat.id, m.from_user.id)
    bot.reply_to(
        m,
        f"Ваш ID: {m.from_user.id}\nusername: @{m.from_user.username}\nrank: {r}",
        parse_mode='HTML'
    )

# ==============================
# DESCRIPTION – описание профиля
# ==============================

@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "+описание"))
def cmd_set_description(m: types.Message):
    add_stat_message(m)
    add_stat_command("setdesc")

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    # определяем target и текст
    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        desc_text = raw_after.strip()
    else:
        if not args:
            return bot.reply_to(
                m,
                premium_prefix("Укажи пользователя и текст описания."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

        first = args[0]
        if (first.startswith("@") or first.startswith("t.me/") or "t.me/" in first
                or (first.isdigit() and len(first) >= 4)):
            target_id = parse_target_user(m, [first])
            if not isinstance(target_id, int) or target_id < 1:
                return bot.reply_to(
                    m,
                    premium_prefix("Не удалось определить пользователя."),
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            desc_text = " ".join(args[1:]).strip()
        else:
            target_id = m.from_user.id
            desc_text = raw_after.strip()

    if target_id != m.from_user.id and _is_target_owner(target_id) and not is_owner(m.from_user):
        return bot.reply_to(
            m,
            premium_prefix("Профиль разработчика нельзя изменять."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if target_id != m.from_user.id:
        status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_SET_DESC_OTHERS)
        if not allowed:
            if status == 'no_perm':
                return bot.reply_to(
                    m,
                    premium_prefix("У вашей должности нет права менять описания другим пользователям."),
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            return bot.reply_to(
                m,
                premium_prefix("Вы не можете менять описания другим пользователям."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

    if not desc_text:
        return bot.reply_to(
            m,
            premium_prefix("Укажи текст описания."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if len(desc_text) > 200:
        return bot.reply_to(
            m,
            premium_prefix("Описание не должно превышать 200 символов."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    profile = get_profile(m.chat.id, target_id)
    profile["description"] = desc_text
    save_profiles()

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_DESC_ID}">📝</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>описание добавлено:</b>\n"
        f"{desc_text}"
    )
    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "-описание"))
def cmd_clear_description(m: types.Message):
    add_stat_message(m)
    add_stat_command("cleardesc")

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
    else:
        if args:
            first = args[0]
            if (first.startswith("@") or first.startswith("t.me/") or "t.me/" in first
                    or (first.isdigit() and len(first) >= 4)):
                target_id = parse_target_user(m, [first])
                if not isinstance(target_id, int) or target_id < 1:
                    return bot.reply_to(
                        m,
                        premium_prefix("Не удалось определить пользователя."),
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
            else:
                target_id = m.from_user.id
        else:
            target_id = m.from_user.id

    if target_id != m.from_user.id and _is_target_owner(target_id) and not is_owner(m.from_user):
        return bot.reply_to(
            m,
            premium_prefix("Профиль разработчика нельзя изменять."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if target_id != m.from_user.id:
        status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_SET_DESC_OTHERS)
        if not allowed:
            if status == 'no_perm':
                return bot.reply_to(
                    m,
                    premium_prefix("У вашей должности нет права менять описания другим пользователям."),
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            return bot.reply_to(
                m,
                premium_prefix("Вы не можете менять описания другим пользователям."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

    profile = get_profile(m.chat.id, target_id)
    if not profile.get("description"):
        return bot.reply_to(
            m,
            premium_prefix("У этого пользователя нет описания."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    profile["description"] = ""
    save_profiles()

    name = link_for_user(m.chat.id, target_id)
    text = (
        f'<tg-emoji emoji-id="{EMOJI_DESC_ID}">📝</tg-emoji> '
        f"{name} [<code>{target_id}</code>] "
        f"<b>описание очищено.</b>"
    )
    bot.reply_to(
        m,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "награды"))
def cmd_show_awards(m: types.Message):
    add_stat_message(m)
    add_stat_command('awards')

    wait_seconds = cooldown_hit('user', int(m.from_user.id), 'awards', 5)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='user', bucket=int(m.from_user.id), action='awards')

    if m.chat.type == 'private':
        return bot.reply_to(
            m,
            premium_prefix("Эта команда работает только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
    else:
        if not args:
            target_id = m.from_user.id
        else:
            first = args[0]
            if first.isdigit() or first.startswith("@") or first.startswith("t.me/") or "t.me/" in first:
                target_id = parse_target_user(m, args)
                if target_id is None:
                    return bot.reply_to(
                        m,
                        premium_prefix(
                            "Не удалось определить пользователя для просмотра наград.\n"
                            "Если используется @username, убедись, что пользователь уже писал в этот чат или боту в ЛС."
                        ),
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
            else:
                return  # текст, но не указатель — молчим

    text = build_profile_awards_text(m.chat.id, target_id)

    bot.reply_to(
        m,
        text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )


@bot.message_handler(func=lambda m: m.text and text_starts_with_ci(m.text, "описание") and not text_starts_with_ci(m.text, "+описание") and not text_starts_with_ci(m.text, "-описание"))
def cmd_show_description(m: types.Message):
    add_stat_message(m)
    add_stat_command('showdesc')

    wait_seconds = cooldown_hit('user', int(m.from_user.id), 'showdesc', 5)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='user', bucket=int(m.from_user.id), action='showdesc')

    if m.chat.type == 'private':
        return bot.reply_to(
            m,
            premium_prefix("Эта команда работает только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1] if len(parts) > 1 else ""
    args = raw_after.split() if raw_after else []

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
    else:
        if not args:
            target_id = m.from_user.id
        else:
            first = args[0]
            if first.isdigit() or first.startswith("@") or first.startswith("t.me/") or "t.me/" in first:
                target_id = parse_target_user(m, args)
                if target_id is None:
                    return bot.reply_to(
                        m,
                        premium_prefix(
                            "Не удалось определить пользователя для просмотра описания.\n"
                            "Если используется @username, убедись, что пользователь уже писал в этот чат или боту в ЛС."
                        ),
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
            else:
                return

    text = build_profile_description_text(m.chat.id, target_id)

    bot.reply_to(
        m,
        text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )


# ==== КОМАНДЫ ЗАКРЫТИЯ/ОТКРЫТИЯ ЧАТА (ПРОСТЫЕ) ====


def user_is_real_admin(chatid: int, userid: int) -> bool:
    try:
        member = bot.get_chat_member(chatid, userid)
        return member.status in ('administrator', 'creator')
    except Exception:
        return False


def format_closechat_duration_text(seconds: int) -> str:
    if seconds <= 0:
        return ""

    units = [
        ('г',   365 * 24 * 60 * 60),
        ('мес',  30 * 24 * 60 * 60),
        ('н',    7 * 24 * 60 * 60),
        ('д',    24 * 60 * 60),
        ('ч',    60 * 60),
        ('м',    60),
    ]
    for suffix, sec_in_unit in units:
        if seconds % sec_in_unit == 0 and seconds >= sec_in_unit:
            amount = seconds // sec_in_unit
            return f"{amount}{suffix}"

    minutes = max(1, seconds // 60)
    return f"{minutes}м"


def send_closechat_message(chatid: int, actorid: int, durationseconds: int | None, withbutton: bool) -> int | None:
    rank = get_user_rank(chatid, actorid)
    role_plain = get_rank_label_plain(rank)
    name_html = link_for_user(chatid, actorid)
    emoji_closed = f'<tg-emoji emoji-id="{EMOJI_CHAT_CLOSED_ID}">🔒</tg-emoji>'

    if durationseconds and durationseconds > 0:
        until_ts = int(time.time() + int(durationseconds))
        text = (
            f"{emoji_closed} <b>{role_plain}</b> {name_html} <b>закрыл чат.</b>\n"
            f"<b>Истекает:</b> {_fmt_time(until_ts)}"
        )
    else:
        text = f"{emoji_closed} <b>{role_plain}</b> {name_html} <b>закрыл чат.</b>"

    kb = None
    if withbutton:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(
            "Открыть чат",
            callback_data=f"openchatbtn:{chatid}",
            icon_custom_emoji_id=EMOJI_CHAT_OPEN_BTN_ID,
        ))

    try:
        msg = bot.send_message(
            chatid,
            text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
        return msg.message_id
    except Exception as e:
        print(f"[CLOSECHAT] send_message failed: {e}")
        return None


@bot.message_handler(commands=['closechat'])
def cmd_closechat(m: types.Message):
    add_stat_message(m)
    add_stat_command('closechat')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    parts = m.text.split(maxsplit=1)
    raw_after = parts[1].strip() if len(parts) > 1 else ""
    durationseconds = None
    if raw_after:
        durationseconds = parse_closechat_duration(raw_after, is_russian=False)
        if durationseconds is None:
            return bot.reply_to(
                m,
                premium_prefix("Неверный формат времени для /closechat или превышен лимит (не более 1 дня)."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )

    actor = m.from_user
    chatid = m.chat.id

    if m.reply_to_message:
        return

    status, allowed = check_role_permission(chatid, actor.id, PERM_CLOSE_CHAT)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права закрывать чат."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    if not user_is_real_admin(chatid, actor.id):
        return bot.reply_to(
            m,
            premium_prefix("Без префикса нельзя закрыть чат."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    closed_perms = build_closed_permissions()
    ok = setchatdefaultpermissions(chatid, closed_perms)
    if not ok:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось изменить права чата."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    untilts = 0
    if durationseconds:
        untilts = time.time() + durationseconds

    setclosechatstate(chatid, closed=True, until_ts=untilts)

    send_closechat_message(chatid, actor.id, durationseconds, withbutton=True)

    if durationseconds:
        schedulereopenchat(chatid, durationseconds)


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "закрыть чат"))
def cmd_closechat_text(m: types.Message):
    add_stat_message(m)
    add_stat_command('closechat_text')

    if m.chat.type not in ['group', 'supergroup']:
        return

    if m.reply_to_message:
        return

    text_full = m.text.strip()
    lower = text_full.lower()
    prefix = "закрыть чат"
    if not lower.startswith(prefix):
        return

    rest = text_full[len(prefix):].strip()
    durationseconds = None

    if rest:
        parts = rest.split()
        if len(parts) != 1:
            return
        token = parts[0]
        durationseconds = parse_closechat_duration(token, is_russian=True)
        if durationseconds is None:
            return

    actor = m.from_user
    chatid = m.chat.id

    status, allowed = check_role_permission(chatid, actor.id, PERM_CLOSE_CHAT)
    if not allowed:
        if status == 'no_perm':
            bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права закрывать чат."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    if not user_is_real_admin(chatid, actor.id):
        return bot.reply_to(
            m,
            premium_prefix("Без префикса нельзя закрыть чат."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    closed_perms = build_closed_permissions()
    ok = setchatdefaultpermissions(chatid, closed_perms)
    if not ok:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось изменить права чата."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    untilts = 0
    if durationseconds:
        untilts = time.time() + durationseconds

    setclosechatstate(chatid, closed=True, until_ts=untilts)

    send_closechat_message(chatid, actor.id, durationseconds, withbutton=True)

    if durationseconds:
        schedulereopenchat(chatid, durationseconds)


@bot.message_handler(commands=['openchat'])
def cmd_openchat(m: types.Message):
    add_stat_message(m)
    add_stat_command('openchat')

    if m.chat.type not in ['group', 'supergroup']:
        return bot.reply_to(
            m,
            premium_prefix("Команда доступна только в группах."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    if m.reply_to_message:
        return

    actor = m.from_user
    chatid = m.chat.id

    status, allowed = check_role_permission(chatid, actor.id, PERM_OPEN_CHAT)
    if not allowed:
        if status == 'no_perm':
            return bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права открывать чат."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    state = getclosechatstate(chatid)
    if not state or not state.get("closed"):
        return bot.reply_to(
            m,
            premium_prefix("Чат уже открыт."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    open_perms = build_open_permissions()
    ok = setchatdefaultpermissions(chatid, open_perms)
    if not ok:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось изменить права чата."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    setclosechatstate(chatid, closed=False, until_ts=0)

    emoji_open = f'<tg-emoji emoji-id="{EMOJI_CHAT_OPEN_BTN_ID}">🔓</tg-emoji>'
    rank = get_user_rank(chatid, actor.id)
    role_plain = get_rank_label_plain(rank)
    name = link_for_user(chatid, actor.id)
    text = f"{emoji_open} <b>{role_plain}</b> {name} <b>открыл чат.</b>"

    bot.send_message(
        chatid,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.message_handler(func=lambda m: text_starts_with_ci(m.text, "открыть чат"))
def cmd_openchat_text(m: types.Message):
    add_stat_message(m)
    add_stat_command('openchat_text')

    if m.chat.type not in ['group', 'supergroup']:
        return

    if m.reply_to_message:
        return

    actor = m.from_user
    chatid = m.chat.id

    status, allowed = check_role_permission(chatid, actor.id, PERM_OPEN_CHAT)
    if not allowed:
        if status == 'no_perm':
            bot.reply_to(
                m,
                premium_prefix("У вашей должности нет права открывать чат."),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    state = getclosechatstate(chatid)
    if not state or not state.get("closed"):
        return

    open_perms = build_open_permissions()
    ok = setchatdefaultpermissions(chatid, open_perms)
    if not ok:
        return

    setclosechatstate(chatid, closed=False, until_ts=0)

    emoji_open = f'<tg-emoji emoji-id="{EMOJI_CHAT_OPEN_BTN_ID}">🔓</tg-emoji>'
    rank = get_user_rank(chatid, actor.id)
    role_plain = get_rank_label_plain(rank)
    name = link_for_user(chatid, actor.id)
    text = f"{emoji_open} <b>{role_plain}</b> {name} <b>открыл чат.</b>"

    bot.send_message(
        chatid,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("openchatbtn:"))
def cb_openchat_button(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    try:
        _, chatid_s = c.data.split(":", 1)
        chatid = int(chatid_s)
    except Exception:
        return bot.answer_callback_query(c.id)

    user = c.from_user

    rank = get_user_rank(chatid, user.id)
    if rank <= 0 and not _is_special_actor(chatid, user):
        return bot.answer_callback_query(c.id)

    status, allowed = check_role_permission(chatid, user.id, PERM_OPEN_CHAT)
    if not allowed:
        if status == 'no_perm':
            return bot.answer_callback_query(
                c.id,
                text="У вашей должности недостаточно прав, чтобы открыть чат.",
                show_alert=True
            )
        return bot.answer_callback_query(c.id)

    state = getclosechatstate(chatid)
    if not state or not state.get("closed"):
        return bot.answer_callback_query(
            c.id,
            text="Чат уже открыт.",
            show_alert=True
        )

    open_perms = build_open_permissions()
    ok = setchatdefaultpermissions(chatid, open_perms)
    if not ok:
        return bot.answer_callback_query(
            c.id,
            text="Не удалось изменить права чата.",
            show_alert=True
        )

    setclosechatstate(chatid, closed=False, until_ts=0)

    try:
        bot.delete_message(chatid, c.message.message_id)
    except Exception:
        pass

    emoji_open = f'<tg-emoji emoji-id="{EMOJI_CHAT_OPEN_BTN_ID}">🔓</tg-emoji>'
    role_plain = get_rank_label_plain(rank)
    name = link_for_user(chatid, user.id)
    text = f"{emoji_open} <b>{role_plain}</b> {name} <b>открыл чат.</b>"

    try:
        bot.send_message(chatid, text, parse_mode='HTML', disable_web_page_preview=True)
    except Exception:
        pass

    bot.answer_callback_query(c.id, text="Чат открыт.", show_alert=False)


# ==== ПРОВЕРКА, МОЖНО ЛИ КИКНУТЬ ЦЕЛЬ ====

def _can_kick_target(chatid: int, actor: types.User, target_id: int) -> tuple[bool, str | None]:
    """
    Проверяем, можно ли кикнуть target_id:
    - нельзя трогать спец-актеров (owner/dev/глобальная верификация и т.п.);
    - нельзя трогать админов/владельца (статус администратора/создателя);
    - нельзя трогать глобальных dev / локально верифицированных;
    - нельзя трогать тех, у кого ранг >= ранга кикающего;
    - нельзя трогать себя.
    """
    if target_id == actor.id:
        return False, "Нельзя кикнуть самого себя."

    # спец-актеры (твоя логика is_special_actor)
    try:
        dummy_user = types.User(id=target_id, is_bot=False, first_name=".", last_name=None, username=None)
        if _is_special_actor(chatid, dummy_user):
            return False, "Нельзя кикнуть пользователя с особым статусом."
    except Exception:
        pass

    # админ/владелец чата
    try:
        member = bot.get_chat_member(chatid, target_id)
        if member.status in ("administrator", "creator"):
            return False, "Нельзя кикнуть пользователя с префиксом."
    except Exception:
        pass

    # глобальные разработчики
    if target_id in VERIFY_DEV:
        return False, "Нельзя кикнуть dev-пользователя."
    
    # по рангам: нельзя кикнуть такой же или более высокий ранг
    actor_rank = get_user_rank(chatid, actor.id)
    target_rank = get_user_rank(chatid, target_id)
    if target_rank >= actor_rank > 0:
        return False, "Нельзя кикнуть пользователя с должностью."

    return True, None


# ==== ЛОГИКА КИКА + РАЗБАН ====

def _kick_with_unban(chatid: int, actor: types.User, target_id: int, reason: str | None) -> str | None:
    """
    Кик + моментальный разбан.
    Возвращает текст ошибки (для premium_prefix) или None, если всё ок.
    Для ранга 0 возвращает понятную ошибку доступа.
    """
    # проверка прав по должности
    status, allowed = check_role_permission(chatid, actor.id, PERM_KICK)
    if not allowed:
        if status == 'no_rank':
            return "Для использования кика назначьте себе должность с этим правом в /settings."
        if status == 'no_perm':
            # есть должность (1–5), но нет права
            return "У вашей должности нет права использовать кик."
        # прочие случаи (теоретически)
        return "Вы не можете использовать кик."

    # нельзя кикнуть недопустимую цель
    ok, err = _can_kick_target(chatid, actor, target_id)
    if not ok:
        return err

    # пробуем кикнуть (бан + разбан)
    try:
        if hasattr(bot, "ban_chat_member"):
            bot.ban_chat_member(chatid, target_id)
        else:
            bot.kick_chat_member(chatid, target_id)
    except ApiTelegramException as e:
        msg = str(e)
        if ("not enough rights" in msg or
                "not sufficient rights" in msg or
                "can_restrict_members" in msg):
            return "У бота нет прав для кика. Дайте ему право «Блокировка пользователей»."
        return f"Не удалось кикнуть пользователя: {e}"
    except Exception as e:
        return f"Не удалось кикнуть пользователя: {e}"

    # моментальный разбан
    try:
        bot.unban_chat_member(chatid, target_id, only_if_banned=True)
    except Exception:
        pass

    tg_invalidate_chat_member_caches(chatid, target_id)
    _mark_farewell_suppressed(chatid, target_id)

    # уведомление
    actor_name = link_for_user(chatid, actor.id)
    target_name = link_for_user(chatid, target_id)

    emoji_kick = f'<tg-emoji emoji-id="{EMOJI_BTN_KICK_ID}">👢</tg-emoji>'
    emoji_reason = f'<tg-emoji emoji-id="{EMOJI_REASON_ID}">📝</tg-emoji>'

    lines: list[str] = []
    lines.append(f"{emoji_kick} <b>Пользователь</b> {target_name} <b>наказан.</b>")
    lines.append("<b>Наказание:</b> Исключение")
    if reason:
        reason = reason.strip()
        if reason:
            lines.append(f"{emoji_reason} <b>Причина:</b> {_html.escape(reason)}")
    lines.append(f"<b>Администратор:</b> {actor_name}")

    text = "\n".join(lines)

    try:
        bot.send_message(chatid, text, parse_mode='HTML', disable_web_page_preview=True)
    except Exception:
        pass

    return None


# ==== МОДЕРАЦИЯ: MUTE / BAN / WARN / LISTS / DEL ==== 

MIN_PUNISH_SECONDS = 60
MAX_PUNISH_SECONDS = 365 * 24 * 60 * 60

MOD_ERR = {
    "no_perm_mute": "У вашей должности нет права выдавать ограничения.",
    "no_perm_ban": "У вашей должности нет права выдавать блокировки.",
    "no_perm_warn": "У вашей должности нет права использовать предупреждения.",
    "no_perm_del": TEXTS["no_perm_del"],
    "bot_no_del": TEXTS["bot_no_del"],
    "bot_no_mute": "У бота нет прав для ограничения. Дайте право «Блокировка пользователей».",
    "bot_no_ban": "У бота нет прав для блокировки. Дайте право «Блокировка пользователей».",
    "user_not_found": "Не удалось определить пользователя. Укажи @username/ID/ссылку или используй ответ на сообщение.",
    "bad_duration": "Время наказания должно быть от 1 минуты до 365 дней. Можно комбинировать до 3 интервалов: 1h 2m, 2mou 1d.",
    "no_rights": "Недостаточно прав для использования команды.",
    "no_rights_delete": "Недостаточно прав для удаления сообщений.",
}

# Временное подавление farewell для пользователей, удалённых модерацией (/ban, /kick).
# Ключ: (chat_id, user_id), значение: unix_ts до которого farewell игнорируется.
_FAREWELL_SUPPRESS: dict[tuple[int, int], float] = {}
FAREWELL_SUPPRESS_SECONDS = 120


def _mark_farewell_suppressed(chat_id: int, user_id: int, seconds: int = FAREWELL_SUPPRESS_SECONDS):
    _FAREWELL_SUPPRESS[(int(chat_id), int(user_id))] = time.time() + max(1, int(seconds))


def _is_farewell_suppressed(chat_id: int, user_id: int) -> bool:
    key = (int(chat_id), int(user_id))
    until_ts = float(_FAREWELL_SUPPRESS.get(key, 0) or 0)
    if until_ts <= 0:
        return False
    if until_ts < time.time():
        _FAREWELL_SUPPRESS.pop(key, None)
        return False
    return True


def _mod_get_chat(chat_id: int) -> dict:
    cid = str(chat_id)
    ch = MODERATION.get(cid)
    if ch is None:
        ch = {
            "settings": {
                "warn_enabled": True,
                "warn_limit": 3,
                "warn_punish": {
                    "type": "mute",
                    "duration": 24 * 60 * 60,
                    "reason": "",
                },
            },
            "active": {"mute": {}, "ban": {}},
            "warns": {},
            "logs": {"mute": [], "ban": [], "warn": [], "kick": []},
        }
        MODERATION[cid] = ch

    settings = ch.get("settings") or {}
    warn_enabled = settings.get("warn_enabled")
    if not isinstance(warn_enabled, bool):
        warn_enabled = True
    settings["warn_enabled"] = warn_enabled

    warn_limit = settings.get("warn_limit")
    try:
        warn_limit = int(warn_limit)
    except Exception:
        warn_limit = 3
    settings["warn_limit"] = max(2, min(10, warn_limit))

    wp = settings.get("warn_punish") or {}
    wp_type = (wp.get("type") or "mute").lower()
    if wp_type not in ("mute", "ban", "kick"):
        wp_type = "mute"

    wp_duration = wp.get("duration")
    if wp_type in ("mute", "ban"):
        if wp_duration is None:
            wp_duration = 24 * 60 * 60
        else:
            try:
                wp_duration = int(wp_duration)
            except Exception:
                wp_duration = 24 * 60 * 60
            if wp_duration != 0:
                wp_duration = max(MIN_PUNISH_SECONDS, min(MAX_PUNISH_SECONDS, wp_duration))
    else:
        wp_duration = None

    settings["warn_punish"] = {
        "type": wp_type,
        "duration": wp_duration,
        "reason": str(wp.get("reason") or ""),
    }
    ch["settings"] = settings

    if not isinstance(ch.get("active"), dict):
        ch["active"] = {}
    ch["active"].setdefault("mute", {})
    ch["active"].setdefault("ban", {})

    if not isinstance(ch.get("warns"), dict):
        ch["warns"] = {}

    if not isinstance(ch.get("logs"), dict):
        ch["logs"] = {}
    ch["logs"].setdefault("mute", [])
    ch["logs"].setdefault("ban", [])
    ch["logs"].setdefault("warn", [])
    ch["logs"].setdefault("kick", [])

    MODERATION[cid] = ch
    return ch


def _mod_save():
    save_moderation()


def _mod_new_action_id() -> str:
    return f"{int(time.time() * 1000)}{random.randint(100, 999)}"


def _mod_fmt_ts(ts: float | int | None) -> str:
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def _fmt_time(unix_ts: int | float | None, fmt: str = "wDT") -> str:
    """HTML-тег <tg-time> — Telegram покажет локальное время пользователя."""
    if not unix_ts:
        return "—"
    ts = int(unix_ts)
    if fmt:
        return f'<tg-time unix="{ts}" format="{fmt}">...</tg-time>'
    return f'<tg-time unix="{ts}">...</tg-time>'


def _ru_plural(n: int, forms: tuple[str, str, str]) -> str:
    n_abs = abs(int(n))
    if n_abs % 10 == 1 and n_abs % 100 != 11:
        return forms[0]
    if n_abs % 10 in (2, 3, 4) and n_abs % 100 not in (12, 13, 14):
        return forms[1]
    return forms[2]


def _format_mod_duration_human(seconds: int) -> str:
    if seconds <= 0:
        return "навсегда"

    remaining = int(seconds)
    units: list[tuple[int, tuple[str, str, str]]] = [
        (365 * 24 * 60 * 60, ("год", "года", "лет")),
        (30 * 24 * 60 * 60, ("месяц", "месяца", "месяцев")),
        (7 * 24 * 60 * 60, ("неделя", "недели", "недель")),
        (24 * 60 * 60, ("день", "дня", "дней")),
        (60 * 60, ("час", "часа", "часов")),
        (60, ("минута", "минуты", "минут")),
    ]

    parts: list[str] = []
    for sec_in_unit, forms in units:
        if remaining >= sec_in_unit:
            amount = remaining // sec_in_unit
            remaining %= sec_in_unit
            parts.append(f"{amount} {_ru_plural(amount, forms)}")

    if not parts:
        return "1 минута"
    return " ".join(parts)


def _mod_duration_text(seconds: int | None) -> str:
    if not seconds or seconds <= 0:
        return "навсегда"
    return _format_mod_duration_human(int(seconds))


def _parse_punish_duration(value: str, is_russian: bool) -> int | None:
    if not value:
        return None
    value = value.strip().lower().strip('.,;:!?)(')

    if value in ("forever", "навсегда"):
        return 0

    num_part = ''
    unit_part = ''
    for ch in value:
        if ch.isdigit():
            if unit_part:
                return None
            num_part += ch
        else:
            unit_part += ch

    if not num_part or not unit_part:
        return None

    try:
        amount = int(num_part)
    except ValueError:
        return None

    if amount <= 0:
        return None

    if not is_russian:
        if unit_part == 'm':
            seconds = amount * 60
        elif unit_part == 'h':
            seconds = amount * 60 * 60
        elif unit_part == 'd':
            seconds = amount * 24 * 60 * 60
        elif unit_part == 'w':
            seconds = amount * 7 * 24 * 60 * 60
        elif unit_part == 'mou':
            seconds = amount * 30 * 24 * 60 * 60
        elif unit_part == 'y':
            seconds = amount * 365 * 24 * 60 * 60
        else:
            return None
    else:
        if unit_part in ('м', 'мин'):
            seconds = amount * 60
        elif unit_part == 'ч':
            seconds = amount * 60 * 60
        elif unit_part == 'д':
            seconds = amount * 24 * 60 * 60
        elif unit_part == 'н':
            seconds = amount * 7 * 24 * 60 * 60
        elif unit_part == 'мес':
            seconds = amount * 30 * 24 * 60 * 60
        elif unit_part == 'г':
            seconds = amount * 365 * 24 * 60 * 60
        else:
            return None

    if seconds == 0:
        return 0

    if seconds < MIN_PUNISH_SECONDS or seconds > MAX_PUNISH_SECONDS:
        return None
    return int(seconds)


def _parse_duration_token_parts(token: str, allow_russian_duration: bool) -> list[int] | None:
    token_norm = (token or "").strip().lower().strip('.,;:!?)(')
    if not token_norm:
        return None

    unit_table: list[tuple[str, int]] = [
        ("mou", 30 * 24 * 60 * 60),
        ("y", 365 * 24 * 60 * 60),
        ("w", 7 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
    ]
    if allow_russian_duration:
        unit_table += [
            ("мес", 30 * 24 * 60 * 60),
            ("мин", 60),
            ("г", 365 * 24 * 60 * 60),
            ("н", 7 * 24 * 60 * 60),
            ("д", 24 * 60 * 60),
            ("ч", 60 * 60),
            ("м", 60),
        ]

    out: list[int] = []
    i = 0
    n = len(token_norm)
    while i < n:
        j = i
        while j < n and token_norm[j].isdigit():
            j += 1
        if j == i:
            return None

        try:
            amount = int(token_norm[i:j])
        except Exception:
            return None
        if amount <= 0:
            return None

        matched_unit: tuple[str, int] | None = None
        for unit, mult in unit_table:
            if token_norm.startswith(unit, j):
                matched_unit = (unit, mult)
                break
        if matched_unit is None:
            return None

        out.append(amount * matched_unit[1])
        i = j + len(matched_unit[0])

    return out or None


def _parse_duration_prefix(
    text: str,
    allow_russian_duration: bool,
    max_parts: int = 3,
) -> tuple[int | None, int, bool]:
    """
    Парсит длительность в начале строки.
    Возвращает: (seconds_or_0_for_forever_or_None, consumed_tokens, is_invalid)
    - consumed_tokens == 0: длительность в начале не обнаружена
    - is_invalid == True: формат времени распознан как ошибочный
    """
    src = (text or "").strip()
    if not src:
        return None, 0, False

    tokens = src.split()
    if not tokens:
        return None, 0, False

    first_norm = tokens[0].strip().lower().strip('.,;:!?)(')
    if first_norm in ("forever", "навсегда"):
        if len(tokens) > 1 and _parse_duration_token_parts(tokens[1], allow_russian_duration):
            return None, 0, True
        return 0, 1, False

    total_seconds = 0
    consumed = 0
    parts_count = 0

    for tok in tokens[:max_parts]:
        parts = _parse_duration_token_parts(tok, allow_russian_duration)
        if not parts:
            break

        if parts_count + len(parts) > max_parts:
            return None, 0, True

        total_seconds += sum(parts)
        parts_count += len(parts)
        consumed += 1

    if consumed == 0:
        return None, 0, False

    if consumed < len(tokens):
        if _parse_duration_token_parts(tokens[consumed], allow_russian_duration):
            return None, 0, True

    if total_seconds < MIN_PUNISH_SECONDS or total_seconds > MAX_PUNISH_SECONDS:
        return None, 0, True

    return int(total_seconds), consumed, False


def _mod_is_row_active(kind: str, row: dict, now_ts: int | None = None) -> bool:
    if not row:
        return False
    if not row.get("active", True):
        return False
    if kind in ("mute", "ban"):
        now_val = int(now_ts or time.time())
        until_ts = int(row.get("until") or 0)
        if until_ts > 0 and until_ts <= now_val:
            return False
    return True


def _mod_deactivate_log(chat_id: int, kind: str, action_id: str, revoked_by: int | None = None):
    row = _mod_find_log(chat_id, kind, action_id)
    if row is None:
        return
    row["active"] = False
    row["revoked_at"] = time.time()
    if revoked_by:
        row["revoked_by"] = revoked_by


def _mod_cleanup_expired(chat_id: int):
    ch = _mod_get_chat(chat_id)
    now_ts = int(time.time())
    changed = False

    for kind in ("mute", "ban"):
        active_map = ch.get("active", {}).get(kind, {})
        for uid_s, rec in list(active_map.items()):
            until_ts = int((rec or {}).get("until") or 0)
            if until_ts > 0 and until_ts <= now_ts:
                active_map.pop(uid_s, None)
                rec_id = str((rec or {}).get("id") or "")
                if rec_id:
                    _mod_deactivate_log(chat_id, kind, rec_id)
                changed = True

        logs = ch.get("logs", {}).get(kind, [])
        for row in logs:
            if row.get("active", True) and not _mod_is_row_active(kind, row, now_ts):
                row["active"] = False
                row["revoked_at"] = float(now_ts)
                changed = True

    if changed:
        _mod_save()


def _mod_parse_target_for_un(m: types.Message, rest: str) -> int | None:
    if m.reply_to_message and m.reply_to_message.from_user:
        return m.reply_to_message.from_user.id

    parts = (rest or "").strip().split(maxsplit=1)
    if not parts:
        return None
    target_id = parse_target_user(m, [parts[0]])
    if isinstance(target_id, int) and target_id > 0:
        return target_id
    return None


def _mod_build_list_header(kind: str, page: int, total_pages: int) -> str:
    title = _mod_list_title(kind)
    return f'<tg-emoji emoji-id="{EMOJI_LOG_ID}">📋</tg-emoji> <b>{title} ({page}/{total_pages})</b>'


MOD_LIST_PAGE_SIZE = 10
LISTS_GROUP_COOLDOWN_SECONDS = 300


def _mod_list_title(kind: str) -> str:
    if kind == "warn":
        return "Список предупреждений"
    if kind == "mute":
        return "Список ограничений"
    return "Черный список"


def _mod_collect_rows(chat_id: int, kind: str) -> list[dict]:
    ch = _mod_get_chat(chat_id)
    rows: list[dict] = []

    if kind in ("mute", "ban"):
        active_map = (ch.get("active") or {}).get(kind) or {}
        for uid_s, rec in active_map.items():
            try:
                target_id = int(uid_s)
            except Exception:
                continue
            row = {
                "id": rec.get("id"),
                "target_id": target_id,
                "actor_id": rec.get("actor_id"),
                "created_at": rec.get("created_at"),
                "duration": rec.get("duration"),
                "until": rec.get("until"),
                "reason": rec.get("reason"),
                "active": True,
            }
            if _mod_is_row_active(kind, row):
                rows.append(row)
    else:
        warns_by_user = ch.get("warns") or {}
        for uid_s, arr in warns_by_user.items():
            try:
                target_id = int(uid_s)
            except Exception:
                continue
            for w in (arr or []):
                if not w.get("active", True):
                    continue
                rows.append({
                    "id": w.get("id"),
                    "target_id": target_id,
                    "actor_id": w.get("actor_id"),
                    "created_at": w.get("created_at"),
                    "duration": None,
                    "reason": w.get("reason"),
                    "active": True,
                })

    rows.sort(key=lambda r: float(r.get("created_at") or 0), reverse=True)
    return rows


def _mod_format_list_item(chat_id: int, row: dict, kind: str) -> str:
    action_id = str(row.get("id") or "")
    target_id = int(row.get("target_id") or 0)
    actor_id = int(row.get("actor_id") or 0)
    created_ts = int(float(row.get("created_at") or 0))
    until_ts = int(float(row.get("until") or 0))
    reason = (row.get("reason") or "").strip() or "—"

    target_name = link_for_user(chat_id, target_id)
    actor_name = link_for_user(chat_id, actor_id)

    kind_title = "Предупреждение" if kind == "warn" else ("Ограничение" if kind == "mute" else "Блокировка")
    lines = [
        f"<b>{kind_title} #{_html.escape(action_id)}</b>",
        f"• <b>Пользователь:</b> {target_name}",
        f"• <b>Выдан:</b> {_fmt_time(created_ts) if created_ts > 0 else '—'}",
    ]

    if kind in ("mute", "ban"):
        if until_ts > 0:
            lines.append(f"• <b>Истекает:</b> {_fmt_time(until_ts)}")
        else:
            lines.append("• <b>Истекает:</b> никогда")

    lines.extend([
        f"• <b>Причина:</b> {_html.escape(reason)}",
        f"• <b>Администратор:</b> {actor_name}",
    ])

    return "\n".join(lines)


def _mod_list_page_text(chat_id: int, kind: str, rows: list[dict], page: int) -> str:
    total_pages = max(1, (len(rows) + MOD_LIST_PAGE_SIZE - 1) // MOD_LIST_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * MOD_LIST_PAGE_SIZE
    end = start + MOD_LIST_PAGE_SIZE

    lines = [_mod_build_list_header(kind, page + 1, total_pages)]
    for row in rows[start:end]:
        lines.append("")
        lines.append(_mod_format_list_item(chat_id, row, kind))

    text = "\n".join(lines)
    if len(text) > 3900:
        text = text[:3897] + "..."
    return text


def _mod_list_keyboard(source_chat_id: int, kind: str, page: int, total_pages: int, viewer_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    nav_row: list[InlineKeyboardButton] = []

    if page > 0:
        nav_row.append(InlineKeyboardButton(
            "Предыдущая страница",
            callback_data=f"modlist:{kind}:{source_chat_id}:{page - 1}:{viewer_id}",
            icon_custom_emoji_id=str(EMOJI_PAGINATION_PREV_ID),
        ))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(
            "Следующая страница",
            callback_data=f"modlist:{kind}:{source_chat_id}:{page + 1}:{viewer_id}",
            icon_custom_emoji_id=str(EMOJI_PAGINATION_NEXT_ID),
        ))

    if nav_row:
        kb.row(*nav_row)

    return kb


def _build_open_pm_markup() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Открыть ЛС бота", url=f"https://t.me/{bot.get_me().username}"))
    return kb


def _reply_sent_to_pm(trigger: types.Message, text: str):
    return bot.reply_to(
        trigger,
        f"<i>{_html.escape(text)}</i>",
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_markup=_build_open_pm_markup(),
    )


def _mod_unpunish_message(text: str) -> str:
    return f'<tg-emoji emoji-id="{EMOJI_UNPUNISH_ID}">✅</tg-emoji> {text}'


def _send_mod_list_to_pm(trigger: types.Message, kind: str, rows: list[dict]):
    source_chat_id = trigger.chat.id
    total_pages = max(1, (len(rows) + MOD_LIST_PAGE_SIZE - 1) // MOD_LIST_PAGE_SIZE)
    text = _mod_list_page_text(source_chat_id, kind, rows, 0)
    kb = _mod_list_keyboard(source_chat_id, kind, 0, total_pages, trigger.from_user.id)

    try:
        bot.send_message(
            trigger.from_user.id,
            text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception:
        return bot.reply_to(
            trigger,
            premium_prefix("Не удалось отправить список в ЛС. Напишите боту в ЛС и повторите команду."),
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=_build_open_pm_markup(),
        )
    return _reply_sent_to_pm(trigger, f"{_mod_list_title(kind)} отправлен в ЛС.")


def _extract_command_info(m: types.Message) -> tuple[str | None, str | None, str]:
    text = (m.text or "").strip()
    if not text:
        return None, None, ""

    parts = text.split(maxsplit=1)
    first = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ""

    prefix = first[0] if first and first[0] in COMMAND_PREFIXES else None
    cmd = first[1:] if prefix else first
    cmd = cmd.split('@', 1)[0].lower()
    return prefix, cmd, rest


def _is_mod_trigger(m: types.Message) -> bool:
    if m.chat.type not in ['group', 'supergroup']:
        return False
    prefix, cmd, rest = _extract_command_info(m)
    if not cmd:
        return False

    prefixed_only = {
        "mute", "ban", "warn", "kick", "delmute", "delban", "delwarn",
        "warnlist", "mutelist", "banlist", "варнлист", "мутлист", "банлист", "del",
        "unmute", "unban", "unwarn",
        "делмут", "делбан", "делварн",
    }
    ru_plain_allowed = {"мут", "бан", "варн", "кик", "дел", "размут", "разбан", "снятьварн", "анмут", "анварн"}
    ru_list_aliases = {"варнлист", "мутлист", "банлист"}

    if prefix in COMMAND_PREFIXES:
        return cmd in prefixed_only or cmd in ru_plain_allowed

    if cmd not in ru_plain_allowed:
        return cmd in ru_list_aliases and not rest

    # без префикса перехватываем только когда есть reply или аргументы,
    # чтобы обычные фразы не воспринимались как команды
    return bool(m.reply_to_message or rest)


def _parse_target_duration_reason(
    m: types.Message,
    rest: str,
    allow_russian_duration: bool,
    force_reply_target: bool = False,
) -> tuple[int | None, int | None, str | None]:
    target_id: int | None = None
    reason: str | None = None
    duration: int | None = None

    if m.reply_to_message and m.reply_to_message.from_user:
        target_id = m.reply_to_message.from_user.id
        tail = (rest or "").strip()
        if tail:
            maybe_duration, consumed_tokens, invalid = _parse_duration_prefix(
                tail,
                allow_russian_duration=allow_russian_duration,
                max_parts=3,
            )
            if invalid:
                duration = -1
                reason = None
            elif consumed_tokens > 0:
                duration = None if maybe_duration == 0 else maybe_duration
                rem = tail.split()[consumed_tokens:]
                reason = " ".join(rem).strip() or None
            else:
                reason = tail
        return target_id, duration, reason

    if force_reply_target:
        return None, None, None

    if not rest:
        return None, None, None

    parts = rest.split(maxsplit=1)
    first = parts[0]
    after_target = parts[1].strip() if len(parts) > 1 else ""

    target_id = parse_target_user(m, [first])
    if not isinstance(target_id, int) or target_id < 1:
        return None, None, None

    if after_target:
        maybe_duration, consumed_tokens, invalid = _parse_duration_prefix(
            after_target,
            allow_russian_duration=allow_russian_duration,
            max_parts=3,
        )
        if invalid:
            duration = -1
            reason = None
        elif consumed_tokens > 0:
            duration = None if maybe_duration == 0 else maybe_duration
            rem = after_target.split()[consumed_tokens:]
            reason = " ".join(rem).strip() or None
        else:
            reason = after_target

    return target_id, duration, reason


def _parse_target_reason(
    m: types.Message,
    rest: str,
    force_reply_target: bool = False,
) -> tuple[int | None, str | None]:
    """
    Парсер цели и причины без поддержки длительности.
    Используется для команд, где временной формат недопустим.
    """
    if m.reply_to_message and m.reply_to_message.from_user:
        reason = (rest or "").strip() or None
        return m.reply_to_message.from_user.id, reason

    if force_reply_target:
        return None, None

    rest = (rest or "").strip()
    if not rest:
        return None, None

    parts = rest.split(maxsplit=1)
    target_id = parse_target_user(m, [parts[0]])
    if not isinstance(target_id, int) or target_id < 1:
        return None, None

    reason = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
    return target_id, reason


def _can_punish_target(chatid: int, actor: types.User, target_id: int) -> tuple[bool, str | None]:
    if target_id == actor.id:
        return False, "Нельзя применить наказание к самому себе."

    try:
        dummy = types.User(id=target_id, is_bot=False, first_name=".", last_name=None, username=None)
        if _is_special_actor(chatid, dummy):
            return False, "Нельзя наказать пользователя с особым статусом."
    except Exception:
        pass

    if target_id in VERIFY_DEV:
        return False, "Нельзя наказать dev-пользователя."

    try:
        member = bot.get_chat_member(chatid, target_id)
        if member.status in ("administrator", "creator"):
            return False, "Нельзя наказать пользователя с префиксом."
    except Exception:
        pass

    if not can_act_on(chatid, actor.id, target_id):
        return False, "Недостаточно иерархии для этого действия."

    return True, None


def _bot_can_restrict(chat_id: int) -> bool:
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        if member.status == "creator":
            return True
        if member.status == "administrator" and getattr(member, "can_restrict_members", False):
            return True
    except Exception:
        pass
    return False


def _mod_restrict_closed_permissions() -> types.ChatPermissions:
    return types.ChatPermissions(
        can_send_messages=False,
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_messages=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False,
    )


def _mod_restrict_open_permissions() -> types.ChatPermissions:
    return types.ChatPermissions(
        can_send_messages=True,
        can_send_audios=True,
        can_send_documents=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_video_messages=True,
        can_send_video_notes=True,
        can_send_voice_notes=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
    )


def _mod_log_append(chat_id: int, kind: str, row: dict):
    ch = _mod_get_chat(chat_id)
    logs = ch["logs"].setdefault(kind, [])
    logs.append(row)
    if len(logs) > 300:
        del logs[:-300]
    _mod_save()


def _mod_find_log(chat_id: int, kind: str, action_id: str) -> dict | None:
    ch = _mod_get_chat(chat_id)
    for row in reversed(ch["logs"].get(kind, [])):
        if str(row.get("id")) == str(action_id):
            return row
    return None


def _mod_warn_count(chat_id: int, user_id: int) -> int:
    ch = _mod_get_chat(chat_id)
    arr = ch["warns"].get(str(user_id)) or []
    return sum(1 for w in arr if w.get("active", True))


def _mod_warn_add(chat_id: int, actor_id: int, target_id: int, reason: str | None) -> tuple[str, int, float]:
    ch = _mod_get_chat(chat_id)
    uid = str(target_id)
    arr = ch["warns"].get(uid)
    if arr is None:
        arr = []
        ch["warns"][uid] = arr

    action_id = _mod_new_action_id()
    entry = {
        "id": action_id,
        "target_id": target_id,
        "actor_id": actor_id,
        "created_at": time.time(),
        "reason": (reason or "").strip(),
        "active": True,
    }
    arr.append(entry)

    count_after = sum(1 for w in arr if w.get("active", True))
    row = dict(entry)
    row["count_after"] = count_after
    _mod_log_append(chat_id, "warn", row)
    _mod_save()
    return action_id, count_after, float(entry["created_at"])


def _mod_warn_remove(chat_id: int, action_id: str, actor_id: int | None = None) -> tuple[bool, int | None]:
    ch = _mod_get_chat(chat_id)
    warns_by_user = ch.get("warns") or {}

    for uid, arr in warns_by_user.items():
        for w in arr:
            if str(w.get("id")) != str(action_id):
                continue
            if not w.get("active", True):
                return False, int(uid)
            w["active"] = False
            w["revoked_at"] = time.time()
            if actor_id:
                w["revoked_by"] = actor_id

            row = _mod_find_log(chat_id, "warn", action_id)
            if row is not None:
                row["active"] = False
                row["revoked_at"] = w["revoked_at"]
                if actor_id:
                    row["revoked_by"] = actor_id

            _mod_save()
            return True, int(uid)

    return False, None


def _mod_clear_all_warns_for_user(chat_id: int, user_id: int):
    ch = _mod_get_chat(chat_id)
    arr = ch.get("warns", {}).get(str(user_id)) or []
    now_ts = time.time()
    for w in arr:
        if w.get("active", True):
            w["active"] = False
            w["revoked_at"] = now_ts
    _mod_save()


def _mod_try_delete(chat_id: int, message_id: int | None):
    if not message_id:
        return
    try:
        bot.delete_message(chat_id, message_id)
    except Exception:
        pass


def _apply_mute(chat_id: int, target_id: int, duration: int | None) -> tuple[bool, str | None, int | None]:
    if not _bot_can_restrict(chat_id):
        return False, MOD_ERR["bot_no_mute"], None

    until_ts = int(time.time() + duration) if duration else None
    perms = _mod_restrict_closed_permissions()
    try:
        if until_ts:
            bot.restrict_chat_member(chat_id, target_id, permissions=perms, until_date=until_ts)
        else:
            bot.restrict_chat_member(chat_id, target_id, permissions=perms)
        tg_invalidate_chat_member_caches(chat_id, target_id)
        return True, None, until_ts
    except Exception as e:
        return False, f"Не удалось выдать ограничение: {e}", None


def _apply_ban(chat_id: int, target_id: int, duration: int | None) -> tuple[bool, str | None, int | None]:
    if not _bot_can_restrict(chat_id):
        return False, MOD_ERR["bot_no_ban"], None

    until_ts = int(time.time() + duration) if duration else None
    try:
        if until_ts:
            bot.ban_chat_member(chat_id, target_id, until_date=until_ts)
        else:
            bot.ban_chat_member(chat_id, target_id)
        _mark_farewell_suppressed(chat_id, target_id)
        tg_invalidate_chat_member_caches(chat_id, target_id)
        return True, None, until_ts
    except AttributeError:
        try:
            if until_ts:
                bot.kick_chat_member(chat_id, target_id, until_date=until_ts)
            else:
                bot.kick_chat_member(chat_id, target_id)
            tg_invalidate_chat_member_caches(chat_id, target_id)
            return True, None, until_ts
        except Exception as e:
            return False, f"Не удалось выдать блокировку: {e}", None
    except Exception as e:
        return False, f"Не удалось выдать блокировку: {e}", None


def _mod_unmute(chat_id: int, target_id: int) -> tuple[bool, str | None]:
    if not _bot_can_restrict(chat_id):
        return False, "У бота нет прав для снятия ограничения."
    try:
        bot.restrict_chat_member(chat_id, target_id, permissions=_mod_restrict_open_permissions())
        tg_invalidate_chat_member_caches(chat_id, target_id)
        return True, None
    except Exception as e:
        return False, f"Не удалось снять ограничение: {e}"


def _mod_unban(chat_id: int, target_id: int) -> tuple[bool, str | None]:
    if not _bot_can_restrict(chat_id):
        return False, "У бота нет прав для снятия блокировки."
    try:
        bot.unban_chat_member(chat_id, target_id, only_if_banned=True)
        tg_invalidate_chat_member_caches(chat_id, target_id)
        return True, None
    except Exception as e:
        return False, f"Не удалось снять блокировку: {e}"


def _send_punish_message_with_button(
    chat_id: int,
    action_kind: str,
    action_id: str,
    target_id: int,
    actor_id: int,
    duration: int | None,
    reason: str | None,
    *,
    until_ts: int | None = None,
    created_at: float | None = None,
    warn_count: int | None = None,
    warn_limit: int | None = None,
):
    if action_kind == "mute":
        label = "Ограничение"
        btn_text = "Снять ограничение"
    elif action_kind == "ban":
        label = "Блокировка"
        btn_text = "Разблокировать"
    else:
        label = "Предупреждение"
        btn_text = "Снять предупреждение"

    emoji_p = f'<tg-emoji emoji-id="{EMOJI_PUNISHMENT_ID}">⚠️</tg-emoji>'
    target_name = link_for_user(chat_id, target_id)
    actor_name = link_for_user(chat_id, actor_id)

    lines = [
        f"{emoji_p} <b>Пользователь</b> {target_name} <b>наказан.</b>",
        f"<b>Наказание:</b> {label}",
    ]

    if action_kind in ("mute", "ban"):
        if until_ts and until_ts > 0:
            lines.append(f"<b>Истекает:</b> {_fmt_time(until_ts)}")
        else:
            lines.append("<b>Истекает:</b> навсегда")

    if action_kind == "warn":
        if warn_count is not None and warn_limit is not None:
            lines.append(f"<b>Предупреждения:</b> {warn_count}/{warn_limit}")

    if reason:
        lines.append(f"<b>Причина:</b> {_html.escape(reason.strip())}")

    lines.extend(["", f"<b>Администратор:</b> {actor_name}"])
    text = "\n".join(lines)

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(
        btn_text,
        callback_data=f"punish_un:{chat_id}:{action_kind}:{target_id}:{action_id}",
        icon_custom_emoji_id=str(EMOJI_UNPUNISH_ID),
    ))

    try:
        sent_msg = bot.send_message(
            chat_id,
            text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )

        # Сохраняем исходный HTML сообщения, чтобы при редактировании не терялись tg-time теги.
        row = _mod_find_log(chat_id, action_kind, action_id)
        if row is not None:
            row["message_html"] = text
            row["message_id"] = int(getattr(sent_msg, "message_id", 0) or 0)
            _mod_save()
    except Exception as e:
        print(f"[PUNISH] send_message failed: {e}")



def _auto_punish_for_warns(chat_id: int, actor: types.User, target_id: int):
    ch = _mod_get_chat(chat_id)
    warn_limit = int((ch.get("settings") or {}).get("warn_limit", 3))
    wp = ch["settings"].get("warn_punish") or {}
    ptype = (wp.get("type") or "mute").lower()
    reason_suffix = (wp.get("reason") or "").strip()
    auto_reason = f"За достижение максимального количества предупреждений ({warn_limit})."
    if reason_suffix:
        auto_reason += f" {reason_suffix}"

    if ptype == "kick":
        err = _kick_with_unban(chat_id, actor, target_id, auto_reason)
        if not err:
            _mod_clear_all_warns_for_user(chat_id, target_id)
        return

    duration = wp.get("duration")
    if duration is not None:
        try:
            duration = int(duration)
        except Exception:
            duration = None

    action_id = _mod_new_action_id()
    until_ts = None
    if ptype == "mute":
        ok, _, until_ts = _apply_mute(chat_id, target_id, duration)
        if not ok:
            return
    elif ptype == "ban":
        ok, _, until_ts = _apply_ban(chat_id, target_id, duration)
        if not ok:
            return
    else:
        return

    row = {
        "id": action_id,
        "target_id": target_id,
        "actor_id": actor.id,
        "created_at": time.time(),
        "duration": int(duration or 0),
        "until": int(until_ts or 0),
        "reason": auto_reason,
        "active": True,
        "auto": True,
    }
    _mod_log_append(chat_id, ptype, row)

    ch = _mod_get_chat(chat_id)
    ch["active"][ptype][str(target_id)] = {
        "id": action_id,
        "actor_id": actor.id,
        "created_at": row["created_at"],
        "duration": row["duration"],
        "until": row["until"],
        "reason": row["reason"],
    }
    _mod_save()

    _send_punish_message_with_button(
        chat_id=chat_id,
        action_kind=ptype,
        action_id=action_id,
        target_id=target_id,
        actor_id=actor.id,
        duration=duration,
        reason=auto_reason,
        until_ts=int(until_ts or 0),
        created_at=row["created_at"],
    )
    _mod_clear_all_warns_for_user(chat_id, target_id)


def _process_moderation_action(
    m: types.Message,
    action_kind: str,
    rest: str,
    allow_russian_duration: bool,
    delete_target_message: bool = False,
    force_reply_target: bool = False,
) -> str | None:
    chat_id = m.chat.id
    actor = m.from_user
    _mod_cleanup_expired(chat_id)

    perm_map = {"mute": PERM_MUTE, "ban": PERM_BAN, "warn": PERM_WARN, "kick": PERM_KICK}
    no_perm_map = {
        "mute": MOD_ERR["no_perm_mute"],
        "ban": MOD_ERR["no_perm_ban"],
        "warn": MOD_ERR["no_perm_warn"],
        "kick": "У вашей должности нет права использовать кик.",
    }
    perm = perm_map[action_kind]

    status, allowed = check_role_permission(chat_id, actor.id, perm)
    if not allowed:
        if status == 'no_rank':
            return None
        if status == 'no_perm':
            return no_perm_map[action_kind]
        return MOD_ERR["no_rights"]

    if action_kind == "warn":
        warn_enabled = bool((_mod_get_chat(chat_id).get("settings") or {}).get("warn_enabled", True))
        if not warn_enabled:
            return "Система предупреждений выключена в /settings."

    if delete_target_message:
        st_del, ok_del = check_role_permission(chat_id, actor.id, PERM_DEL_MSG)
        if not ok_del:
            if st_del == 'no_rank':
                return None
            if st_del == 'no_perm':
                return MOD_ERR["no_perm_del"]
            return MOD_ERR["no_rights_delete"]
        if not _bot_can_delete_messages(chat_id):
            return MOD_ERR["bot_no_del"]

    if action_kind == "kick":
        target_id, reason = _parse_target_reason(
            m,
            rest,
            force_reply_target=force_reply_target,
        )
        duration = None
    else:
        target_id, duration, reason = _parse_target_duration_reason(
            m,
            rest,
            allow_russian_duration=allow_russian_duration,
            force_reply_target=force_reply_target,
        )
    if not target_id:
        return MOD_ERR["user_not_found"]

    ok_target, target_err = _can_punish_target(chat_id, actor, target_id)
    if not ok_target:
        return target_err or "Нельзя применить наказание к этому пользователю."

    if action_kind in ("mute", "ban") and duration is not None:
        if duration == -1:
            return MOD_ERR["bad_duration"]
        if duration < MIN_PUNISH_SECONDS or duration > MAX_PUNISH_SECONDS:
            return MOD_ERR["bad_duration"]

    if action_kind == "mute":
        ok, err, until_ts = _apply_mute(chat_id, target_id, duration)
        if not ok:
            return err or "Не удалось выдать ограничение."

        ch = _mod_get_chat(chat_id)
        prev = (ch.get("active", {}).get("mute", {}) or {}).get(str(target_id)) or {}
        prev_id = str(prev.get("id") or "")
        if prev_id:
            _mod_deactivate_log(chat_id, "mute", prev_id, revoked_by=actor.id)

        action_id = _mod_new_action_id()
        row = {
            "id": action_id,
            "target_id": target_id,
            "actor_id": actor.id,
            "created_at": time.time(),
            "duration": int(duration or 0),
            "until": int(until_ts or 0),
            "reason": (reason or "").strip(),
            "active": True,
        }
        _mod_log_append(chat_id, "mute", row)
        ch = _mod_get_chat(chat_id)
        ch["active"]["mute"][str(target_id)] = {
            "id": action_id,
            "actor_id": actor.id,
            "created_at": row["created_at"],
            "duration": row["duration"],
            "until": row["until"],
            "reason": row["reason"],
        }
        _mod_save()
        _send_punish_message_with_button(chat_id, "mute", action_id, target_id, actor.id, duration, reason,
                         until_ts=until_ts, created_at=row["created_at"])

    elif action_kind == "ban":
        ok, err, until_ts = _apply_ban(chat_id, target_id, duration)
        if not ok:
            return err or "Не удалось выдать блокировку."

        ch = _mod_get_chat(chat_id)
        prev = (ch.get("active", {}).get("ban", {}) or {}).get(str(target_id)) or {}
        prev_id = str(prev.get("id") or "")
        if prev_id:
            _mod_deactivate_log(chat_id, "ban", prev_id, revoked_by=actor.id)

        action_id = _mod_new_action_id()
        row = {
            "id": action_id,
            "target_id": target_id,
            "actor_id": actor.id,
            "created_at": time.time(),
            "duration": int(duration or 0),
            "until": int(until_ts or 0),
            "reason": (reason or "").strip(),
            "active": True,
        }
        _mod_log_append(chat_id, "ban", row)
        ch = _mod_get_chat(chat_id)
        ch["active"]["ban"][str(target_id)] = {
            "id": action_id,
            "actor_id": actor.id,
            "created_at": row["created_at"],
            "duration": row["duration"],
            "until": row["until"],
            "reason": row["reason"],
        }
        _mod_save()
        _send_punish_message_with_button(chat_id, "ban", action_id, target_id, actor.id, duration, reason,
                         until_ts=until_ts, created_at=row["created_at"])

    elif action_kind == "warn":
        action_id, count_after, warn_created_at = _mod_warn_add(chat_id, actor.id, target_id, reason)
        warn_limit = int((_mod_get_chat(chat_id).get("settings") or {}).get("warn_limit", 3))
        if count_after >= warn_limit:
            _auto_punish_for_warns(chat_id, actor, target_id)
        else:
            _send_punish_message_with_button(chat_id, "warn", action_id, target_id, actor.id, None, reason,
                     created_at=warn_created_at, warn_count=count_after, warn_limit=warn_limit)

    else:
        err = _kick_with_unban(chat_id, actor, target_id, reason)
        if err:
            return err
        row = {
            "id": _mod_new_action_id(),
            "target_id": target_id,
            "actor_id": actor.id,
            "created_at": time.time(),
            "duration": 0,
            "until": 0,
            "reason": (reason or "").strip(),
            "active": True,
        }
        _mod_log_append(chat_id, "kick", row)

    if delete_target_message and m.reply_to_message:
        _mod_try_delete(chat_id, m.reply_to_message.message_id)
    if delete_target_message:
        _mod_try_delete(chat_id, m.message_id)

    return None


@bot.message_handler(func=lambda m: _is_mod_trigger(m))
def cmd_moderation_main(m: types.Message):
    add_stat_message(m)

    prefix, cmd, rest = _extract_command_info(m)
    if not cmd:
        return

    if cmd in ("del", "дел") and prefix in ('/', '.'):
        add_stat_command('del')
        if not m.reply_to_message:
            return

        status, allowed = check_role_permission(m.chat.id, m.from_user.id, PERM_DEL_MSG)
        if not allowed:
            if status == 'no_perm':
                return reply_error(m, "no_perm_del")
            return

        if not _bot_can_delete_messages(m.chat.id):
            return reply_error(m, "bot_no_del")

        target_user = m.reply_to_message.from_user
        _mod_try_delete(m.chat.id, m.reply_to_message.message_id)
        _mod_try_delete(m.chat.id, m.message_id)

        if rest:
            emoji_del = f'<tg-emoji emoji-id="{EMOJI_DELETED_REASON_ID}">🗑️</tg-emoji>'
            mention = mention_html_by_id(target_user.id, target_user.full_name or target_user.first_name or "Пользователь")
            txt = f"{emoji_del} {mention} <b>Ваше сообщение было удалено по причине:</b> {_html.escape(rest.strip())}."
            try:
                bot.send_message(m.chat.id, txt, parse_mode='HTML', disable_web_page_preview=True)
            except Exception:
                pass
        return

    list_aliases = {
        "warnlist": "warn",
        "mutelist": "mute",
        "banlist": "ban",
        "варнлист": "warn",
        "мутлист": "mute",
        "банлист": "ban",
    }
    if cmd in list_aliases:
        # По требованию: у list-команд не должно быть аргументов.
        if rest:
            return
        add_stat_command(cmd)

        if not is_owner(m.from_user):
            wait_seconds = cooldown_hit('chat', int(m.chat.id), f"list_{cmd}", LISTS_GROUP_COOLDOWN_SECONDS)
            if wait_seconds > 0:
                return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action=f"list_{cmd}")

        _mod_cleanup_expired(m.chat.id)
        st, ok = check_role_permission(m.chat.id, m.from_user.id, PERM_VIEW_LISTS)
        if not ok and not _is_special_actor(m.chat.id, m.from_user):
            if st == 'no_perm':
                return bot.reply_to(
                    m,
                    premium_prefix("У вашей должности нет права смотреть списки наказаний."),
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            return

        kind = list_aliases[cmd]
        rows = _mod_collect_rows(m.chat.id, kind)

        if not rows:
            return bot.reply_to(m, premium_prefix("Список пуст."), parse_mode='HTML', disable_web_page_preview=True)

        return _send_mod_list_to_pm(m, kind, rows)

    if cmd in ("unmute", "размут", "анмут"):
        add_stat_command(cmd)
        _mod_cleanup_expired(m.chat.id)
        st, ok = check_role_permission(m.chat.id, m.from_user.id, PERM_UNMUTE)
        if not ok:
            if st == 'no_perm':
                return bot.reply_to(m, premium_prefix("У вашей должности нет права снимать ограничения."), parse_mode='HTML', disable_web_page_preview=True)
            return

        target_id = _mod_parse_target_for_un(m, rest)
        if not target_id:
            return bot.reply_to(m, premium_prefix(MOD_ERR["user_not_found"]), parse_mode='HTML', disable_web_page_preview=True)

        ok_target, target_err = _can_punish_target(m.chat.id, m.from_user, target_id)
        if not ok_target:
            return bot.reply_to(m, premium_prefix(target_err or "Нельзя снять ограничение этому пользователю."), parse_mode='HTML', disable_web_page_preview=True)

        ok2, err = _mod_unmute(m.chat.id, target_id)
        if not ok2:
            return bot.reply_to(m, premium_prefix(err or "Не удалось снять ограничение."), parse_mode='HTML', disable_web_page_preview=True)

        ch = _mod_get_chat(m.chat.id)
        prev = (ch.get("active", {}).get("mute", {}) or {}).pop(str(target_id), None)
        if prev and prev.get("id"):
            _mod_deactivate_log(m.chat.id, "mute", str(prev.get("id")), revoked_by=m.from_user.id)
            _mod_save()

        return bot.reply_to(m, _mod_unpunish_message("Ограничение снято."), parse_mode='HTML', disable_web_page_preview=True)

    if cmd in ("unban", "разбан"):
        add_stat_command(cmd)
        _mod_cleanup_expired(m.chat.id)
        st, ok = check_role_permission(m.chat.id, m.from_user.id, PERM_UNBAN)
        if not ok:
            if st == 'no_perm':
                return bot.reply_to(m, premium_prefix("У вашей должности нет права снимать блокировки."), parse_mode='HTML', disable_web_page_preview=True)
            return

        target_id = _mod_parse_target_for_un(m, rest)
        if not target_id:
            return bot.reply_to(m, premium_prefix(MOD_ERR["user_not_found"]), parse_mode='HTML', disable_web_page_preview=True)

        ok_target, target_err = _can_punish_target(m.chat.id, m.from_user, target_id)
        if not ok_target:
            return bot.reply_to(m, premium_prefix(target_err or "Нельзя снять блокировку этому пользователю."), parse_mode='HTML', disable_web_page_preview=True)

        ok2, err = _mod_unban(m.chat.id, target_id)
        if not ok2:
            return bot.reply_to(m, premium_prefix(err or "Не удалось снять блокировку."), parse_mode='HTML', disable_web_page_preview=True)

        ch = _mod_get_chat(m.chat.id)
        prev = (ch.get("active", {}).get("ban", {}) or {}).pop(str(target_id), None)
        if prev and prev.get("id"):
            _mod_deactivate_log(m.chat.id, "ban", str(prev.get("id")), revoked_by=m.from_user.id)
            _mod_save()

        return bot.reply_to(m, _mod_unpunish_message("Пользователь разблокирован."), parse_mode='HTML', disable_web_page_preview=True)

    if cmd in ("unwarn", "снятьварн", "анварн"):
        add_stat_command(cmd)
        st, ok = check_role_permission(m.chat.id, m.from_user.id, PERM_UNWARN)
        if not ok:
            if st == 'no_perm':
                return bot.reply_to(m, premium_prefix("У вашей должности нет права снимать предупреждения."), parse_mode='HTML', disable_web_page_preview=True)
            return

        target_id = _mod_parse_target_for_un(m, rest)
        if not target_id:
            return bot.reply_to(m, premium_prefix(MOD_ERR["user_not_found"]), parse_mode='HTML', disable_web_page_preview=True)

        ch = _mod_get_chat(m.chat.id)
        warns = (ch.get("warns") or {}).get(str(target_id)) or []
        active_warns = [w for w in warns if w.get("active", True)]
        if not active_warns:
            return bot.reply_to(m, premium_prefix("У пользователя нет активных предупреждений."), parse_mode='HTML', disable_web_page_preview=True)

        latest = sorted(active_warns, key=lambda x: float(x.get("created_at") or 0), reverse=True)[0]
        ok2, _ = _mod_warn_remove(m.chat.id, str(latest.get("id")), actor_id=m.from_user.id)
        if not ok2:
            return bot.reply_to(m, premium_prefix("Не удалось снять предупреждение."), parse_mode='HTML', disable_web_page_preview=True)

        return bot.reply_to(m, _mod_unpunish_message("Предупреждение снято."), parse_mode='HTML', disable_web_page_preview=True)

    action_map = {
        "mute": "mute",
        "мут": "mute",
        "ban": "ban",
        "бан": "ban",
        "warn": "warn",
        "варн": "warn",
        "kick": "kick",
        "кик": "kick",
        "delmute": "mute",
        "delban": "ban",
        "delwarn": "warn",
        "делмут": "mute",
        "делбан": "ban",
        "делварн": "warn",
    }
    if cmd not in action_map:
        return

    add_stat_command(cmd)
    action_kind = action_map[cmd]

    allow_ru = True

    need_reply = cmd in ("delmute", "delban", "delwarn", "делмут", "делбан", "делварн")
    if need_reply and not m.reply_to_message:
        return

    err = _process_moderation_action(
        m,
        action_kind=action_kind,
        rest=rest,
        allow_russian_duration=allow_ru,
        delete_target_message=need_reply,
        force_reply_target=need_reply,
    )
    if err:
        return bot.reply_to(m, premium_prefix(err), parse_mode='HTML', disable_web_page_preview=True)


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("punish_un:"))
def cb_punish_un(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    try:
        _, chat_s, kind, target_s, action_id = c.data.split(":", 4)
        chat_id = int(chat_s)
        target_id = int(target_s)
    except Exception:
        return bot.answer_callback_query(c.id)

    user = c.from_user
    perm_map = {
        "mute": (PERM_UNMUTE, "У вашей должности нет права снимать ограничения."),
        "ban": (PERM_UNBAN, "У вашей должности нет права снимать блокировки."),
        "warn": (PERM_UNWARN, "У вашей должности нет права снимать предупреждения."),
    }
    if kind not in perm_map:
        return bot.answer_callback_query(c.id)

    perm_name, no_perm_text = perm_map[kind]
    st, ok = check_role_permission(chat_id, user.id, perm_name)
    if not ok:
        if st == 'no_perm':
            return bot.answer_callback_query(c.id, no_perm_text, show_alert=True)
        return bot.answer_callback_query(c.id)

    row_snapshot = _mod_find_log(chat_id, kind, action_id)

    _mod_cleanup_expired(chat_id)

    if kind == "mute":
        ok2, err = _mod_unmute(chat_id, target_id)
        if not ok2:
            return bot.answer_callback_query(c.id, err or "Не удалось снять ограничение.", show_alert=True)
        ch = _mod_get_chat(chat_id)
        ch["active"]["mute"].pop(str(target_id), None)
        row = _mod_find_log(chat_id, "mute", action_id)
        if row is not None:
            row["active"] = False
            row["revoked_at"] = time.time()
            row["revoked_by"] = user.id
        result_line = "<b>Ограничение снято.</b>"
    elif kind == "ban":
        ok2, err = _mod_unban(chat_id, target_id)
        if not ok2:
            return bot.answer_callback_query(c.id, err or "Не удалось снять блокировку.", show_alert=True)
        ch = _mod_get_chat(chat_id)
        ch["active"]["ban"].pop(str(target_id), None)
        row = _mod_find_log(chat_id, "ban", action_id)
        if row is not None:
            row["active"] = False
            row["revoked_at"] = time.time()
            row["revoked_by"] = user.id
        result_line = "<b>Пользователь разблокирован.</b>"
    else:
        ok2, _ = _mod_warn_remove(chat_id, action_id, actor_id=user.id)
        if not ok2:
            return bot.answer_callback_query(c.id, "Предупреждение уже снято.", show_alert=True)
        result_line = "<b>Предупреждение снято.</b>"

    _mod_save()

    old_html = ""
    if isinstance(row_snapshot, dict):
        old_html = str(row_snapshot.get("message_html") or "").strip()
    if not old_html:
        old_html = c.message.html_text or c.message.text or ""
    append_block = _mod_unpunish_message(result_line)

    try:
        bot.edit_message_text(
            f"{old_html}\n\n{append_block}",
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=None,
        )
    except Exception:
        try:
            bot.edit_message_reply_markup(
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                reply_markup=None,
            )
        except Exception:
            pass

    return bot.answer_callback_query(c.id, "Готово.")


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("modlist:"))
def cb_modlist(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    parts = c.data.split(":")
    if len(parts) not in (4, 5):
        return bot.answer_callback_query(c.id)

    if len(parts) == 5:
        _, kind_or_close, source_chat_s, page_s, viewer_s = parts
    else:
        # Легаси-формат: modlist:{kind}:{page}:{viewer}
        _, kind_or_close, page_s, viewer_s = parts
        source_chat_s = str(c.message.chat.id)

    try:
        viewer_id = int(viewer_s)
        source_chat_id = int(source_chat_s)
    except Exception:
        return bot.answer_callback_query(c.id)

    if c.from_user.id != viewer_id:
        return bot.answer_callback_query(c.id, "Эти кнопки доступны только вызвавшему список.", show_alert=True)

    if kind_or_close == "close":
        try:
            bot.delete_message(c.message.chat.id, c.message.message_id)
        except Exception:
            pass
        return bot.answer_callback_query(c.id)

    if not _is_special_actor(source_chat_id, c.from_user):
        st, ok = check_role_permission(source_chat_id, c.from_user.id, PERM_VIEW_LISTS)
        if not ok:
            if st == 'no_perm':
                return bot.answer_callback_query(c.id, "У вашей должности нет права смотреть списки наказаний.", show_alert=True)
            return bot.answer_callback_query(c.id)

    kind = kind_or_close
    if kind not in ("warn", "mute", "ban"):
        return bot.answer_callback_query(c.id)

    try:
        page = int(page_s)
    except Exception:
        page = 0

    _mod_cleanup_expired(source_chat_id)
    rows = _mod_collect_rows(source_chat_id, kind)
    if not rows:
        try:
            bot.edit_message_text(
                premium_prefix("Список пуст."),
                chat_id=c.message.chat.id,
                message_id=c.message.message_id,
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_markup=None,
            )
        except Exception:
            pass
        return bot.answer_callback_query(c.id)

    total_pages = max(1, (len(rows) + MOD_LIST_PAGE_SIZE - 1) // MOD_LIST_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    text = _mod_list_page_text(source_chat_id, kind, rows, page)
    kb = _mod_list_keyboard(source_chat_id, kind, page, total_pages, viewer_id)

    try:
        bot.edit_message_text(
            text,
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception:
        pass

    return bot.answer_callback_query(c.id)


ADMIN_STATS_PAGE_SIZE = 10


def _can_view_adminstats_for_chat(chat_id: int, user: types.User) -> bool:
    if _is_special_actor(chat_id, user):
        return True
    _, allowed = check_role_permission(chat_id, user.id, PERM_VIEW_LISTS)
    return bool(allowed)


def _find_adminstats_groups_for_user(user: types.User) -> list[tuple[int, str]]:
    chat_ids: set[int] = set()
    for cid_str in (USERS or {}):
        try:
            chat_ids.add(int(cid_str))
        except ValueError:
            pass
    for cid_str in (MODERATION or {}):
        try:
            chat_ids.add(int(cid_str))
        except ValueError:
            pass
    for cid_str in (CHAT_ROLES or {}):
        try:
            chat_ids.add(int(cid_str))
        except ValueError:
            pass

    result: list[tuple[int, str]] = []
    for chat_id in chat_ids:
        if not is_group_approved(chat_id):
            continue
        if not _can_view_adminstats_for_chat(chat_id, user):
            continue
        try:
            chat = bot.get_chat(chat_id)
            title = chat.title or str(chat_id)
        except Exception:
            title = str(chat_id)
        result.append((chat_id, title))

    result.sort(key=lambda x: x[1].lower())
    return result


def _adminstats_is_current_admin(chat_id: int, user_id: int) -> bool:
    if get_user_rank(chat_id, user_id) > 0:
        return True

    try:
        member = bot.get_chat_member(chat_id, user_id)
        if member.status in ("administrator", "creator"):
            return True
    except Exception:
        pass

    if user_id in VERIFY_DEV:
        return True

    try:
        u = tg_get_chat(user_id)
        if is_owner(u):
            return True
    except Exception:
        pass

    return False


def _adminstats_role_label(chat_id: int, user_id: int) -> str:
    rank = get_user_rank(chat_id, user_id)
    role = get_rank_label_plain(rank)
    if role:
        return role

    try:
        member = bot.get_chat_member(chat_id, user_id)
        if member.status == "creator":
            return "Владелец чата"
        if member.status == "administrator":
            return "Администратор Telegram"
    except Exception:
        pass

    if user_id in VERIFY_DEV:
        return "Dev-пользователь"

    try:
        u = tg_get_chat(user_id)
        if is_owner(u):
            return "Разработчик бота"
    except Exception:
        pass

    return "Без должности"


def _adminstats_collect(chat_id: int) -> tuple[list[dict], list[dict]]:
    ch = _mod_get_chat(chat_id)
    logs = ch.get("logs") or {}
    by_actor: dict[int, dict] = {}

    for kind in ("mute", "ban", "warn", "kick"):
        for row in (logs.get(kind) or []):
            if not isinstance(row, dict):
                continue

            try:
                actor_id = int(row.get("actor_id") or 0)
            except Exception:
                actor_id = 0
            if actor_id <= 0:
                continue

            rec = by_actor.get(actor_id)
            if rec is None:
                rec = {
                    "user_id": actor_id,
                    "mute_count": 0,
                    "ban_count": 0,
                    "kick_count": 0,
                    "warn_count": 0,
                    "total": 0,
                    "last_ts": 0,
                }
                by_actor[actor_id] = rec

            if kind == "mute":
                rec["mute_count"] += 1
            elif kind == "ban":
                rec["ban_count"] += 1
            elif kind == "kick":
                rec["kick_count"] += 1
            elif kind == "warn":
                rec["warn_count"] += 1

            rec["total"] += 1

            try:
                ts = int(float(row.get("created_at") or 0))
            except Exception:
                ts = 0
            if ts > rec["last_ts"]:
                rec["last_ts"] = ts

    all_stats = list(by_actor.values())
    all_stats.sort(key=lambda x: (-int(x.get("total") or 0), -int(x.get("last_ts") or 0), int(x.get("user_id") or 0)))

    current: list[dict] = []
    past: list[dict] = []
    for rec in all_stats:
        uid = int(rec.get("user_id") or 0)
        rec["role"] = _adminstats_role_label(chat_id, uid)
        if _adminstats_is_current_admin(chat_id, uid):
            current.append(rec)
        else:
            past.append(rec)

    return current, past


def _adminstats_format_entry(chat_id: int, row: dict) -> str:
    uid = int(row.get("user_id") or 0)
    role_plain = str(row.get("role") or "Без должности")
    role_lower = role_plain.lower()
    last_ts = int(row.get("last_ts") or 0)
    last_ts_text = _fmt_time(last_ts) if last_ts > 0 else "—"
    name = link_for_user(chat_id, uid)

    rank = get_user_rank(chat_id, uid)
    if "разработчик" in role_lower or "dev" in role_lower:
        role_html = f'<tg-emoji emoji-id="{EMOJI_DEV_ID}">👨‍💻</tg-emoji> Разработчик бота'
    elif rank in (1, 2, 3, 4, 5):
        role_html = get_rank_label_html(rank)
    elif "владелец" in role_lower:
        role_html = f'<tg-emoji emoji-id="{EMOJI_ROLE_OWNER_ID}">👑</tg-emoji> Владелец чата'
    elif "администратор" in role_lower:
        role_html = f'<tg-emoji emoji-id="{EMOJI_ROLE_ADMIN_ID}">🛡️</tg-emoji> Администратор Telegram'
    else:
        role_html = f'<tg-emoji emoji-id="{EMOJI_MEMBER_ID}">👤</tg-emoji> {_html.escape(role_plain)}'

    lines = [
        f"<b>Пользователь:</b> {name} [<code>{uid}</code>] [{role_html}]",
        f"• <b>Ограничений:</b> <code>{int(row.get('mute_count') or 0)}</code>",
        f"• <b>Блокировок:</b> <code>{int(row.get('ban_count') or 0)}</code>",
        f"• <b>Предупреждений:</b> <code>{int(row.get('warn_count') or 0)}</code>",
        f"• <b>Исключений:</b> <code>{int(row.get('kick_count') or 0)}</code>",
        f"• <b>Всего наказаний:</b> <code>{int(row.get('total') or 0)}</code>",
        f"• <b>Последнее выданное наказание:</b> {last_ts_text}",
    ]
    return "\n".join(lines)


def _adminstats_render_section(chat_id: int, title: str, rows: list[dict], page: int) -> tuple[str, int, int]:
    total_pages = max(1, (len(rows) + ADMIN_STATS_PAGE_SIZE - 1) // ADMIN_STATS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * ADMIN_STATS_PAGE_SIZE
    end = start + ADMIN_STATS_PAGE_SIZE

    lines = [f"<b>{title} ({page + 1}/{total_pages})</b>"]
    sliced = rows[start:end]
    if not sliced:
        lines.append("<i>Нет данных.</i>")
    else:
        for row in sliced:
            lines.append("")
            lines.append(_adminstats_format_entry(chat_id, row))

    return "\n".join(lines), page, total_pages


def _adminstats_text(chat_id: int, current_rows: list[dict], past_rows: list[dict], view: str, page: int) -> tuple[str, int, int]:
    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    if view == "past":
        block, page, total = _adminstats_render_section(chat_id, "Прошлые администраторы", past_rows, page)
    else:
        view = "current"
        block, page, total = _adminstats_render_section(chat_id, "Нынешние администраторы", current_rows, page)

    emoji_stats = f'<tg-emoji emoji-id="{PREMIUM_STATS_EMOJI_ID}">📊</tg-emoji>'
    text = (
        f"{emoji_stats} <b>Статистика администраторов</b>\n"
        f"<b>Чат:</b> {_html.escape(title)} [<code>{chat_id}</code>]\n\n"
        f"{block}"
    )
    if len(text) > 3900:
        text = text[:3897] + "..."

    return text, page, total


def _adminstats_keyboard(chat_id: int, view: str, page: int, total: int, viewer_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(
            "Предыдущая страница",
            callback_data=f"astnav:{chat_id}:{viewer_id}:{view}:{page}:prev",
            icon_custom_emoji_id=str(EMOJI_PAGINATION_PREV_ID),
        ))
    if page < total - 1:
        nav_row.append(InlineKeyboardButton(
            "Следующая страница",
            callback_data=f"astnav:{chat_id}:{viewer_id}:{view}:{page}:next",
            icon_custom_emoji_id=str(EMOJI_PAGINATION_NEXT_ID),
        ))
    if nav_row:
        kb.row(*nav_row)

    toggle_label = "Нынешние администраторы" if view == "past" else "Прошлые администраторы"
    kb.row(InlineKeyboardButton(
        toggle_label,
        callback_data=f"astnav:{chat_id}:{viewer_id}:{view}:{page}:switch",
        icon_custom_emoji_id=str(EMOJI_LIST_ID),
    ))

    return kb


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and match_command_aliases(m.text, ['adminstats', 'adminstat', 'админстата']))
def cmd_adminstats(m: types.Message):
    add_stat_message(m)
    _, cmd, _ = _extract_command_info(m)
    add_stat_command(cmd or 'adminstats')

    if not check_group_approval(m):
        return

    if not is_owner(m.from_user):
        wait_seconds = cooldown_hit('chat', int(m.chat.id), 'adminstats', LISTS_GROUP_COOLDOWN_SECONDS)
        if wait_seconds > 0:
            return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='adminstats')

    if not _can_view_adminstats_for_chat(m.chat.id, m.from_user):
        return bot.reply_to(
            m,
            premium_prefix("У вашей должности нет права смотреть статистику администраторов."),
            parse_mode='HTML',
            disable_web_page_preview=True,
        )

    current_rows, past_rows = _adminstats_collect(m.chat.id)
    text, page, total = _adminstats_text(m.chat.id, current_rows, past_rows, "current", 0)
    kb = _adminstats_keyboard(m.chat.id, "current", page, total, m.from_user.id)

    try:
        bot.send_message(
            m.from_user.id,
            text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось отправить статистику в ЛС. Напишите боту в ЛС и повторите команду."),
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=_build_open_pm_markup(),
        )

    return _reply_sent_to_pm(m, "Статистика администраторов отправлена в ЛС.")


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("astchat:"))
def cb_adminstats_chat(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    try:
        _, chat_s, viewer_s = c.data.split(":", 2)
        chat_id = int(chat_s)
        viewer_id = int(viewer_s)
    except Exception:
        return bot.answer_callback_query(c.id)

    if c.from_user.id != viewer_id:
        return bot.answer_callback_query(c.id, "Эти кнопки доступны только вызвавшему команду.", show_alert=True)
    if c.message.chat.type != 'private':
        return bot.answer_callback_query(c.id)
    if not _can_view_adminstats_for_chat(chat_id, c.from_user):
        return bot.answer_callback_query(c.id, "Нет прав для просмотра статистики этого чата.", show_alert=True)

    current_rows, past_rows = _adminstats_collect(chat_id)
    text, page, total = _adminstats_text(chat_id, current_rows, past_rows, "current", 0)
    kb = _adminstats_keyboard(chat_id, "current", page, total, viewer_id)

    try:
        bot.edit_message_text(
            text,
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception:
        pass

    return bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("astnav:"))
def cb_adminstats_nav(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    try:
        _, chat_s, viewer_s, view, page_s, action = c.data.split(":", 5)
        chat_id = int(chat_s)
        viewer_id = int(viewer_s)
        page = int(page_s)
    except Exception:
        return bot.answer_callback_query(c.id)

    if c.from_user.id != viewer_id:
        return bot.answer_callback_query(c.id, "Эти кнопки доступны только вызвавшему команду.", show_alert=True)
    if c.message.chat.type != 'private':
        return bot.answer_callback_query(c.id)
    if not _can_view_adminstats_for_chat(chat_id, c.from_user):
        return bot.answer_callback_query(c.id, "Нет прав для просмотра статистики этого чата.", show_alert=True)

    current_rows, past_rows = _adminstats_collect(chat_id)
    if view not in ("current", "past"):
        view = "current"

    rows = current_rows if view == "current" else past_rows
    total = max(1, (len(rows) + ADMIN_STATS_PAGE_SIZE - 1) // ADMIN_STATS_PAGE_SIZE)
    page = max(0, min(page, total - 1))

    if action == "prev":
        page = max(0, page - 1)
    elif action == "next":
        page = min(total - 1, page + 1)
    elif action == "switch":
        view = "past" if view == "current" else "current"
        page = 0

    text, page, total = _adminstats_text(chat_id, current_rows, past_rows, view, page)
    kb = _adminstats_keyboard(chat_id, view, page, total, viewer_id)

    try:
        bot.edit_message_text(
            text,
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception:
        pass

    return bot.answer_callback_query(c.id)


# ==== УДАЛЕНИЕ СЕРВИСНОГО СООБЩЕНИЯ О ПИНЕ (TELETHON) ==== 

async def _mt_delete_last_pin_service_message(chat_id: int):
    """
    Удаляет последнее сервисное сообщение о закреплении в данном чате.
    Для супергрупп используем PeerChannel(chat_id), для обычных групп — PeerChat(-chat_id).
    Добавляем небольшую задержку, чтобы сервисное сообщение успело прилететь. [file:184]
    """
    try:
        await tg_client.start()

        # небольшая задержка, чтобы Telegram успел создать service-message о пине
        await asyncio.sleep(0.3)

        # определяем peer
        if str(chat_id).startswith("-100"):
            # супергруппа: id вида -100..., Telethon ждёт положительный id канала
            peer = PeerChannel(-chat_id)
        elif chat_id < 0:
            # обычная группа: отрицательный id -> PeerChat с положительным
            peer = PeerChat(-chat_id)
        else:
            # ЛС/канал по прямому id (на всякий случай)
            peer = chat_id

        # ищем самое верхнее сервисное сообщение о пине
        async for msg in tg_client.iter_messages(peer, limit=30):
            if isinstance(msg, MessageService) and getattr(msg, "action", None):
                if msg.action.__class__.__name__ == "MessageActionPinMessage":
                    await msg.delete()
                    break
    except Exception as e:
        print(f"[Telethon] Не удалось удалить сервисное сообщение о пине: {e}")


def _try_delete_last_bot_service_pin(chat_id: int):
    """
    Обёртка для запуска удаления сервисного сообщения о пине.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        asyncio.create_task(_mt_delete_last_pin_service_message(chat_id))
    else:
        asyncio.run(_mt_delete_last_pin_service_message(chat_id))


# ==== ЗАКРЕПЛЕНИЕ / ОТКРЕПЛЕНИЕ СООБЩЕНИЙ ==== 

PIN_INTERFACE_EXPIRE_SECONDS = 60
PIN_NOTIFY_KEEP_SERVICE_SECONDS = 20
PIN_NOTIFY_KEEP_SERVICE_UNTIL: dict[int, float] = {}


def _mark_keep_pin_service_message(chat_id: int):
    PIN_NOTIFY_KEEP_SERVICE_UNTIL[chat_id] = time.time() + PIN_NOTIFY_KEEP_SERVICE_SECONDS


def _should_keep_pin_service_message(chat_id: int) -> bool:
    until_ts = float(PIN_NOTIFY_KEEP_SERVICE_UNTIL.get(chat_id) or 0)
    if until_ts <= 0:
        return False
    if time.time() > until_ts:
        PIN_NOTIFY_KEEP_SERVICE_UNTIL.pop(chat_id, None)
        return False
    PIN_NOTIFY_KEEP_SERVICE_UNTIL.pop(chat_id, None)
    return True


def _user_can_do_pin_perm(chat_id: int, user_id: int, perm_name: str) -> tuple[bool, str, str | None]:
    """
    Общая проверка для закрепа/открепа.
    Возвращает (allowed, status, err_text_for_command_or_None).
    status: 'ok' / 'no_rank' / 'no_perm' / другое.
    Для КОМАНД:
      - 'no_rank' -> молчим,
      - 'no_perm' -> err_text,
      - ok -> err_text None.
    Для КНОПОК:
      - 'no_rank' -> False, callback молчит,
      - 'no_perm' -> False, текст отдаём в колбэк.
    """
    status, allowed = check_role_permission(chat_id, user_id, perm_name)
    if allowed:
        return True, 'ok', None

    if status == 'no_rank':
        return False, 'no_rank', None

    if status == 'no_perm':
        if perm_name == PERM_PIN:
            return False, 'no_perm', "У вашей должности нет права закреплять сообщения."
        if perm_name == PERM_UNPIN:
            return False, 'no_perm', "У вашей должности нет права откреплять сообщения."
        return False, 'no_perm', "У вашей должности нет нужного права."

    return False, status, "Вы не можете управлять закрепами."


def _bot_can_pin(chat_id: int) -> bool:
    """
    Проверяем, есть ли у бота право закреплять сообщения.
    """
    try:
        me = bot.get_me()
        member = bot.get_chat_member(chat_id, me.id)
        can_pin = getattr(member, "can_pin_messages", None)
        if member.status in ("administrator", "creator") and (can_pin is None or can_pin):
            return True
        if member.status in ("administrator", "creator") and can_pin is None:
            return True
        return False
    except Exception:
        return False


def _pin_message(chat_id: int, reply_msg_id: int, silent: bool) -> bool:
    """
    Пытаемся закрепить сообщение reply_msg_id.
    """
    if not _bot_can_pin(chat_id):
        return False
    try:
        _pin_message_or_raise(chat_id, reply_msg_id, silent)
        return True
    except ApiTelegramException:
        return False
    except Exception:
        return False


def _unpin_message(chat_id: int, msg_id: int | None = None) -> bool:
    """
    Открепить либо конкретное сообщение (если msg_id дан),
    либо последнее закреплённое (если msg_id None).
    """
    try:
        _unpin_message_or_raise(chat_id, msg_id)
        return True
    except ApiTelegramException:
        return False
    except Exception:
        return False


def _pin_message_or_raise(chat_id: int, reply_msg_id: int, silent: bool) -> None:
    if not _bot_can_pin(chat_id):
        raise RuntimeError("У бота нет прав для закрепления сообщений.")
    bot.pin_chat_message(chat_id, reply_msg_id, disable_notification=silent)


def _unpin_message_or_raise(chat_id: int, msg_id: int | None = None) -> None:
    if msg_id is not None:
        bot.unpin_chat_message(chat_id, msg_id)
    else:
        bot.unpin_all_chat_messages(chat_id)


def _next_operation_queue_id() -> int:
    global _OPERATION_QUEUE_NEXT_ID
    with _OPERATION_QUEUE_LOCK:
        _OPERATION_QUEUE_NEXT_ID += 1
        return _OPERATION_QUEUE_NEXT_ID


def get_operation_queue_size() -> int:
    with _OPERATION_QUEUE_LOCK:
        active_count = len(_OPERATION_QUEUE_ACTIVE)
    return _OPERATION_QUEUE.qsize() + active_count


def get_operation_queue_stats() -> dict[str, int]:
    with _OPERATION_QUEUE_LOCK:
        active_count = len(_OPERATION_QUEUE_ACTIVE)
    queued_count = _OPERATION_QUEUE.qsize()
    return {
        "queued": queued_count,
        "active": active_count,
        "total": queued_count + active_count,
    }


def enqueue_operation(kind: str, payload: dict[str, Any]) -> tuple[int, int]:
    operation_id = _next_operation_queue_id()
    task = {
        "id": operation_id,
        "kind": kind,
        "created_at": time.time(),
    }
    task.update(payload)
    _OPERATION_QUEUE.put(task)
    return operation_id, _OPERATION_QUEUE.qsize()


def _extract_retry_after_seconds(exc: Exception) -> int | None:
    seconds = getattr(exc, "seconds", None)
    if isinstance(seconds, (int, float)) and seconds > 0:
        return int(seconds)

    raw_parts = [
        str(getattr(exc, "description", "") or ""),
        str(exc or ""),
    ]
    raw_text = " ".join(part for part in raw_parts if part).lower()
    for pattern in (
        r"retry after (\d+)",
        r"flood(?:[ _-]?wait)?(?: of)? (\d+)",
        r"too many requests.*?(\d+)",
    ):
        match = _re.search(pattern, raw_text)
        if match:
            try:
                value = int(match.group(1))
            except Exception:
                value = 0
            if value > 0:
                return value
    return None


def _is_retryable_telegram_exception(exc: Exception) -> bool:
    class_name = exc.__class__.__name__.lower()
    if "floodwait" in class_name:
        return True

    if not isinstance(exc, ApiTelegramException):
        return False

    description = str(getattr(exc, "description", "") or exc).lower()
    retry_markers = (
        "retry after",
        "too many requests",
        "flood",
        "temporarily unavailable",
        "timeout",
        "timed out",
    )
    return any(marker in description for marker in retry_markers)


def _operation_backoff_seconds(exc: Exception, attempt: int) -> float:
    retry_after = _extract_retry_after_seconds(exc)
    if retry_after is not None:
        return float(min(max(1, retry_after), OPERATION_QUEUE_MAX_BACKOFF_SECONDS))
    return min(float(2 ** max(0, attempt - 1)) + random.uniform(0.0, 0.25), float(OPERATION_QUEUE_MAX_BACKOFF_SECONDS))


def _run_operation_with_retry(fn, *, label: str):
    last_error: Exception | None = None
    for attempt in range(1, OPERATION_QUEUE_MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            if attempt >= OPERATION_QUEUE_MAX_RETRIES or not _is_retryable_telegram_exception(exc):
                raise
            delay = _operation_backoff_seconds(exc, attempt)
            print(f"[OP_QUEUE] retry {attempt}/{OPERATION_QUEUE_MAX_RETRIES} for {label} after {delay:.2f}s: {exc}")
            time.sleep(delay)

    if last_error is not None:
        raise last_error


def _notify_operation_failure(chat_id: int, text: str, *, reply_to_message_id: int | None = None) -> None:
    try:
        bot.send_message(
            chat_id,
            premium_prefix(text),
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_to_message_id=reply_to_message_id,
        )
    except Exception:
        pass


def _process_broadcast_queue_task(task: dict[str, Any]) -> None:
    panel_chat_id = int(task["panel_chat_id"])
    panel_message_id = int(task["panel_message_id"])
    html_text = str(task.get("html_text") or "")
    media = list(task.get("media") or [])
    buttons = dict(task.get("buttons") or {})
    targets = list(task.get("targets") or [])

    sent_ok = 0
    sent_err = 0
    for uid in targets:
        try:
            _run_operation_with_retry(
                lambda uid=uid: _broadcast_send_payload_once(uid, html_text, media, buttons),
                label=f"broadcast:{uid}",
            )
            sent_ok += 1
        except Exception:
            sent_err += 1
        time.sleep(0.03)

    summary = (
        f'<tg-emoji emoji-id="{EMOJI_SENT_OK_ID}">✅</tg-emoji> <b>Сообщение отправлено!</b>\n\n'
        f"<b>Успешно:</b> <code>{sent_ok}</code>\n"
        f"<b>Ошибок:</b> <code>{sent_err}</code>"
    )

    try:
        bot.edit_message_text(
            summary,
            chat_id=panel_chat_id,
            message_id=panel_message_id,
            parse_mode='HTML',
            disable_web_page_preview=True,
        )
    except Exception:
        try:
            bot.send_message(panel_chat_id, summary, parse_mode='HTML', disable_web_page_preview=True)
        except Exception:
            pass


def _process_pin_apply_queue_task(task: dict[str, Any]) -> None:
    chat_id = int(task["chat_id"])
    iface_msg_id = int(task["iface_msg_id"])
    reply_msg_id = int(task["reply_msg_id"])
    silent = bool(task.get("silent", False))

    _run_operation_with_retry(
        lambda: _pin_message_or_raise(chat_id, reply_msg_id, silent),
        label=f"pin:{chat_id}:{reply_msg_id}",
    )

    if silent:
        _try_delete_last_bot_service_pin(chat_id)
    else:
        _mark_keep_pin_service_message(chat_id)

    try:
        bot.delete_message(chat_id, iface_msg_id)
    except Exception:
        pass


def _process_pin_repin_queue_task(task: dict[str, Any]) -> None:
    chat_id = int(task["chat_id"])
    iface_msg_id = int(task["iface_msg_id"])
    reply_msg_id = int(task["reply_msg_id"])

    _run_operation_with_retry(
        lambda: _unpin_message_or_raise(chat_id, reply_msg_id),
        label=f"repin:{chat_id}:{reply_msg_id}",
    )

    try:
        bot.delete_message(chat_id, iface_msg_id)
    except Exception:
        pass

    fake_trigger = types.Message(message_id=iface_msg_id, chat=types.Chat(chat_id, 'group'), date=0)
    fake_target = types.Message(message_id=reply_msg_id, chat=types.Chat(chat_id, 'group'), date=0)
    _send_pin_interface(chat_id, fake_trigger, fake_target)


def _dispatch_operation_queue_task(task: dict[str, Any]) -> None:
    kind = str(task.get("kind") or "")
    if kind == "broadcast_send":
        _process_broadcast_queue_task(task)
        return
    if kind == "pin_apply":
        _process_pin_apply_queue_task(task)
        return
    if kind == "pin_repin":
        _process_pin_repin_queue_task(task)
        return
    raise RuntimeError(f"Неизвестный тип операции очереди: {kind}")


def _operation_queue_worker():
    while True:
        task = _OPERATION_QUEUE.get()
        task_id = int(task.get("id") or 0)
        with _OPERATION_QUEUE_LOCK:
            _OPERATION_QUEUE_ACTIVE[task_id] = task
        try:
            _dispatch_operation_queue_task(task)
        except Exception as exc:
            print(f"[OP_QUEUE] task {task_id} failed: {exc}")
            kind = str(task.get("kind") or "")
            if kind == "broadcast_send":
                _notify_operation_failure(int(task.get("panel_chat_id") or 0), "Не удалось завершить рассылку.")
            elif kind in ("pin_apply", "pin_repin"):
                _notify_operation_failure(
                    int(task.get("chat_id") or 0),
                    "Не удалось выполнить операцию закрепа.",
                    reply_to_message_id=int(task.get("reply_msg_id") or 0) or None,
                )
        finally:
            with _OPERATION_QUEUE_LOCK:
                _OPERATION_QUEUE_ACTIVE.pop(task_id, None)
            _OPERATION_QUEUE.task_done()


_OPERATION_QUEUE_THREAD = threading.Thread(target=_operation_queue_worker, daemon=True)
_OPERATION_QUEUE_THREAD.start()


def _is_exact_command(m: types.Message, names: list[str]) -> bool:
    """
    Текст — ровно команда из списка (без аргументов),
    с префиксом из COMMAND_PREFIXES или русское слово без префикса.
    """
    text = (m.text or "").strip()
    if not text:
        return False

    lower = text.lower()

    # без префикса (русские)
    for name in names:
        if all('а' <= ch <= 'я' or ch == 'ё' for ch in name.lower()) and lower == name:
            return True

    # с префиксами
    for prefix in COMMAND_PREFIXES:
        for name in names:
            if lower == f"{prefix}{name}":
                return True

    return False


def _get_last_pinned_message_id(chat_id: int) -> int | None:
    """
    Получаем последнее закреплённое сообщение через Bot API. [file:195]
    """
    try:
        chat = bot.get_chat(chat_id)
        pinned = getattr(chat, "pinned_message", None)
        if isinstance(pinned, types.Message):
            return pinned.message_id
    except Exception:
        pass
    return None


def _build_pin_interface_keyboard(chat_id: int, iface_msg_id: int, replied_msg_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton(
            "С уведомлением",
            callback_data=f"pin_notify:{chat_id}:{iface_msg_id}:{replied_msg_id}",
            icon_custom_emoji_id=str(EMOJI_PIN_NOTIFY_ID)
        ),
        InlineKeyboardButton(
            "Без уведомления",
            callback_data=f"pin_silent:{chat_id}:{iface_msg_id}:{replied_msg_id}",
            icon_custom_emoji_id=str(EMOJI_PIN_SILENT_ID)
        ),
    )
    kb.row(
        InlineKeyboardButton(
            "Отменить",
            callback_data=f"pin_cancel:{chat_id}:{iface_msg_id}",
            icon_custom_emoji_id=str(EMOJI_ROLE_SETTINGS_CANCEL_ID)
        )
    )
    return kb


def _build_pin_repin_keyboard(chat_id: int, iface_msg_id: int, replied_msg_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton(
            "Открепить и закрепить снова",
            callback_data=f"pin_repin:{chat_id}:{iface_msg_id}:{replied_msg_id}",
            icon_custom_emoji_id=str(EMOJI_PIN_REPIN_ID)
        )
    )
    kb.row(
        InlineKeyboardButton(
            "Отменить",
            callback_data=f"pin_cancel:{chat_id}:{iface_msg_id}",
            icon_custom_emoji_id=str(EMOJI_ROLE_SETTINGS_CANCEL_ID)
        )
    )
    return kb


def _send_pin_interface(chat_id: int, trigger_msg: types.Message, target_msg: types.Message):
    """
    Отправить сообщение с выбором типа закрепа.
    Если target_msg уже закреплено — даём кнопку «Открепить и закрепить снова».
    Сообщение интерфейса отправляем reply на команду и добавляем имя закрепляющего.
    """
    if not _bot_can_pin(chat_id):
        return bot.reply_to(
            trigger_msg,
            premium_prefix("У бота нет прав для закрепления сообщений."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    actor_name = link_for_user(chat_id, trigger_msg.from_user.id)

    current_pinned_id = _get_last_pinned_message_id(chat_id)
    if current_pinned_id == target_msg.message_id:
        text = (
            "<b>Это сообщение уже закреплено.</b>\n"
            f"Закрепляет: {actor_name}"
        )
        sent = bot.reply_to(
            trigger_msg,
            text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=_build_pin_repin_keyboard(chat_id, 0, target_msg.message_id)
        )
        kb_fixed = _build_pin_repin_keyboard(chat_id, sent.message_id, target_msg.message_id)
        try:
            bot.edit_message_reply_markup(chat_id, sent.message_id, reply_markup=kb_fixed)
        except Exception:
            pass
        return

    text = (
        "<b>Выберите как закрепить это сообщение:</b>\n"
        f"Закрепляет: {actor_name}"
    )
    sent = bot.reply_to(
        trigger_msg,
        text,
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_markup=_build_pin_interface_keyboard(chat_id, 0, target_msg.message_id)
    )
    kb_fixed = _build_pin_interface_keyboard(chat_id, sent.message_id, target_msg.message_id)
    try:
        bot.edit_message_reply_markup(chat_id, sent.message_id, reply_markup=kb_fixed)
    except Exception:
        pass


# ==== КОМАНДЫ ПИНА / СПИН / НПИН ====


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and _is_exact_command(m, ["pin", "пин", "закреп", "закрепить"]))
def cmd_pin(m: types.Message):
    add_stat_message(m)
    add_stat_command('pin')

    # Проверка одобрения группы
    if not check_group_approval(m):
        return

    if not m.reply_to_message:
        return

    chat_id = m.chat.id
    user_id = m.from_user.id

    allowed, status, err_text = _user_can_do_pin_perm(chat_id, user_id, PERM_PIN)
    if not allowed:
        if status == 'no_perm' and err_text:
            return bot.reply_to(
                m,
                premium_prefix(err_text),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    _send_pin_interface(chat_id, m, m.reply_to_message)


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and _is_exact_command(m, ["spin"]))
def cmd_spin(m: types.Message):
    """
    /spin — тихое закрепление сразу.
    """
    add_stat_message(m)
    add_stat_command('spin')

    # Проверка одобрения группы
    if not check_group_approval(m):
        return

    if not m.reply_to_message:
        return

    chat_id = m.chat.id
    user_id = m.from_user.id

    allowed, status, err_text = _user_can_do_pin_perm(chat_id, user_id, PERM_PIN)
    if not allowed:
        if status == 'no_perm' and err_text:
            return bot.reply_to(
                m,
                premium_prefix(err_text),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    if not _bot_can_pin(chat_id):
        return bot.reply_to(
            m,
            premium_prefix("У бота нет прав для закрепления сообщений."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    ok = _pin_message(chat_id, m.reply_to_message.message_id, silent=True)
    if not ok:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось закрепить сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    _try_delete_last_bot_service_pin(chat_id)


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and _is_exact_command(m, ["npin"]))
def cmd_npin(m: types.Message):
    """
    /npin — закрепление с уведомлением сразу.
    """
    add_stat_message(m)
    add_stat_command('npin')

    if not m.reply_to_message:
        return

    chat_id = m.chat.id
    user_id = m.from_user.id

    allowed, status, err_text = _user_can_do_pin_perm(chat_id, user_id, PERM_PIN)
    if not allowed:
        if status == 'no_perm' and err_text:
            return bot.reply_to(
                m,
                premium_prefix(err_text),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    if not _bot_can_pin(chat_id):
        return bot.reply_to(
            m,
            premium_prefix("У бота нет прав для закрепления сообщений."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    ok = _pin_message(chat_id, m.reply_to_message.message_id, silent=False)
    if not ok:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось закрепить сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    _mark_keep_pin_service_message(chat_id)


# ==== КОМАНДЫ ОТКРЕПЛЕНИЯ ====


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and _is_exact_command(m, ["unpin", "анпин", "откреп", "открепить"]))
def cmd_unpin(m: types.Message):
    add_stat_message(m)
    add_stat_command('unpin')

    # Проверка одобрения группы
    if not check_group_approval(m):
        return

    if not m.reply_to_message:
        return

    chat_id = m.chat.id
    user_id = m.from_user.id

    allowed, status, err_text = _user_can_do_pin_perm(chat_id, user_id, PERM_UNPIN)
    if not allowed:
        if status == 'no_perm' and err_text:
            txt = err_text.replace("закреплять", "откреплять")
            return bot.reply_to(
                m,
                premium_prefix(txt),
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    if not _bot_can_pin(chat_id):
        return bot.reply_to(
            m,
            premium_prefix("У бота нет прав для закрепления сообщений."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    ok = _unpin_message(chat_id, m.reply_to_message.message_id)
    if not ok:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось открепить сообщение."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    emoji_saved = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    text = f"{emoji_saved} Сообщение откреплено."
    bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True)


# ==== CALLBACK-КНОПКИ ПИНА ====


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith(("pin_notify:", "pin_silent:", "pin_cancel:", "pin_repin:")))
def cb_pin_interface(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    data = c.data or ""
    user = c.from_user

    try:
        if data.startswith("pin_cancel:"):
            _, chat_id_s, iface_msg_id_s = data.split(":", 2)
            chat_id = int(chat_id_s)
            iface_msg_id = int(iface_msg_id_s)

            allowed, status, _ = _user_can_do_pin_perm(chat_id, user.id, PERM_PIN)
            if not allowed:
                if status == 'no_perm':
                    bot.answer_callback_query(
                        c.id,
                        "У вашей должности нет права управлять закрепами.",
                        show_alert=True
                    )
                return

            try:
                bot.delete_message(chat_id, iface_msg_id)
            except Exception:
                pass
            bot.answer_callback_query(c.id)
            return

        if data.startswith("pin_repin:"):
            _, chat_id_s, iface_msg_id_s, reply_msg_id_s = data.split(":", 3)
            chat_id = int(chat_id_s)
            iface_msg_id = int(iface_msg_id_s)
            reply_msg_id = int(reply_msg_id_s)

            allowed, status, _ = _user_can_do_pin_perm(chat_id, user.id, PERM_PIN)
            if not allowed:
                if status == 'no_perm':
                    bot.answer_callback_query(
                        c.id,
                        "У вашей должности нет права закреплять сообщения.",
                        show_alert=True
                    )
                return

            if not _bot_can_pin(chat_id):
                bot.answer_callback_query(
                    c.id,
                    "У бота нет прав для закрепления сообщений.",
                    show_alert=True
                )
                return

            _, queue_size = enqueue_operation(
                "pin_repin",
                {
                    "chat_id": chat_id,
                    "iface_msg_id": iface_msg_id,
                    "reply_msg_id": reply_msg_id,
                    "actor_id": user.id,
                },
            )

            bot.answer_callback_query(c.id, f"Операция поставлена в очередь ({queue_size}).")
            return

        mode, chat_id_s, iface_msg_id_s, reply_msg_id_s = data.split(":", 3)
        chat_id = int(chat_id_s)
        iface_msg_id = int(iface_msg_id_s)
        reply_msg_id = int(reply_msg_id_s)
        silent = (mode == "pin_silent")

        allowed, status, _ = _user_can_do_pin_perm(chat_id, user.id, PERM_PIN)
        if not allowed:
            if status == 'no_perm':
                bot.answer_callback_query(
                    c.id,
                    "У вашей должности нет права закреплять сообщения.",
                    show_alert=True
                )
            return

        if not _bot_can_pin(chat_id):
            bot.answer_callback_query(
                c.id,
                "У бота нет прав для закрепления сообщений.",
                show_alert=True
            )
            return

        _, queue_size = enqueue_operation(
            "pin_apply",
            {
                "chat_id": chat_id,
                "iface_msg_id": iface_msg_id,
                "reply_msg_id": reply_msg_id,
                "actor_id": user.id,
                "silent": silent,
            },
        )

        bot.answer_callback_query(c.id, f"Закрепление поставлено в очередь ({queue_size}).")

    except Exception:
        try:
            bot.answer_callback_query(c.id)
        except Exception:
            pass


# ============================================
# ==== НАСТРОЙКИ ЧАТА (/settings) + WELCOME / FAREWELL / RULES
# ============================================

def _now_ts() -> int:
    return int(time.time())


# ------------------------------------------------------------
# Pending helpers (чтобы cancel/ok работали одинаково везде)
# ------------------------------------------------------------

def _pending_get(key: str) -> dict:
    return CHAT_SETTINGS.get(key) or {}


def _pending_put(key: str, user_id: int, chat_id: int):
    d = _pending_get(key)
    d[str(user_id)] = str(chat_id)
    CHAT_SETTINGS[key] = d
    save_chat_settings()


def _pending_pop(key: str, user_id: int) -> Optional[str]:
    d = _pending_get(key)
    val = d.pop(str(user_id), None)
    CHAT_SETTINGS[key] = d
    save_chat_settings()
    return val


def _pending_msg_get(key: str, user_id: int) -> Optional[int]:
    d = _pending_get(key)
    val = d.get(str(user_id))
    try:
        return int(val) if val is not None else None
    except Exception:
        return None


def _pending_msg_set(key: str, user_id: int, msg_id: int):
    d = _pending_get(key)
    d[str(user_id)] = int(msg_id)
    CHAT_SETTINGS[key] = d
    save_chat_settings()


def _pending_msg_pop(key: str, user_id: int) -> Optional[int]:
    d = _pending_get(key)
    val = d.pop(str(user_id), None)
    CHAT_SETTINGS[key] = d
    save_chat_settings()
    try:
        return int(val) if val is not None else None
    except Exception:
        return None


def _try_delete_private_prompt(chat_id: int, msg_id: Optional[int]):
    """Пытаемся удалить сообщение бота в ЛС. Любые ошибки проглатываем."""
    if not msg_id:
        return
    try:
        raw_delete_message(chat_id, msg_id)
        return
    except Exception:
        pass
    try:
        bot.delete_message(chat_id, msg_id)
    except Exception:
        pass


def _delete_pending_ui(chat_id: int, msg_key: str, user_id: int, also_msg_id: Optional[int] = None):
    """
    Удаляет текущую UI-мессагу для pending_* (prompt/error/deleted),
    которая хранится в pending_*_msg. Если stored msg_id нет — можно
    передать also_msg_id (например c.message.message_id).
    """
    stored = _pending_msg_pop(msg_key, user_id)
    if stored:
        _try_delete_private_prompt(chat_id, stored)
    if also_msg_id and (not stored or stored != also_msg_id):
        _try_delete_private_prompt(chat_id, also_msg_id)


def _replace_pending_ui(chat_id: int, msg_key: str, user_id: int, text: str, reply_markup=None, parse_mode: str = "HTML"):
    """
    Заменяет UI-мессагу для pending_*: удаляет предыдущую (если была),
    отправляет новую и сохраняет её message_id в msg_key.
    """
    old_id = _pending_msg_pop(msg_key, user_id)
    _try_delete_private_prompt(chat_id, old_id)
    sent = bot.send_message(chat_id, text, parse_mode=parse_mode, disable_web_page_preview=True, reply_markup=reply_markup)
    _pending_msg_set(msg_key, user_id, sent.message_id)
    return sent


def _build_cancel_btn(callback_data: str) -> "InlineKeyboardButton":
    btn = InlineKeyboardButton("Отмена", callback_data=callback_data)
    try:
        btn.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_CANCEL_ID)
    except Exception:
        pass
    return btn


def _build_back_to_prompt_btn(callback_data: str) -> "InlineKeyboardButton":
    btn = InlineKeyboardButton("Назад", callback_data=callback_data)
    try:
        btn.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
    except Exception:
        pass
    try:
        btn.style = "primary"
    except Exception:
        pass
    return btn


def _kb_error_cancel(callback_data: str) -> "InlineKeyboardMarkup":
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(_build_cancel_btn(callback_data))
    return kb


def _kb_deleted(back_cb: str, cancel_cb: str) -> "InlineKeyboardMarkup":
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(_build_back_to_prompt_btn(back_cb), _build_cancel_btn(cancel_cb))
    return kb


def _safe_edit_message_html(chat_id: int, msg_id: int, text: str, reply_markup=None) -> bool:
    """
    FIX #1:
    при edit_message_text всегда передаём parse_mode='HTML', иначе
    в интерфейсе/превью будут показываться теги (<tg-emoji>, <b>, <quote> и т.д.)
    """
    try:
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
        return True
    except Exception as e:
        # Частый кейс: пользователь нажал уже выбранное значение, Telegram отвечает
        # "message is not modified" — это не ошибка для UI.
        if "message is not modified" in str(e).lower():
            return True
        # fallback на твой raw-редактор, если он есть
        try:
            resp = raw_edit_message_with_keyboard(chat_id, msg_id, text, reply_markup)
            if isinstance(resp, dict):
                if resp.get("ok"):
                    return True
                desc = str(resp.get("description") or "").lower()
                if "message is not modified" in desc:
                    return True
            return False
        except Exception:
            return False


# ------------------------------------------------------------
# Section model: welcome / farewell / rules
# ------------------------------------------------------------

SECTION_KEYS = ("welcome", "farewell", "rules")


def _default_section(enabled: bool) -> dict:
    return {
        "enabled": enabled,          # rules может быть выключен/включен, но использоваться по кнопке
        "text_custom": "",           # канон: твой кастом
        "source": "plain",           # plain/custom/entities/hybrid
        "entities": [],              # debug
        "updated_at": 0,
        "media": [],                 # список элементов медиа (dict)
        "buttons": {                 # кнопки + попапы
            "rows": [],              # rows: [[btn,btn],[btn]]
            "popups": [],            # список текстов попапов
        },
    }


def get_chat_settings(chat_id: int) -> dict:
    cid = str(chat_id)
    st = CHAT_SETTINGS.get(cid)

    # --- новый чат ---
    if st is None or not isinstance(st, dict):
        st = {
            "welcome": _default_section(False),
            "farewell": _default_section(False),
            "rules": _default_section(False),
            "cleanup": _default_cleanup(),
        }
        CHAT_SETTINGS[cid] = st
        save_chat_settings()
        return st

    # --- миграция/нормализация секций ---
    for sec in SECTION_KEYS:
        cur = st.get(sec)

        if cur is None or not isinstance(cur, dict):
            cur = _default_section(False)

        # --- миграция старых полей ---
        if "text_custom" not in cur:
            raw = (
                cur.get("text_custom")
                or cur.get("text_raw")
                or cur.get("text_html")
                or cur.get("text")
                or ""
            )
            cur["text_custom"] = raw if isinstance(raw, str) else ""
            cur["source"] = cur.get("source") or (
                "custom" if _contains_custom_tags(cur["text_custom"]) else "plain"
            )
            cur["entities"] = cur.get("text_entities") or cur.get("entities") or []
            cur["updated_at"] = cur.get("updated_at") or 0

            if not isinstance(cur.get("media"), list):
                cur["media"] = []

            btn = cur.get("buttons")
            if isinstance(btn, list):
                cur["buttons"] = {"rows": btn, "popups": []}
            elif btn is None:
                cur["buttons"] = {"rows": [], "popups": []}
            elif isinstance(btn, dict):
                btn.setdefault("rows", [])
                btn.setdefault("popups", [])
                cur["buttons"] = btn
            else:
                cur["buttons"] = {"rows": [], "popups": []}

            for k in ("text_raw", "text_html", "text_entities", "text"):
                cur.pop(k, None)

        # --- нормализация текущего формата ---
        cur.setdefault("enabled", False)
        cur.setdefault("text_custom", "")
        cur.setdefault("source", "plain")
        cur.setdefault("entities", [])
        cur.setdefault("updated_at", 0)

        if not isinstance(cur.get("media"), list):
            cur["media"] = []

        btn = cur.get("buttons")
        if isinstance(btn, list):
            cur["buttons"] = {"rows": btn, "popups": []}
        elif btn is None:
            cur["buttons"] = {"rows": [], "popups": []}
        elif isinstance(btn, dict):
            btn.setdefault("rows", [])
            btn.setdefault("popups", [])
            cur["buttons"] = btn
        else:
            cur["buttons"] = {"rows": [], "popups": []}

        if not isinstance(cur["buttons"].get("rows"), list):
            cur["buttons"]["rows"] = []
        if not isinstance(cur["buttons"].get("popups"), list):
            cur["buttons"]["popups"] = []

        st[sec] = cur

    # --- нормализация cleanup (В КОНЦЕ, после секций) ---
    cleanup_norm, changed = _normalize_cleanup(st.get("cleanup"))
    if changed:
        st["cleanup"] = cleanup_norm

    CHAT_SETTINGS[cid] = st
    save_chat_settings()
    return st

# ------------------------------------------------------------
# CLEANUP: удаление сообщений (Команды / Системные сообщения)
# ------------------------------------------------------------

CLEANUP_CMD_SIGNS = ("/", ".", "!", ",", "#")

# Premium emoji ids (твои)
CLEANUP_ICON_ENABLE_ID = "5825794181183836432"   # включить
CLEANUP_ICON_DISABLE_ID = "5778527486270770928"  # выключить

# Системные типы (как content_type у pyTelegramBotAPI)
CLEANUP_SYSTEM_TYPES_ORDER = [
    "new_chat_members",
    "left_chat_member",
    "new_chat_title",
    "new_chat_photo",
    "delete_chat_photo",
    "pinned_message",
    "message_auto_delete_timer_changed",
    "video_chat_scheduled",
    "video_chat_started",
    "video_chat_ended",
    "video_chat_participants_invited",
]

CLEANUP_SYSTEM_LABELS = {
    "new_chat_members": "Вход/добавление участников",
    "left_chat_member": "Выход/удаление участников",
    "new_chat_title": "Изменение названия",
    "new_chat_photo": "Новое фото чата",
    "delete_chat_photo": "Удаление фото чата",
    "pinned_message": "Закрепление сообщения",
    "message_auto_delete_timer_changed": "Таймер автоудаления",
    "video_chat_scheduled": "Запланирован видеочат",
    "video_chat_started": "Видеочат начался",
    "video_chat_ended": "Видеочат закончился",
    "video_chat_participants_invited": "Приглашения в видеочат",
}

CLEANUP_SYSTEM_CONTENT_TYPES = list(CLEANUP_SYSTEM_LABELS.keys())


def _default_cleanup() -> dict:
    return {
        "commands": {s: False for s in CLEANUP_CMD_SIGNS},
        "system": {ct: False for ct in CLEANUP_SYSTEM_TYPES_ORDER},
        "updated_at": 0,
        # legacy: оставим, но UI больше не использует
        "system_messages": False,
    }


def _normalize_cleanup(cleanup_any) -> tuple[dict, bool]:
    """
    Возвращает (cleanup_norm, changed_flag).
    Миграция с legacy system_messages:
      - если system ещё не было, а system_messages=True -> включим ВСЕ system-типы.
    """
    changed = False
    if not isinstance(cleanup_any, dict):
        return _default_cleanup(), True

    cleanup = dict(cleanup_any)  # копия

    # commands
    cmds = cleanup.get("commands")
    if not isinstance(cmds, dict):
        cmds = {}
        changed = True
    for s in CLEANUP_CMD_SIGNS:
        v = cmds.get(s, False)
        if not isinstance(v, bool):
            v = bool(v)
            changed = True
        if s not in cmds:
            changed = True
        cmds[s] = v
    cleanup["commands"] = cmds

    # system
    legacy_sys = cleanup.get("system_messages")
    sysd = cleanup.get("system")
    sys_was_missing = not isinstance(sysd, dict)
    if not isinstance(sysd, dict):
        sysd = {}
        changed = True

    for ct in CLEANUP_SYSTEM_TYPES_ORDER:
        v = sysd.get(ct, False)
        if not isinstance(v, bool):
            v = bool(v)
            changed = True
        if ct not in sysd:
            changed = True
        # миграция legacy
        if sys_was_missing and isinstance(legacy_sys, bool) and legacy_sys:
            v = True
        sysd[ct] = v

    cleanup["system"] = sysd

    # updated_at
    if not isinstance(cleanup.get("updated_at"), int):
        cleanup["updated_at"] = int(cleanup.get("updated_at") or 0)
        changed = True

    # legacy key keep
    if not isinstance(cleanup.get("system_messages"), bool):
        cleanup["system_messages"] = bool(cleanup.get("system_messages"))
        changed = True

    return cleanup, changed


def _cleanup_get(chat_id: int) -> dict:
    st = get_chat_settings(chat_id)
    cleanup_norm, changed = _normalize_cleanup(st.get("cleanup"))
    if changed:
        st["cleanup"] = cleanup_norm
        CHAT_SETTINGS[str(chat_id)] = st
        save_chat_settings()
    return cleanup_norm


def _cleanup_save(chat_id: int, cleanup: dict):
    st = get_chat_settings(chat_id)
    st["cleanup"] = cleanup
    CHAT_SETTINGS[str(chat_id)] = st
    save_chat_settings()


def _bot_can_delete_messages(chat_id: int) -> bool:
    """
    Проверяем право бота на удаление сообщений.
    ВАЖНО: никаких уведомлений в чат в рантайме -> без флуда.
    """
    bot_id = _get_bot_id()  # определён ниже в файле — ок
    if not bot_id:
        return False
    try:
        member = bot.get_chat_member(chat_id, bot_id)
        if getattr(member, "status", "") == "creator":
            return True
        if getattr(member, "status", "") == "administrator" and getattr(member, "can_delete_messages", False):
            return True
    except Exception:
        pass
    return False


# ------------------------------------------------------------
# Твой кастом -> Telegram HTML
# ------------------------------------------------------------

def _contains_custom_tags(s: str) -> bool:
    """
    FIX #1 (часть 2):
    Раньше функция не считала <b>/<i>/<u>/<s>/<code>/<pre>/<a ...> за кастом,
    из-за чего source часто становился "plain" и внешний код мог отправлять text_custom
    без конвертации -> в чате показывались теги.
    """
    if not s:
        return False
    sl = s.lower()
    return (
        "<b" in sl or "<i" in sl or "<u" in sl or "<s" in sl or "<code" in sl or "<pre" in sl
        or "<sp" in sl or "<spoiler" in sl or "<quote" in sl or "<emoji" in sl
        or "<br" in sl or "<a " in sl
        # поддержим также "официальные" теги Telegram, если пользователь их вставит:
        or "<tg-emoji" in sl or "<blockquote" in sl or 'class="tg-spoiler"' in sl
    )


class _Node:
    __slots__ = ("tag", "attrs", "children")

    def __init__(self, tag: Optional[str] = None, attrs: Optional[dict] = None):
        self.tag = tag
        self.attrs = attrs or {}
        self.children: List[Any] = []

    def append(self, child: Any):
        self.children.append(child)

    def render(self) -> str:
        # FIX: нормализуем escape/unescape, чтобы не было двойного &amp;amp;
        if self.tag is None:
            return "".join(
                ch.render() if isinstance(ch, _Node) else _html.escape(_html.unescape(str(ch)))
                for ch in self.children
            )

        inner = "".join(
            ch.render() if isinstance(ch, _Node) else _html.escape(_html.unescape(str(ch)))
            for ch in self.children
        )

        tag = self.tag
        attrs = self.attrs or {}

        if tag == "tg-emoji":
            eid = _html.escape(attrs.get("emoji-id", ""), quote=True)
            return f'<tg-emoji emoji-id="{eid}">{inner}</tg-emoji>'

        if tag == "span" and attrs.get("class") == "tg-spoiler":
            return f'<span class="tg-spoiler">{inner}</span>'

        if tag == "a":
            href = _html.escape(attrs.get("href", ""), quote=True)
            return f'<a href="{href}">{inner}</a>'

        if tag == "blockquote":
            if attrs.get("expandable") == "true":
                return f'<blockquote expandable="true">{inner}</blockquote>'
            return f"<blockquote>{inner}</blockquote>"

        return f"<{tag}>{inner}</{tag}>"


def convert_custom_markup_to_telegram_html(text: str) -> str:
    """
    Вход (твой кастом):
      <b>..</> <i>..</> <u>..</> <s>..</>
      <code>..</> <pre>..</>
      <sp>..</> / <spoiler>..</>
      <a href='URL'>..</>
      <quote>..</> / <quote exp>..</>
      <emoji id='123'>😀</>
      <br> -> \n

    + ПОДДЕРЖКА "официального" Telegram HTML (если пользователь вставит):
      <tg-emoji emoji-id="...">..</tg-emoji>
      <blockquote expandable="true">..</blockquote>
      <span class="tg-spoiler">..</span>
    """
    if not text:
        return ""

    s = text
    i = 0
    n = len(s)
    root = _Node()
    stack = [root]

    def push_text(chunk: str):
        if chunk:
            stack[-1].append(chunk)

    while i < n:
        if s[i] != "<":
            nxt = s.find("<", i)
            if nxt == -1:
                push_text(s[i:])
                break
            push_text(s[i:nxt])
            i = nxt
            continue

        close = s.find(">", i + 1)
        if close == -1:
            push_text(s[i:])
            break

        rawtag = s[i + 1:close].strip()
        i = close + 1
        if not rawtag:
            continue

        raw_low = rawtag.lower()

        if raw_low in ("br", "br/"):
            push_text("\n")
            continue

        # закрывающие: </> или </b> или </tg-emoji> и т.д.
        if rawtag.startswith("/"):
            name = rawtag[1:].strip().lower()
            if not name:
                if len(stack) > 1:
                    stack.pop()
                continue
            name = name.split()[0]
            j = len(stack) - 1
            while j > 0 and stack[j].tag not in (name, "blockquote"):
                j -= 1
            if j > 0:
                while len(stack) - 1 >= j:
                    stack.pop()
            continue

        # --- Официальный tg-emoji ---
        if raw_low.startswith("tg-emoji"):
            m = re.match(r'tg-emoji\s+emoji-id=[\'"]?(\d+)[\'"]?', rawtag, flags=re.I)
            if not m:
                push_text("<" + rawtag + ">")
                continue
            eid = m.group(1)
            node = _Node("tg-emoji", {"emoji-id": eid})
            stack[-1].append(node)
            stack.append(node)
            continue

        # --- Официальный blockquote ---
        if raw_low.startswith("blockquote"):
            attrs = {}
            if re.search(r'expandable\s*=\s*[\'"]?true[\'"]?', rawtag, flags=re.I):
                attrs["expandable"] = "true"
            node = _Node("blockquote", attrs)
            stack[-1].append(node)
            stack.append(node)
            continue

        # --- Официальный spoiler span ---
        if raw_low.startswith("span"):
            if re.search(r'class\s*=\s*[\'"]tg-spoiler[\'"]', rawtag, flags=re.I):
                node = _Node("span", {"class": "tg-spoiler"})
                stack[-1].append(node)
                stack.append(node)
                continue

        # quote (твой кастом)
        if raw_low.startswith("quote"):
            attrs = {}
            if re.match(r"quote\s+exp", raw_low):
                attrs["expandable"] = "true"
            node = _Node("blockquote", attrs)
            stack[-1].append(node)
            stack.append(node)
            continue

        # emoji (твой кастом)
        if raw_low.startswith("emoji"):
            m = re.match(r"emoji\s+id=['\"]?(\d+)['\"]?", rawtag, flags=re.I)
            if not m:
                push_text("<" + rawtag + ">")
                continue
            eid = m.group(1)
            node = _Node("tg-emoji", {"emoji-id": eid})
            stack[-1].append(node)
            stack.append(node)
            continue

        # a href
        if raw_low.startswith("a"):
            m = re.match(r'a\s+href=[\'"]([^\'"]+)[\'"]', rawtag, flags=re.I)
            if not m:
                push_text("<" + rawtag + ">")
                continue
            href = m.group(1)
            node = _Node("a", {"href": href})
            stack[-1].append(node)
            stack.append(node)
            continue

        tagname = raw_low.split()[0]

        if tagname in ("sp", "spoiler"):
            node = _Node("span", {"class": "tg-spoiler"})
            stack[-1].append(node)
            stack.append(node)
            continue

        if tagname in ("b", "i", "u", "s", "code", "pre"):
            node = _Node(tagname, {})
            stack[-1].append(node)
            stack.append(node)
            continue

        push_text("<" + rawtag + ">")

    return root.render()


# ------------------------------------------------------------
# Telegram entities -> твой кастом (UTF-16 offsets)
# ------------------------------------------------------------

def _utf16_units(text: str) -> List[int]:
    b = text.encode("utf-16-le")
    return [int.from_bytes(b[i:i + 2], "little") for i in range(0, len(b), 2)]


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _slice_utf16(text: str, units: List[int], start_u: int, len_u: int) -> str:
    start = max(start_u, 0)
    end = max(start_u + len_u, 0)
    sub = units[start:end]
    bb = b"".join(u.to_bytes(2, "little") for u in sub)
    return bb.decode("utf-16-le")


def _remove_utf16_range(text: str, start_u: int, len_u: int) -> str:
    units = _utf16_units(text)
    start = max(start_u, 0)
    end = max(start_u + len_u, 0)
    if start >= len(units) or end <= start:
        return text
    end = min(end, len(units))
    new_units = units[:start] + units[end:]
    bb = b"".join(u.to_bytes(2, "little") for u in new_units)
    return bb.decode("utf-16-le")


def _serialize_entities(entities: list) -> list:
    out = []
    for e in (entities or []):
        out.append({
            "type": getattr(e, "type", "") or "",
            "offset": int(getattr(e, "offset", 0) or 0),
            "length": int(getattr(e, "length", 0) or 0),
            "custom_emoji_id": getattr(e, "custom_emoji_id", None),
            "url": getattr(e, "url", None),
        })
    return out


def _entity_conflicts_with_tags(text: str, entities: list) -> bool:
    if not text or not entities:
        return False
    units = _utf16_units(text)
    for e in entities:
        try:
            off = int(getattr(e, "offset", 0) or 0)
            ln = int(getattr(e, "length", 0) or 0)
        except Exception:
            continue
        if ln <= 0:
            continue
        seg = _slice_utf16(text, units, off, ln)
        if "<" in seg or ">" in seg:
            return True
    return False


def _wrap_custom(escaped_inner: str, ent) -> str:
    et = (getattr(ent, "type", "") or "").lower()

    if et == "custom_emoji":
        ce_id = getattr(ent, "custom_emoji_id", None)
        ce_safe = _html.escape(str(ce_id or ""), quote=True)
        return f"<emoji id='{ce_safe}'>{escaped_inner}</>"

    if et == "bold":
        return f"<b>{escaped_inner}</>"
    if et == "italic":
        return f"<i>{escaped_inner}</>"
    if et == "underline":
        return f"<u>{escaped_inner}</>"
    if et == "strikethrough":
        return f"<s>{escaped_inner}</>"
    if et == "spoiler":
        return f"<spoiler>{escaped_inner}</>"
    if et == "code":
        return f"<code>{escaped_inner}</>"
    if et == "pre":
        return f"<pre>{escaped_inner}</>"
    if et == "text_link":
        url = getattr(ent, "url", "") or ""
        url_safe = _html.escape(url, quote=True)
        return f"<a href='{url_safe}'>{escaped_inner}</>"
    if et == "url":
        href = _html.unescape(escaped_inner)
        href_safe = _html.escape(href, quote=True)
        return f"<a href='{href_safe}'>{escaped_inner}</>"

    return escaped_inner


def entities_to_custom(text: str, entities: list) -> str:
    if not text:
        return ""
    if not entities:
        return _html.escape(text)

    units = _utf16_units(text)
    total_u = len(units)

    norm = []
    for e in entities:
        try:
            off = int(getattr(e, "offset", 0) or 0)
            ln = int(getattr(e, "length", 0) or 0)
        except Exception:
            continue
        if ln <= 0:
            continue
        end = min(off + ln, total_u)
        if off < 0 or off >= total_u or end <= off:
            continue
        norm.append((off, end, e))

    if not norm:
        return _html.escape(text)

    bounds = {0, total_u}
    for off, end, _ in norm:
        bounds.add(off)
        bounds.add(end)
    bounds = sorted(bounds)

    def prio(ent) -> int:
        t = (getattr(ent, "type", "") or "").lower()
        order = {
            "blockquote": 0,
            "expandable_blockquote": 0,
            "text_link": 1,
            "url": 1,
            "bold": 2,
            "italic": 3,
            "underline": 4,
            "strikethrough": 5,
            "spoiler": 6,
            "code": 7,
            "pre": 7,
            "custom_emoji": 8,
        }
        return order.get(t, 50)

    out_parts: List[str] = []

    for i in range(len(bounds) - 1):
        seg_start = bounds[i]
        seg_end = bounds[i + 1]
        if seg_end <= seg_start:
            continue

        raw_seg = _slice_utf16(text, units, seg_start, seg_end - seg_start)
        esc_seg = _html.escape(raw_seg)

        active = [ent for off, end, ent in norm if off <= seg_start and end >= seg_end]
        if not active:
            out_parts.append(esc_seg)
            continue

        quote_type = None
        non_quote = []
        for ent in active:
            t = (getattr(ent, "type", "") or "").lower()
            if t == "blockquote":
                quote_type = "quote"
            elif t == "expandable_blockquote":
                quote_type = "quote exp"
            else:
                non_quote.append(ent)

        non_quote_sorted = sorted(non_quote, key=prio)

        inner = esc_seg
        for ent in reversed(non_quote_sorted):
            inner = _wrap_custom(inner, ent)

        if quote_type == "quote":
            inner = f"<quote>{inner}</>"
        elif quote_type == "quote exp":
            inner = f"<quote exp>{inner}</>"

        out_parts.append(inner)

    return "".join(out_parts)


# ------------------------------------------------------------
# Message -> canonical text_custom
# ------------------------------------------------------------

def convert_section_text_from_message(m: types.Message) -> Tuple[str, str, list]:
    # ВАЖНО: offsets entities считаются по исходному тексту.
    raw_full = (m.text or "")
    entities = m.entities or []

    if not raw_full.strip():
        return "", "plain", []

    # если есть entities — НЕ strip'аем, иначе оффсеты съедут
    raw_text = raw_full if entities else raw_full.strip()

    entities_ser = _serialize_entities(entities)
    has_custom = _contains_custom_tags(raw_text)
    has_entities = bool(entities)

    if not has_custom and not has_entities:
        return raw_text, "plain", entities_ser

    if has_custom and not has_entities:
        return raw_text, "custom", entities_ser

    if (not has_custom) and has_entities:
        return entities_to_custom(raw_text, entities), "entities", entities_ser

    if _entity_conflicts_with_tags(raw_text, entities):
        return raw_text, "custom", entities_ser

    return entities_to_custom(raw_text, entities), "hybrid", entities_ser


def build_html_from_text_custom(text_custom: str) -> str:
    tc = (text_custom or "").strip()
    if not tc:
        return ""
    try:
        return convert_custom_markup_to_telegram_html(tc)
    except Exception:
        return _html.escape(tc)


def _apply_vars(html_text: str, chat_id: int, chat_title: str, user_obj) -> str:
    viewer = user_obj
    viewer_name = (viewer.full_name or viewer.first_name or "").strip() or "Участник"
    viewer_link = link_for_user(chat_id, viewer.id)
    try:
        viewer_mention = mention_html_user(viewer)
    except Exception:
        viewer_mention = viewer_link

    return (
        (html_text or "")
        .replace("[NAME]", _html.escape(viewer_name))
        .replace("[ID]", str(viewer.id))
        .replace("[GROUP_NAME]", _html.escape(chat_title or str(chat_id)))
        .replace("[NAME_LINK]", viewer_link)
        .replace("[MENTION]", viewer_mention)
    )


# ------------------------------------------------------------
# Media: store file_id, type, (caption не храним отдельно!)
# ------------------------------------------------------------

SUPPORTED_MEDIA_TYPES = {"photo", "video", "document", "audio", "animation"}


def _extract_media_payload(m: types.Message) -> Optional[dict]:
    ct = m.content_type
    if ct not in SUPPORTED_MEDIA_TYPES:
        return None

    if ct == "photo":
        # берём самое большое
        fid = m.photo[-1].file_id if m.photo else None
    elif ct == "video":
        fid = m.video.file_id if m.video else None
    elif ct == "document":
        fid = m.document.file_id if m.document else None
    elif ct == "audio":
        fid = m.audio.file_id if m.audio else None
    elif ct == "animation":
        fid = m.animation.file_id if m.animation else None
    else:
        fid = None

    if not fid:
        return None

    return {"type": ct, "file_id": fid}


def _media_can_album(items: List[dict]) -> bool:
    if not items or len(items) < 2:
        return False
    # альбомы: фото/видео (gif/audio/doc не альбом)
    for it in items:
        if it.get("type") not in ("photo", "video"):
            return False
    return True


def _send_media_only(chat_id: int, media: List[dict]):
    # показ без текста и без кнопок
    if not media:
        return

    if _media_can_album(media):
        mg = []
        for it in media:
            t = it["type"]
            fid = it["file_id"]
            if t == "photo":
                mg.append(types.InputMediaPhoto(media=fid))
            else:
                mg.append(types.InputMediaVideo(media=fid))
        bot.send_media_group(chat_id, mg)
        return

    # single or non-album list: шлём по одному
    for it in media:
        t = it["type"]
        fid = it["file_id"]
        if t == "photo":
            bot.send_photo(chat_id, fid)
        elif t == "video":
            bot.send_video(chat_id, fid)
        elif t == "document":
            bot.send_document(chat_id, fid)
        elif t == "audio":
            bot.send_audio(chat_id, fid)
        elif t == "animation":
            bot.send_animation(chat_id, fid)


def _send_payload(chat_id: int, html_text: str, media: List[dict], reply_markup=None, disable_web_page_preview=True):
    """
    Главное правило: caption отдельно не задаём пользователю.
    Если media есть, то caption = html_text (если поддерживается),
    иначе text message = html_text.
    """
    html_text = (html_text or "").strip()

    if media:
        if _media_can_album(media):
            mg = []
            for idx, it in enumerate(media):
                t = it["type"]
                fid = it["file_id"]
                if t == "photo":
                    if idx == 0 and html_text:
                        mg.append(types.InputMediaPhoto(media=fid, caption=html_text, parse_mode="HTML"))
                    else:
                        mg.append(types.InputMediaPhoto(media=fid))
                else:
                    if idx == 0 and html_text:
                        mg.append(types.InputMediaVideo(media=fid, caption=html_text, parse_mode="HTML"))
                    else:
                        mg.append(types.InputMediaVideo(media=fid))
            bot.send_media_group(chat_id, mg)
            # кнопки нельзя к media_group, поэтому отдельным сообщением с невидимым символом
            if reply_markup:
                bot.send_message(chat_id, "\u2063", disable_web_page_preview=True, reply_markup=reply_markup)
            return

        # НЕ альбом: шлём первое медиа с caption, остальное без
        first = True
        for it in media:
            t = it["type"]
            fid = it["file_id"]
            cap = html_text if (first and html_text) else None
            first = False

            if t == "photo":
                bot.send_photo(chat_id, fid, caption=cap, parse_mode="HTML" if cap else None, reply_markup=reply_markup if cap else None)
                reply_markup = None  # клаву цепляем только один раз
            elif t == "video":
                bot.send_video(chat_id, fid, caption=cap, parse_mode="HTML" if cap else None, reply_markup=reply_markup if cap else None)
                reply_markup = None
            elif t == "document":
                bot.send_document(chat_id, fid, caption=cap, parse_mode="HTML" if cap else None, reply_markup=reply_markup if cap else None)
                reply_markup = None
            elif t == "audio":
                bot.send_audio(chat_id, fid, caption=cap, parse_mode="HTML" if cap else None, reply_markup=reply_markup if cap else None)
                reply_markup = None
            elif t == "animation":
                bot.send_animation(chat_id, fid, caption=cap, parse_mode="HTML" if cap else None, reply_markup=reply_markup if cap else None)
                reply_markup = None
        return

    # no media
    if not html_text:
        # нельзя отправить «пусто» с кнопками — подставим невидимый символ
        if reply_markup:
            return bot.send_message(chat_id, "\u2063", disable_web_page_preview=True, reply_markup=reply_markup)
        return

    bot.send_message(
        chat_id,
        html_text,
        parse_mode="HTML",
        disable_web_page_preview=disable_web_page_preview,
        reply_markup=reply_markup,
    )


# ------------------------------------------------------------
# Buttons parsing
# ------------------------------------------------------------

MAX_ROWS = 10
MAX_TOTAL_BTNS = 30
MAX_PER_ROW = 3  # твоя логика


class ButtonSyntaxError(ValueError):
    def __init__(self, line_no: int, problem: str, details: str = ""):
        self.line_no = int(line_no or 0)
        self.problem = str(problem or "other")
        self.details = str(details or "").strip()
        super().__init__(self.details or self.problem)


def _format_button_syntax_error(err: ButtonSyntaxError) -> str:
    line_no = int(getattr(err, "line_no", 0) or 0)
    problem = str(getattr(err, "problem", "other") or "other")
    details = str(getattr(err, "details", "") or "").strip()

    if problem == "format":
        base = "Неправильный формат"
    elif problem == "url":
        base = "Неправильная ссылка"
    else:
        base = "Другая проблема"

    if line_no > 0:
        prefix = f"<b>Строка {line_no}:</b> "
    else:
        prefix = "<b>Ошибка:</b> "

    if details:
        return f"{prefix}{base}. {details}"
    return f"{prefix}{base}."


def _normalize_url(raw: str) -> str:
    u = (raw or "").strip()
    if not u:
        return u
    # если нет схемы — добавим https://
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", u):
        u = "https://" + u
    return u


def _is_supported_button_url(url: str) -> bool:
    value = (url or "").strip()
    if not value or re.search(r"\s", value):
        return False

    if re.match(r"^tg://", value, flags=re.I):
        return True

    if not re.match(r"^https?://", value, flags=re.I):
        return False

    host = re.sub(r"^https?://", "", value, flags=re.I).split("/", 1)[0].strip()
    if not host:
        return False

    return "." in host or host.lower() == "localhost"


def _button_syntax_error(line_no: int, problem: str, details: str = "") -> ButtonSyntaxError:
    return ButtonSyntaxError(line_no=line_no, problem=problem, details=details)


def _sanitize_button_for_payload(button: Any, popups: List[str]) -> Optional[dict]:
    if not isinstance(button, dict):
        return None

    btn_type = str(button.get("type") or "").strip().lower()
    if btn_type not in {"url", "popup", "rules", "del"}:
        return None

    icon_eid = button.get("icon_emoji_id")
    text = str(button.get("text") or "").strip()
    if not text and not icon_eid:
        return None
    if not text:
        text = " "

    style = button.get("style")
    if style not in {None, "danger", "success", "primary"}:
        style = None

    normalized = {
        "type": btn_type,
        "text": text,
        "style": style,
        "icon_emoji_id": str(icon_eid) if icon_eid else None,
    }

    if btn_type == "url":
        url = _normalize_url(str(button.get("url") or ""))
        if not _is_supported_button_url(url):
            return None
        normalized["url"] = url
        return normalized

    if btn_type == "popup":
        try:
            idx = int(button.get("popup_index"))
        except Exception:
            return None

        if idx < 0 or idx >= len(popups):
            return None

        popup_text = str(popups[idx] or "").strip()
        if not popup_text:
            return None

        normalized["popup_index"] = idx
        return normalized

    return normalized


def _extract_button_icon_custom_emoji_id(label: str) -> Tuple[str, Optional[str]]:
    """
    Поддержка твоего кастома для премиум-эмодзи в начале:
      <emoji id='123'>😀</> Текст
    -> icon_custom_emoji_id=123, label="Текст"

    Если эмодзи не в начале — считаем обычным текстом (не пытаемся магичить).
    """
    s = (label or "").strip()
    m = re.match(r"^\s*<emoji\s+id=['\"](\d+)['\"]>\s*.*?\s*</>\s*", s, flags=re.I | re.S)
    if not m:
        return s, None
    eid = m.group(1)
    rest = re.sub(r"^\s*<emoji\s+id=['\"]\d+['\"]>\s*.*?\s*</>\s*", "", s, flags=re.I | re.S).strip()
    return (rest if rest else " "), eid


def _find_custom_emoji_entity_at_offset(entities: list, offset_u: int):
    """Ищем custom_emoji entity, который начинается ровно на offset_u."""
    if not entities:
        return None
    for e in entities:
        try:
            et = (getattr(e, "type", "") or "").lower()
            if et != "custom_emoji":
                continue
            off = int(getattr(e, "offset", 0) or 0)
            ln = int(getattr(e, "length", 0) or 0)
            if off != offset_u or ln <= 0:
                continue
            ce_id = getattr(e, "custom_emoji_id", None)
            if not ce_id:
                continue
            return ln, str(ce_id)
        except Exception:
            continue
    return None


def parse_buttons_text(user_text: str, entities: Optional[list] = None) -> Tuple[List[List[dict]], List[str]]:
    """
    Формат:
      #r Название - example.com & #g Название - popup: текст
      Название - rules
      Название - del

    Возвращает:
      rows: [[btn, btn], [btn]]
      popups: ["текст", ...]
    btn dict:
      {"type":"url|popup|rules|del", "text":"...", "style":"danger|success|primary|None", "url": "...", "popup_index": int, "icon_emoji_id": "..."}

    FIX #2:
    если в названии кнопки стоит premium/custom emoji БЕЗ нашего <emoji id='...'>,
    то Telegram присылает entity type=custom_emoji. Мы забираем custom_emoji_id и
    ставим как icon_custom_emoji_id для кнопки.
    """
    original = user_text or ""
    text = original.strip()
    if not text:
        return [], []

    if len(original) > 6000:
        raise _button_syntax_error(0, "other", "Слишком длинный текст кнопок.")

    has_custom_emoji_entities = False
    if entities:
        for e in entities:
            try:
                if (getattr(e, "type", "") or "").lower() == "custom_emoji":
                    has_custom_emoji_entities = True
                    break
            except Exception:
                continue

    # Сопоставление offset'ов entities (UTF-16) нужно только при custom_emoji.
    original_u = ""
    if has_custom_emoji_entities:
        original_u = "".join(chr(u) for u in _utf16_units(original))

    lines = [ln.strip() for ln in original.splitlines() if ln.strip()]
    rows: List[List[dict]] = []
    popups: List[str] = []

    search_pos_u = 0  # глобальный указатель по original_u

    def parse_one(token: str, token_start_u: int, line_no: int) -> dict:
        tok = token.strip()

        style = None
        prefix_units = 0

        # цвет для КАЖДОЙ кнопки отдельно (фикс бага)
        mcol = re.match(r"^(#r|#g|#b)(\s+)(.*)$", tok, flags=re.I | re.S)
        if mcol:
            col = (mcol.group(1) or "").lower()
            spaces = mcol.group(2) or " "
            rest = (mcol.group(3) or "").strip()
            prefix_units = _utf16_len(mcol.group(1) + spaces)
            tok = rest
            if col == "#r":
                style = "danger"
            elif col == "#g":
                style = "success"
            elif col == "#b":
                style = "primary"

        # name/value
        if " - " not in tok:
            raise _button_syntax_error(
                line_no,
                "format",
                "Используйте формат «Название - ссылка», «Название - popup: текст», «Название - rules» или «Название - del»."
            )

        name_raw, value = tok.split(" - ", 1)

        name_start_u = 0
        name_end_u = 0
        if has_custom_emoji_entities:
            # offsets для имени (в исходном сообщении)
            name_raw_start_u = token_start_u + prefix_units
            name_raw_end_u = name_raw_start_u + _utf16_len(name_raw)

            # strip для имени
            name_lead = name_raw[:len(name_raw) - len(name_raw.lstrip())]
            name_trail = name_raw[len(name_raw.rstrip()):]
            lead_u = _utf16_len(name_lead)
            trail_u = _utf16_len(name_trail)

            name_start_u = name_raw_start_u + lead_u
            name_end_u = name_raw_end_u - trail_u

        name = name_raw.strip()
        value = (value or "").strip()

        # 1) сначала наш кастом <emoji id='...'>
        name, icon_eid = _extract_button_icon_custom_emoji_id(name)

        # 2) если нет кастома — пробуем entity custom_emoji в начале названия
        if not icon_eid and has_custom_emoji_entities and entities:
            found = _find_custom_emoji_entity_at_offset(entities, name_start_u)
            if found and name_end_u > name_start_u:
                ln_u, ce_id = found
                new_name = _remove_utf16_range(name, 0, ln_u).strip()
                name = (new_name if new_name else " ")
                icon_eid = ce_id

        if not name.strip() and not icon_eid:
            raise _button_syntax_error(line_no, "format", "У кнопки отсутствует название.")

        if not name.strip():
            name = " "

        if not value:
            raise _button_syntax_error(line_no, "format", "После « - » нужно указать ссылку, popup, rules или del.")

        vlow = (value or "").lower()
        if vlow == "rules":
            return {"type": "rules", "text": name, "style": style, "icon_emoji_id": icon_eid}
        if vlow == "del":
            return {"type": "del", "text": name, "style": style, "icon_emoji_id": icon_eid}

        if vlow.startswith("popup:"):
            popup_text = value[len("popup:"):].strip()
            if not popup_text:
                raise _button_syntax_error(line_no, "format", "Для popup укажите текст после «popup:».")
            popups.append(popup_text)
            idx = len(popups) - 1
            return {"type": "popup", "text": name, "style": style, "popup_index": idx, "icon_emoji_id": icon_eid}

        # url
        url = _normalize_url(value)
        if not _is_supported_button_url(url):
            raise _button_syntax_error(line_no, "url", "Поддерживаются http(s) и tg:// ссылки без пробелов.")
        return {"type": "url", "text": name, "style": style, "url": url, "icon_emoji_id": icon_eid}

    for line_no, ln in enumerate(lines, start=1):
        line_start_u = None
        if has_custom_emoji_entities:
            ln_u = "".join(chr(u) for u in _utf16_units(ln))
            line_start_u = original_u.find(ln_u, search_pos_u)
            if line_start_u == -1:
                line_start_u = None
            else:
                search_pos_u = line_start_u + len(ln_u)

        parts = [p.strip() for p in ln.split("&") if p.strip()]
        if not parts:
            continue
        if len(parts) > MAX_PER_ROW:
            raise _button_syntax_error(line_no, "format", f"В одном ряду можно использовать не больше {MAX_PER_ROW} кнопок.")

        row: List[dict] = []
        line_seek_u = line_start_u if line_start_u is not None else None

        for p in parts:
            tok = p.strip()
            token_start_u = 0
            if has_custom_emoji_entities:
                tok_u = "".join(chr(u) for u in _utf16_units(tok))

                if line_seek_u is not None:
                    token_start_u = original_u.find(tok_u, line_seek_u)
                    if token_start_u == -1:
                        token_start_u = line_seek_u
                    else:
                        line_seek_u = token_start_u + len(tok_u)

            row.append(parse_one(tok, token_start_u, line_no))

        rows.append(row)
        if len(rows) >= MAX_ROWS:
            remaining_rows = lines[line_no:]
            if any(item.strip() for item in remaining_rows):
                raise _button_syntax_error(line_no + 1, "other", f"Допустимо не больше {MAX_ROWS} рядов кнопок.")

    # total limit
    flat = sum(len(r) for r in rows)
    if flat > MAX_TOTAL_BTNS:
        raise _button_syntax_error(0, "other", f"Допустимо не больше {MAX_TOTAL_BTNS} кнопок в одном наборе.")

    return rows, popups


def build_inline_keyboard_for_payload(section_name: str, chat_id: int, rows: List[List[dict]], popups: List[str], viewer_user_id: int) -> Optional[InlineKeyboardMarkup]:
    """
    section_name: welcome/farewell/rules — чтобы callback различать
    viewer_user_id: кто имеет право нажимать rules/del/popup
    """
    if not rows:
        return None

    kb = InlineKeyboardMarkup(row_width=MAX_PER_ROW)
    for r in rows:
        btns = []
        for b in (r or [])[:MAX_PER_ROW]:
            b = _sanitize_button_for_payload(b, popups)
            if not b:
                continue

            text = b.get("text") or " "
            btn = None

            if b["type"] == "url":
                btn = InlineKeyboardButton(text, url=b.get("url") or "")
            elif b["type"] == "popup":
                idx = int(b.get("popup_index", 0))
                btn = InlineKeyboardButton(text, callback_data=f"p:{section_name}:{chat_id}:{viewer_user_id}:{idx}")
            elif b["type"] == "rules":
                btn = InlineKeyboardButton(text, callback_data=f"rules:{chat_id}:{viewer_user_id}")
            elif b["type"] == "del":
                btn = InlineKeyboardButton(text, callback_data=f"del:{chat_id}:{viewer_user_id}")

            if not btn:
                continue

            # цвет
            st = b.get("style")
            if st:
                try:
                    btn.style = st
                except Exception:
                    pass

            # премиум-эмодзи как icon_custom_emoji_id (если задан)
            eid = b.get("icon_emoji_id")
            if eid:
                try:
                    btn.icon_custom_emoji_id = str(eid)
                except Exception:
                    pass

            btns.append(btn)

        if btns:
            kb.row(*btns)

    return kb


# ------------------------------------------------------------
# UI helpers: одинаковые для welcome/farewell/rules
# ------------------------------------------------------------

def _section_title(sec: str) -> str:
    return {"welcome": "приветствия", "farewell": "прощания", "rules": "правил"}.get(sec, sec)


def _render_section_preview(chat_id: int, sec: str) -> str:
    st = get_chat_settings(chat_id)
    sc = st.get(sec) or _default_section(False)

    enabled = bool(sc.get("enabled"))
    has_text = bool((sc.get("text_custom") or "").strip())
    has_media = bool(sc.get("media"))
    has_buttons = bool((sc.get("buttons") or {}).get("rows"))

    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    emoji_x = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'

    status = f"{emoji_ok} Включено" if enabled else f"{emoji_x} Выключено"
    text_flag = emoji_ok if has_text else emoji_x
    media_flag = emoji_ok if has_media else emoji_x
    buttons_flag = emoji_ok if has_buttons else emoji_x
    src = (sc.get("source") or "plain").upper()

    return (
        f"{emoji_settings} <b>Настройки {_section_title(sec)}</b>\n\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Текст:</b> {text_flag}\n"
        f"<b>Медиа:</b> {media_flag}\n"
        f"<b>Кнопки:</b> {buttons_flag}\n"
        f"<b>Источник:</b> <code>{_html.escape(src)}</code>"
    )


def _render_cleanup_main(chat_id: int) -> str:
    cl = _cleanup_get(chat_id)
    cmds = cl.get("commands") or {}
    sysd = cl.get("system") or {}

    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    emoji_x = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'

    enabled_cmds = [s for s in CLEANUP_CMD_SIGNS if cmds.get(s)]
    enabled_cmds_txt = " ".join(enabled_cmds) if enabled_cmds else "нет"

    enabled_sys = [ct for ct in CLEANUP_SYSTEM_TYPES_ORDER if sysd.get(ct)]
    enabled_sys_txt = str(len(enabled_sys))

    can_del = _bot_can_delete_messages(chat_id)
    rights = f"{emoji_ok} Есть" if can_del else f"{emoji_x} Нет"

    warn = ""
    if not can_del:
        warn = (
            "\n\n<blockquote expandable=\"true\">"
            "<b>Важно:</b> у бота нет права <b>Удалять сообщения</b> в этом чате.\n"
            "Пока право не выдано, функции удаления работать не будут."
            "</blockquote>"
        )

    return (
        f"{emoji_settings} <b>Удаление сообщений</b>\n\n"
        f"<b>Права бота на удаление:</b> {rights}\n"
        f"<b>Команды (по префиксу):</b> <code>{_html.escape(enabled_cmds_txt)}</code>\n"
        f"<b>Системные сообщения:</b> <code>{_html.escape(enabled_sys_txt)}</code> включено\n"
        f"{warn}"
    )


def _build_cleanup_main_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)

    kb.add(InlineKeyboardButton("Команды", callback_data=f"st_cleanup_cmds:{chat_id}"))
    kb.add(InlineKeyboardButton("Системные сообщения", callback_data=f"st_cleanup_sys:{chat_id}"))

    btn_back = InlineKeyboardButton("Назад", callback_data=f"st_back_main:{chat_id}")
    try:
        btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        btn_back.style = "primary"
    except Exception:
        pass
    kb.add(btn_back)
    return kb


def _render_cleanup_commands(chat_id: int) -> str:
    cl = _cleanup_get(chat_id)
    cmds = cl.get("commands") or {}
    enabled = [s for s in CLEANUP_CMD_SIGNS if cmds.get(s)]
    enabled_txt = " ".join(enabled) if enabled else "нет"

    emoji = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">🧹</tg-emoji>'
    return (
        f"{emoji} <b>Удаление команд</b>\n\n"
        "Удаляет <b>только</b> сообщения, которые <b>начинаются</b> с выбранного знака.\n"
        "Напр.: <code>/cmd ...</code> — удалит, а <code>текст /cmd</code> — нет.\n\n"
        f"<b>Включены:</b> <code>{_html.escape(enabled_txt)}</code>"
    )


def _btn_style_pair(is_enabled: bool) -> tuple[str, str]:
    # включено -> ON зелёная, OFF красная
    # выключено -> ON красная, OFF зелёная
    return ("success", "danger") if is_enabled else ("danger", "success")


def _build_cleanup_commands_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    cl = _cleanup_get(chat_id)
    cmds = cl.get("commands") or {}

    kb = InlineKeyboardMarkup(row_width=3)
    inv = "\u2063"  # “пустой символ”, чтобы была видна только иконка

    for sign in CLEANUP_CMD_SIGNS:
        is_on = bool(cmds.get(sign))
        on_style, off_style = _btn_style_pair(is_on)

        lbl = InlineKeyboardButton(sign, callback_data=f"st_cleanup_cmdnoop:{chat_id}:{sign}")
        try:
            lbl.style = "primary"
        except Exception:
            pass

        b_on = InlineKeyboardButton(inv, callback_data=f"st_cleanup_cmdset:{chat_id}:{sign}:1")
        b_off = InlineKeyboardButton(inv, callback_data=f"st_cleanup_cmdset:{chat_id}:{sign}:0")

        try:
            b_on.icon_custom_emoji_id = str(CLEANUP_ICON_ENABLE_ID)
            b_off.icon_custom_emoji_id = str(CLEANUP_ICON_DISABLE_ID)
        except Exception:
            pass

        try:
            b_on.style = on_style
            b_off.style = off_style
        except Exception:
            pass

        kb.row(lbl, b_on, b_off)

    btn_back = InlineKeyboardButton("Назад", callback_data=f"st_main:{chat_id}:cleanup")
    try:
        btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        btn_back.style = "primary"
    except Exception:
        pass
    kb.add(btn_back)

    return kb


def _render_cleanup_system(chat_id: int) -> str:
    cl = _cleanup_get(chat_id)
    sysd = cl.get("system") or {}
    enabled = [ct for ct in CLEANUP_SYSTEM_TYPES_ORDER if sysd.get(ct)]

    emoji = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">🧽</tg-emoji>'
    return (
        f"{emoji} <b>Удаление системных сообщений</b>\n\n"
        "Нажмите на тип системного сообщения — появятся кнопки ВКЛ/ВЫКЛ под ним.\n"
        f"<b>Включено типов:</b> <code>{len(enabled)}</code>\n"
        "<i>Если у бота нет права “Удалять сообщения”, удаление работать не будет.</i>"
    )


def _build_cleanup_system_keyboard(chat_id: int, selected_idx: Optional[int] = None) -> InlineKeyboardMarkup:
    cl = _cleanup_get(chat_id)
    sysd = cl.get("system") or {}

    kb = InlineKeyboardMarkup(row_width=2)
    inv = "\u2063"  # “пустой символ”, чтобы была видна только иконка

    for idx, ct in enumerate(CLEANUP_SYSTEM_TYPES_ORDER):
        label = CLEANUP_SYSTEM_LABELS.get(ct, ct)

        is_selected = (selected_idx == idx)
        title = f"»{label}«" if is_selected else label

                # 1) строка с названием типа
        btn_type = InlineKeyboardButton(title[:48], callback_data=f"st_cleanup_syspick:{chat_id}:{idx}")

        # ✅ без цвета по умолчанию, primary только для выбранного
        if is_selected:
            try:
                btn_type.style = "primary"
            except Exception:
                pass

        kb.row(btn_type)
        
        # 2) если выбран — добавляем строку ВКЛ/ВЫКЛ под ним
        if is_selected:
            is_on = bool(sysd.get(ct))
            on_style, off_style = _btn_style_pair(is_on)

            b_on = InlineKeyboardButton(inv, callback_data=f"st_cleanup_sysset:{chat_id}:{idx}:1")
            b_off = InlineKeyboardButton(inv, callback_data=f"st_cleanup_sysset:{chat_id}:{idx}:0")

            try:
                b_on.icon_custom_emoji_id = str(CLEANUP_ICON_ENABLE_ID)
                b_off.icon_custom_emoji_id = str(CLEANUP_ICON_DISABLE_ID)
            except Exception:
                pass

            try:
                b_on.style = on_style
                b_off.style = off_style
            except Exception:
                pass

            kb.row(b_on, b_off)

    btn_back = InlineKeyboardButton("Назад", callback_data=f"st_main:{chat_id}:cleanup")
    try:
        btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        btn_back.style = "primary"
    except Exception:
        pass
    kb.add(btn_back)

    return kb


def _warn_type_label(ptype: str) -> str:
    return {
        "mute": "Ограничение",
        "ban": "Блокировка",
        "kick": "Исключение",
    }.get((ptype or "").lower(), "Ограничение")


def _render_warn_settings(chat_id: int, page: str = "main") -> str:
    ch = _mod_get_chat(chat_id)
    settings = ch.get("settings") or {}

    enabled = bool(settings.get("warn_enabled", True))
    warn_limit = int(settings.get("warn_limit") or 3)
    wp = settings.get("warn_punish") or {}
    ptype = (wp.get("type") or "mute").lower()
    duration = wp.get("duration")

    type_label = _warn_type_label(ptype)
    dur_label = "Не используется" if ptype == "kick" else _mod_duration_text(int(duration or 0))

    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    emoji_x = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'

    status = f"{emoji_ok} Включено" if enabled else f"{emoji_x} Выключено"
    if page == "punish":
        return (
            f"{emoji_settings} <b>Наказание за предупреждения</b>\n\n"
            f"<b>Текущий тип:</b> <code>{_html.escape(type_label)}</code>\n"
            f"<b>Текущая длительность:</b> <code>{_html.escape(dur_label)}</code>\n\n"
            "Выберите тип наказания и длительность ниже:"
        )

    return (
        f"{emoji_settings} <b>Настройки предупреждений</b>\n\n"
        f"<b>Статус:</b> {status}\n"
        f"<b>Макс. предупреждений:</b> <code>{warn_limit}</code>\n"
        f"<b>Наказание за максимум:</b> <code>{_html.escape(type_label)}</code>\n"
        f"<b>Длительность наказания:</b> <code>{_html.escape(dur_label)}</code>\n\n"
        "Ниже можно быстро задать лимит от 2 до 10."
    )


def _build_warn_settings_keyboard(chat_id: int, page: str = "main") -> InlineKeyboardMarkup:
    ch = _mod_get_chat(chat_id)
    settings = ch.get("settings") or {}

    enabled = bool(settings.get("warn_enabled", True))
    warn_limit = int(settings.get("warn_limit") or 3)
    wp = settings.get("warn_punish") or {}
    ptype = (wp.get("type") or "mute").lower()
    duration = int(wp.get("duration") or 24 * 60 * 60)

    kb = InlineKeyboardMarkup(row_width=5)

    if page == "punish":
        btn_mute = InlineKeyboardButton("Ограничение", callback_data=f"st_warn_ptype:{chat_id}:mute")
        btn_ban = InlineKeyboardButton("Блокировка", callback_data=f"st_warn_ptype:{chat_id}:ban")
        btn_kick = InlineKeyboardButton("Кик", callback_data=f"st_warn_ptype:{chat_id}:kick")
        for btn, key in ((btn_mute, "mute"), (btn_ban, "ban"), (btn_kick, "kick")):
            try:
                btn.style = "primary" if ptype == key else "secondary"
            except Exception:
                pass
        kb.row(btn_mute, btn_ban, btn_kick)

        if ptype in ("mute", "ban"):
            presets = [
                (60 * 60, "1ч"),
                (6 * 60 * 60, "6ч"),
                (12 * 60 * 60, "12ч"),
                (24 * 60 * 60, "1д"),
                (3 * 24 * 60 * 60, "3д"),
                (7 * 24 * 60 * 60, "7д"),
                (30 * 24 * 60 * 60, "30д"),
            ]
            row = []
            for sec, label in presets:
                b = InlineKeyboardButton(label, callback_data=f"st_warn_dur:{chat_id}:{sec}")
                try:
                    b.style = "primary" if duration == sec else "secondary"
                except Exception:
                    pass
                row.append(b)

            for i in range(0, len(row), 4):
                kb.row(*row[i:i + 4])

        btn_back_warn = InlineKeyboardButton("Назад", callback_data=f"st_warn_page:{chat_id}:main")
        try:
            btn_back_warn.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
            btn_back_warn.style = "primary"
        except Exception:
            pass
        kb.add(btn_back_warn)
        return kb

    btn_status = InlineKeyboardButton("Статус", callback_data=f"st_warn_toggle:{chat_id}")
    try:
        btn_status.style = "success" if enabled else "danger"
    except Exception:
        pass
    kb.add(btn_status)

    btn_punish = InlineKeyboardButton("Наказание", callback_data=f"st_warn_page:{chat_id}:punish")
    try:
        btn_punish.style = "primary"
    except Exception:
        pass
    kb.add(btn_punish)

    number_buttons: list[InlineKeyboardButton] = []
    for n in range(2, 11):
        btn = InlineKeyboardButton(str(n), callback_data=f"st_warn_setlimit:{chat_id}:{n}")
        try:
            btn.style = "primary" if warn_limit == n else "secondary"
        except Exception:
            pass
        number_buttons.append(btn)

    for i in range(0, len(number_buttons), 5):
        kb.row(*number_buttons[i:i + 5])

    btn_back = InlineKeyboardButton("Назад", callback_data=f"st_back_main:{chat_id}")
    try:
        btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        btn_back.style = "primary"
    except Exception:
        pass
    kb.add(btn_back)

    return kb

def _build_settings_main_keyboard(chat_id: int, viewer_user: types.User | None = None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)

    btn_welcome = InlineKeyboardButton("Приветствие", callback_data=f"st_main:{chat_id}:welcome")
    try:
        btn_welcome.icon_custom_emoji_id = "5472055112702629499"
    except Exception:
        pass

    btn_farewell = InlineKeyboardButton("Прощание", callback_data=f"st_main:{chat_id}:farewell")
    try:
        btn_farewell.icon_custom_emoji_id = "5370867268051806190"
    except Exception:
        pass

    btn_rules = InlineKeyboardButton("Правила", callback_data=f"st_main:{chat_id}:rules")
    try:
        btn_rules.icon_custom_emoji_id = "5226512880362332956"
    except Exception:
        pass

    btn_cleanup = InlineKeyboardButton("Удаление сообщений", callback_data=f"st_main:{chat_id}:cleanup")
    try:
        btn_cleanup.icon_custom_emoji_id = "5229113891081956317"
    except Exception:
        pass

    btn_warns = InlineKeyboardButton("Предупреждения", callback_data=f"stw:open:{chat_id}")
    try:
        btn_warns.icon_custom_emoji_id = "5467928559664242360"
    except Exception:
        pass

    can_manage_roles = bool(viewer_user and _user_can_edit_now(viewer_user, chat_id))
    btn_roles = InlineKeyboardButton("Права ролей", callback_data=f"st_main:{chat_id}:roles")
    try:
        btn_roles.icon_custom_emoji_id = str(EMOJI_ADMIN_RIGHTS_ID)
    except Exception:
        pass

    kb.add(btn_welcome, btn_farewell)
    kb.add(btn_rules, btn_cleanup)
    if can_manage_roles:
        kb.add(btn_warns, btn_roles)
    else:
        kb.add(btn_warns)

    btn_close = InlineKeyboardButton("Закрыть", callback_data=f"st_close:{chat_id}")
    try:
        btn_close.icon_custom_emoji_id = str(PREMIUM_CLOSE_EMOJI_ID)
    except Exception:
        pass
    kb.add(btn_close)

    return kb

def _build_section_keyboard(chat_id: int, sec: str) -> InlineKeyboardMarkup:
    st = get_chat_settings(chat_id)
    sc = st.get(sec) or _default_section(False)
    enabled = bool(sc.get("enabled"))

    kb = InlineKeyboardMarkup(row_width=2)

    btn_status = InlineKeyboardButton("Статус", callback_data=f"st_{sec}_toggle:{chat_id}")
    try:
        btn_status.style = "success" if enabled else "danger"
    except Exception:
        pass
    kb.add(btn_status)

    btn_show = InlineKeyboardButton("Показать текущее", callback_data=f"st_{sec}_show:{chat_id}")
    try:
        btn_show.style = "primary"
    except Exception:
        pass
    kb.add(btn_show)

    btn_text = InlineKeyboardButton("Текст", callback_data=f"st_{sec}_text:{chat_id}")
    btn_text.icon_custom_emoji_id = EMOJI_WELCOME_TEXT_ID

    btn_media = InlineKeyboardButton("Медиа", callback_data=f"st_{sec}_media:{chat_id}")
    btn_media.icon_custom_emoji_id = EMOJI_WELCOME_MEDIA_ID

    kb.add(btn_text, btn_media)

    btn_buttons = InlineKeyboardButton("Кнопки", callback_data=f"st_{sec}_buttons:{chat_id}")
    btn_buttons.icon_custom_emoji_id = EMOJI_WELCOME_BUTTONS_ID
    kb.add(btn_buttons)

    btn_back = InlineKeyboardButton("Назад", callback_data=f"st_back_main:{chat_id}")
    btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
    try:
        btn_back.style = "primary"
    except Exception:
        pass
    kb.add(btn_back)

    return kb


def _only_back_kb(chat_id: int, sec: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    btn_back = InlineKeyboardButton("Назад", callback_data=f"st_main:{chat_id}:{sec}")
    btn_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
    try:
        btn_back.style = "primary"
    except Exception:
        pass
    kb.add(btn_back)
    return kb


# ------------------------------------------------------------
# /settings
# ------------------------------------------------------------

def _find_settings_groups_for_user(user: types.User) -> list[tuple[int, str]]:
    """
    Возвращает список (chat_id, title) подтверждённых групп,
    в которых пользователь имеет право открывать /settings.
    """
    # Собираем все известные chat_id из БД
    chat_ids: set[int] = set()
    for cid_str in (USERS or {}):
        try:
            chat_ids.add(int(cid_str))
        except ValueError:
            pass
    for cid_str in (CHAT_SETTINGS or {}):
        try:
            chat_ids.add(int(cid_str))
        except ValueError:
            pass
    for cid_str in (GROUP_SETTINGS or {}):
        try:
            chat_ids.add(int(cid_str))
        except ValueError:
            pass

    result: list[tuple[int, str]] = []
    for chat_id in chat_ids:
        if not is_group_approved(chat_id):
            continue
        allowed, _ = _user_can_open_settings(chat_id, user)
        if not allowed:
            continue
        try:
            chat = bot.get_chat(chat_id)
            title = chat.title or str(chat_id)
        except Exception:
            title = str(chat_id)
        result.append((chat_id, title))
    # Сортируем по названию для удобства
    result.sort(key=lambda x: x[1].lower())
    return result


@bot.message_handler(func=lambda m: match_command(m.text, 'settings'))
def cmd_settings(m: types.Message):
    add_stat_message(m)
    add_stat_command('settings')

    wait_seconds = cooldown_hit('user', int(m.from_user.id), 'settings', 5)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='user', bucket=int(m.from_user.id), action='settings')

    if m.chat.type == 'private':
        user = m.from_user
        groups = _find_settings_groups_for_user(user)
        emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
        if not groups:
            return bot.reply_to(
                m,
                f"{emoji_settings} <b>Нет доступных групп</b>\n\n"
                "У вас нет права изменения настроек ни в одной из групп.",
                parse_mode='HTML',
                disable_web_page_preview=True
            )

        text = (
            f"{emoji_settings} <b>Настройки групп</b>\n\n"
            "Выберите группу для настройки:"
        )
        kb = InlineKeyboardMarkup()
        for cid, title in groups:
            kb.add(InlineKeyboardButton(
                f"{title}",
                callback_data=f"pm_settings_open:{cid}",
            ))
        return bot.reply_to(m, text, parse_mode='HTML', disable_web_page_preview=True, reply_markup=kb)

    if m.chat.type not in ['group', 'supergroup']:
        return

    # Проверка одобрения группы
    if not check_group_approval(m):
        return

    chat_id = m.chat.id
    user = m.from_user

    allowed, err = _user_can_open_settings(chat_id, user)
    if not allowed:
        if err:
            return bot.reply_to(m, premium_prefix(err), parse_mode='HTML', disable_web_page_preview=True)
        return

    get_chat_settings(chat_id)

    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    text = (
        f"{emoji_settings} <b>Настройки чата</b>\n"
        f"Чат: {m.chat.title or chat_id}\n\n"
        "Выберите раздел для настройки:"
    )

    kb = _build_settings_main_keyboard(chat_id, viewer_user=user)

    try:
        bot.send_message(user.id, text, parse_mode='HTML', disable_web_page_preview=True, reply_markup=kb)
    except Exception:
        return bot.reply_to(
            m,
            premium_prefix("Не удалось отправить интерфейс в ЛС. Напишите боту в ЛС и попробуйте снова."),
            parse_mode='HTML',
            disable_web_page_preview=True
        )

    bot.reply_to(
        m,
        "<i>Настройки отправлены в ЛС.</i>",
        parse_mode='HTML',
        disable_web_page_preview=True,
        reply_markup=_build_open_pm_markup(),
    )


# ------------------------------------------------------------
# Callbacks: settings + section UI
# ------------------------------------------------------------

def _is_warn_settings_callback_data(data: str) -> bool:
    return bool(data) and data.startswith("stw:")


def _render_warn_settings_local(chat_id: int, page: str = "main") -> str:
    ch = _mod_get_chat(chat_id)
    settings = ch.get("settings") or {}
    enabled = bool(settings.get("warn_enabled", True))
    warn_limit = int(settings.get("warn_limit") or 3)
    wp = settings.get("warn_punish") or {}
    ptype = (wp.get("type") or "mute").lower()
    duration = wp.get("duration")

    type_label = _warn_type_label(ptype)
    dur_label = "Не используется" if ptype == "kick" else _mod_duration_text(int(duration or 0))
    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    emoji_ok = f'<tg-emoji emoji-id="{EMOJI_UNPUNISH_ID}">✅</tg-emoji>'
    emoji_x = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
    status_line = f"{emoji_ok} Включён" if enabled else f"{emoji_x} Выключен"

    hint = ""
    if page == "count":
        hint = "\n\n<i>Выберите максимальное количество предупреждений.</i>"
    elif page == "punish":
        hint = "\n\n<i>Выберите наказание, которое будет применяться при достижении максимального количества предупреждений.</i>"
    elif page == "duration":
        if ptype == "kick":
            hint = "\n\nДля наказания «Кик» длительность не устанавливается."
        else:
            hint = "\n\n<i>Установите время наказания.</i>"

    return (
        f"{emoji_settings} <b>Настройки предупреждений</b>\n\n"
        f"<b>Статус:</b> {status_line}\n"
        f"<b>Максимальное количество:</b> <code>{warn_limit}</code>\n"
        f"<b>Наказание:</b> <code>{_html.escape(type_label)}</code>\n"
        f"<b>Длительность:</b> <code>{_html.escape(dur_label)}</code>"
        f"{hint}"
    )


def _build_warn_settings_keyboard_local(chat_id: int, page: str = "main") -> InlineKeyboardMarkup:
    ch = _mod_get_chat(chat_id)
    settings = ch.get("settings") or {}
    enabled = bool(settings.get("warn_enabled", True))
    warn_limit = int(settings.get("warn_limit") or 3)
    wp = settings.get("warn_punish") or {}
    ptype = (wp.get("type") or "mute").lower()
    duration = int(wp.get("duration") or 24 * 60 * 60)

    kb = InlineKeyboardMarkup(row_width=3)

    b_status = InlineKeyboardButton("Статус", callback_data=f"stw:toggle:{chat_id}")
    try:
        b_status.icon_custom_emoji_id = str(EMOJI_UNPUNISH_ID if enabled else EMOJI_ROLE_SETTINGS_CANCEL_ID)
        b_status.style = "success" if enabled else "danger"
    except Exception:
        pass
    kb.add(b_status)

    nav_defs = [
        ("count", "Количество"),
        ("punish", "Наказание"),
        ("duration", "Длительность"),
    ]
    for key, title in nav_defs:
        is_selected = (page == key)
        btn_title = f"»{title}«" if is_selected else title
        b_nav = InlineKeyboardButton(btn_title, callback_data=f"stw:page:{chat_id}:{key}")
        try:
            if is_selected:
                b_nav.style = "primary"
        except Exception:
            pass
        kb.add(b_nav)

        if not is_selected:
            continue

        if key == "count":
            nums: list[InlineKeyboardButton] = []
            for n in range(2, 11):
                b = InlineKeyboardButton(str(n), callback_data=f"stw:limit:{chat_id}:{n}")
                try:
                    if warn_limit == n:
                        b.style = "primary"
                except Exception:
                    pass
                nums.append(b)
            for i in range(0, len(nums), 5):
                kb.row(*nums[i:i + 5])

        if key == "punish":
            b_mute = InlineKeyboardButton("Ограничение", callback_data=f"stw:ptype:{chat_id}:mute")
            b_ban = InlineKeyboardButton("Блокировка", callback_data=f"stw:ptype:{chat_id}:ban")
            b_kick = InlineKeyboardButton("Кик", callback_data=f"stw:ptype:{chat_id}:kick")
            for btn, p_key in ((b_mute, "mute"), (b_ban, "ban"), (b_kick, "kick")):
                try:
                    if ptype == p_key:
                        btn.style = "primary"
                except Exception:
                    pass
            kb.row(b_mute, b_ban, b_kick)

        if key == "duration" and ptype in ("mute", "ban"):
            b_set = InlineKeyboardButton("Установить время", callback_data=f"stw:dur_prompt:{chat_id}")
            kb.add(b_set)

    b_back = InlineKeyboardButton("Назад", callback_data=f"st_back_main:{chat_id}")
    try:
        b_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        b_back.style = "primary"
    except Exception:
        pass
    kb.add(b_back)
    return kb


def _clone_inline_kb_plain(kb: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    plain = InlineKeyboardMarkup(row_width=5)
    try:
        for row in (kb.keyboard or []):
            new_row: list[InlineKeyboardButton] = []
            for btn in row:
                text = getattr(btn, "text", "") or "-"
                cb = getattr(btn, "callback_data", None)
                url = getattr(btn, "url", None)
                if cb is not None:
                    new_row.append(InlineKeyboardButton(text, callback_data=cb))
                elif url is not None:
                    new_row.append(InlineKeyboardButton(text, url=url))
                else:
                    new_row.append(InlineKeyboardButton(text, callback_data="stw:noop:0"))
            if new_row:
                plain.row(*new_row)
    except Exception:
        pass
    return plain


def _strip_tg_emoji_tags(text: str) -> str:
    try:
        return _re.sub(r"</?tg-emoji[^>]*>", "", text or "")
    except Exception:
        return text or ""


def _show_warn_settings_ui(pm_chat_id: int, message_id: int, text: str, kb: InlineKeyboardMarkup) -> bool:
    try:
        resp = raw_edit_message_with_keyboard(pm_chat_id, message_id, text, kb)
        if isinstance(resp, dict):
            if resp.get("ok"):
                return True
            desc = str(resp.get("description") or "").lower()
            if "message is not modified" in desc:
                return True
    except Exception:
        pass

    if _safe_edit_message_html(pm_chat_id, message_id, text, kb):
        return True

    try:
        resp = raw_send_with_inline_keyboard(pm_chat_id, text, kb)
        if isinstance(resp, dict) and resp.get("ok"):
            return True
    except Exception:
        pass

    plain_text = _strip_tg_emoji_tags(text)
    plain_kb = _clone_inline_kb_plain(kb)

    # fallback без style/icon_custom_emoji_id и без tg-emoji в тексте
    if _safe_edit_message_html(pm_chat_id, message_id, plain_text, plain_kb):
        return True

    try:
        resp = raw_send_with_inline_keyboard(pm_chat_id, plain_text, plain_kb)
        if isinstance(resp, dict) and resp.get("ok"):
            return True
    except Exception:
        pass

    try:
        bot.send_message(
            pm_chat_id,
            plain_text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=plain_kb,
        )
        return True
    except Exception:
        pass

    try:
        bot.send_message(
            pm_chat_id,
            premium_prefix("Не удалось отрисовать клавиатуру, попробуйте снова /settings."),
            parse_mode='HTML',
            disable_web_page_preview=True,
        )
    except Exception:
        pass

    return False


@bot.callback_query_handler(func=lambda c: _is_warn_settings_callback_data(c.data or ""))
def cb_warn_settings_only(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    data = c.data or ""
    user = c.from_user
    msg_chat = c.message.chat

    if msg_chat.type != 'private':
        bot.answer_callback_query(c.id)
        return

    parts = data.split(":", 3)
    if len(parts) < 3:
        bot.answer_callback_query(c.id)
        return

    _, action, chat_id_s, extra = (parts + [""])[:4]
    try:
        chat_id = int(chat_id_s)
    except ValueError:
        bot.answer_callback_query(c.id)
        return

    allowed, err = _user_can_open_settings(chat_id, user)
    if not allowed:
        bot.answer_callback_query(c.id, err or "Недостаточно прав для этого действия.", show_alert=True)
        return

    ch = _mod_get_chat(chat_id)
    settings = ch.get("settings") or {}
    page = "main"
    should_render = True

    if action != "dur_prompt":
        _pending_pop("pending_warn_duration", user.id)
        _pending_msg_pop("pending_warn_duration_msg", user.id)

    if action == "open":
        page = "main"
    elif action == "noop":
        bot.answer_callback_query(c.id)
        return
    elif action == "toggle":
        settings["warn_enabled"] = not bool(settings.get("warn_enabled", True))
        ch["settings"] = settings
        _mod_save()
    elif action == "limit":
        try:
            value = int(extra)
        except Exception:
            bot.answer_callback_query(c.id)
            return
        value = max(2, min(10, value))
        current = int(settings.get("warn_limit") or 3)
        if value == current:
            bot.answer_callback_query(c.id)
            return
        settings["warn_limit"] = value
        ch["settings"] = settings
        _mod_save()
    elif action == "ptype":
        ptype = (extra or "").strip().lower()
        if ptype in ("mute", "ban", "kick"):
            wp = settings.get("warn_punish") or {}
            wp["type"] = ptype
            if ptype == "kick":
                wp["duration"] = None
            elif wp.get("duration") is None:
                wp["duration"] = 24 * 60 * 60
            settings["warn_punish"] = wp
            ch["settings"] = settings
            _mod_save()
            page = "punish"
    elif action == "dur_prompt":
        wp = settings.get("warn_punish") or {}
        if (wp.get("type") or "mute").lower() not in ("mute", "ban"):
            bot.answer_callback_query(c.id, "Для кика длительность не используется.", show_alert=True)
            return
        _pending_put("pending_warn_duration", user.id, chat_id)
        _delete_pending_ui(msg_chat.id, "pending_warn_duration_msg", user.id, also_msg_id=c.message.message_id)

        kb_prompt = InlineKeyboardMarkup(row_width=1)
        b_cancel = InlineKeyboardButton("Назад", callback_data=f"stw:open:{chat_id}")
        try:
            b_cancel.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
            b_cancel.style = "primary"
        except Exception:
            pass
        kb_prompt.add(b_cancel)

        prompt_text = (
            "<b>Установите время наказания</b>\n\n"
            "<b>Подсказка по интервалам:</b>\n"
            "<code>m</code> - минуты, <code>h</code> - часы, <code>d</code> - дни, <code>w</code> - недели, <code>mou</code> - месяцы, <code>y</code> - годы\n"
            "<code>м</code> - минуты, <code>мин</code> - минуты, <code>ч</code> - часы, <code>д</code> - дни, <code>н</code> - недели, <code>мес</code> - месяцы, <code>г</code> - годы\n"
            "Можно комбинировать до <b>3</b> интервалов.\n\n"
            "<b>Примеры:</b> <code>30m</code>, <code>2h</code>, <code>3д</code>, <code>1н</code>, <code>1h 2m</code>, <code>2mou 1d</code>, <code>навсегда</code>."
        )
        sent = bot.send_message(
            msg_chat.id,
            prompt_text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb_prompt,
        )
        _pending_msg_set("pending_warn_duration_msg", user.id, sent.message_id)
        bot.answer_callback_query(c.id)
        return
    elif action == "page":
        if extra in ("count", "punish", "duration"):
            page = extra
        else:
            page = "main"

    if not should_render:
        bot.answer_callback_query(c.id)
        return

    text = _render_warn_settings_local(chat_id, page=page)
    kb = _build_warn_settings_keyboard_local(chat_id, page=page)
    if not _show_warn_settings_ui(msg_chat.id, c.message.message_id, text, kb):
        bot.answer_callback_query(c.id, "Не удалось открыть раздел предупреждений.", show_alert=True)
        return

    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data and (
    c.data.startswith("st_close:") or
    (c.data.startswith("st_main:") and not c.data.endswith(":warn") and not c.data.endswith(":warns")) or
    c.data.startswith("st_back_main:") or
    c.data.startswith("st_welcome_") or
    c.data.startswith("st_farewell_") or
    c.data.startswith("st_rules_") or
    c.data.startswith("p:") or
    c.data.startswith("rules:") or
    c.data.startswith("del:") or
    c.data.startswith("st_cleanup_")
))
def cb_settings_main(c: types.CallbackQuery):
    if _is_duplicate_callback_query(c):
        return
    data = c.data or ""
    user = c.from_user
    msg_chat = c.message.chat

    # popup/rules/del работают в группах и в ЛС (но доступ по uid)
    if data.startswith("p:"):
        # p:section:chat_id:uid:idx
        try:
            _, sec, chat_id_s, uid_s, idx_s = data.split(":")
            chat_id = int(chat_id_s)
            uid = int(uid_s)
            idx = int(idx_s)
        except Exception:
            bot.answer_callback_query(c.id)
            return

        if user.id != uid:
            bot.answer_callback_query(c.id, "Недоступно.", show_alert=True)
            return

        st = get_chat_settings(chat_id)
        popups = ((st.get(sec) or {}).get("buttons") or {}).get("popups") or []
        txt = popups[idx] if 0 <= idx < len(popups) else "..."
        bot.answer_callback_query(c.id, txt, show_alert=True)
        return

    if data.startswith("rules:"):
        # rules:chat_id:uid
        try:
            _, chat_id_s, uid_s = data.split(":")
            chat_id = int(chat_id_s)
            uid = int(uid_s)
        except Exception:
            bot.answer_callback_query(c.id)
            return
        if user.id != uid:
            bot.answer_callback_query(c.id, "Недоступно.", show_alert=True)
            return

        st = get_chat_settings(chat_id)
        rules = st.get("rules") or _default_section(False)
        html = build_html_from_text_custom(rules.get("text_custom") or "")
        media = rules.get("media") or []
        rows = ((rules.get("buttons") or {}).get("rows")) or []
        popups = ((rules.get("buttons") or {}).get("popups")) or []
        kb = build_inline_keyboard_for_payload("rules", chat_id, rows, popups, uid)

        bot.answer_callback_query(c.id)
        _send_payload(c.message.chat.id, html, media, reply_markup=kb)
        return

    if data.startswith("del:"):
        # del:chat_id:uid
        try:
            _, chat_id_s, uid_s = data.split(":")
            uid = int(uid_s)
        except Exception:
            bot.answer_callback_query(c.id)
            return
        if user.id != uid:
            bot.answer_callback_query(c.id, "Недоступно.", show_alert=True)
            return
        try:
            bot.delete_message(c.message.chat.id, c.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(c.id)
        return

    # settings UI only in private
    if msg_chat.type != 'private':
        bot.answer_callback_query(c.id)
        return

    parts = data.split(":", 2)
    prefix = parts[0]
    if len(parts) < 2:
        bot.answer_callback_query(c.id)
        return

    try:
        chat_id = int(parts[1])
    except ValueError:
        bot.answer_callback_query(c.id)
        return

    allowed, err = _user_can_open_settings(chat_id, user)
    if not allowed:
        bot.answer_callback_query(c.id, err or "Недостаточно прав для этого действия.", show_alert=True)
        return

    # close
    if prefix == "st_close":
        _try_delete_private_prompt(msg_chat.id, c.message.message_id)
        bot.answer_callback_query(c.id)
        return

    # back main
    if prefix == "st_back_main":
        emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
        text = (
            f"{emoji_settings} <b>Настройки чата</b>\n"
            f"Чат ID: <code>{chat_id}</code>\n\n"
            "<b>Выберите раздел для настройки:</b>"
        )
        kb = _build_settings_main_keyboard(chat_id, viewer_user=user)
        edited = _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
        if not edited:
            try:
                bot.send_message(
                    msg_chat.id,
                    text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            except Exception:
                bot.answer_callback_query(c.id, "Не удалось открыть раздел настроек.", show_alert=True)
                return
        bot.answer_callback_query(c.id)
        return

    # main section
    if prefix == "st_main":
        if len(parts) < 3:
            bot.answer_callback_query(c.id)
            return
        sec = parts[2]

        get_chat_settings(chat_id)

        if sec in SECTION_KEYS:
            text = _render_section_preview(chat_id, sec)
            kb = _build_section_keyboard(chat_id, sec)
        elif sec == "cleanup":
            text = _render_cleanup_main(chat_id)
            kb = _build_cleanup_main_keyboard(chat_id)
        elif sec in ("warns", "warn"):
            try:
                text = _render_warn_settings_local(chat_id, page="main")
                kb = _build_warn_settings_keyboard_local(chat_id, page="main")
            except Exception:
                bot.answer_callback_query(c.id, "Не удалось открыть раздел предупреждений.", show_alert=True)
                return
        elif sec == "roles":
            if not _user_can_edit_now(user, chat_id):
                bot.answer_callback_query(c.id, "Недостаточно прав для настройки ролей.", show_alert=True)
                return

            emoji_chat = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHAT_ID}">📋</tg-emoji>'
            emoji_choose = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CHOOSE_RANK_ID}">🔽</tg-emoji>'
            try:
                chat_obj = bot.get_chat(chat_id)
                title = chat_obj.title or str(chat_id)
            except Exception:
                title = str(chat_id)

            text = (
                f"{emoji_chat} <b>Настройка прав должностей для чата</b> "
                f"<b>{_html.escape(title)}</b> (<code>{chat_id}</code>)\n"
                f"{emoji_choose} <b>Выберите должность для настройки прав:</b>"
            )
            kb = _build_ranks_keyboard(chat_id, for_pm=True, back_callback=f"st_back_main:{chat_id}")
        else:
            text = premium_prefix("Неизвестный раздел настроек.")
            kb = _build_settings_main_keyboard(chat_id, viewer_user=user)

        edited = _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
        if not edited:
            try:
                bot.send_message(
                    msg_chat.id,
                    text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            except Exception:
                bot.answer_callback_query(c.id, "Не удалось открыть раздел предупреждений.", show_alert=True)
                return
        bot.answer_callback_query(c.id)
        return

    # section actions: st_<sec>_...
    msec = re.match(r"st_(welcome|farewell|rules|cleanup|warn)_(.+)", prefix)
    if not msec:
        bot.answer_callback_query(c.id)
        return

    sec = msec.group(1)
    action = msec.group(2)

    if sec == "warn":
        ch = _mod_get_chat(chat_id)
        settings = ch.get("settings") or {}
        page = "main"

        if action == "open":
            text = _render_warn_settings(chat_id, page="main")
            kb = _build_warn_settings_keyboard(chat_id, page="main")
            edited = _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
            if not edited:
                try:
                    bot.send_message(
                        msg_chat.id,
                        text,
                        parse_mode='HTML',
                        disable_web_page_preview=True,
                        reply_markup=kb,
                    )
                except Exception:
                    bot.answer_callback_query(c.id, "Не удалось открыть раздел предупреждений.", show_alert=True)
                    return
            bot.answer_callback_query(c.id)
            return

        if action == "noop":
            bot.answer_callback_query(c.id)
            return

        if action == "toggle":
            settings["warn_enabled"] = not bool(settings.get("warn_enabled", True))
            ch["settings"] = settings
            _mod_save()
        elif action == "ptype":
            try:
                _, chat_id_s, ptype = data.split(":", 2)
            except Exception:
                bot.answer_callback_query(c.id)
                return
            if ptype in ("mute", "ban", "kick"):
                wp = settings.get("warn_punish") or {}
                wp["type"] = ptype
                if ptype == "kick":
                    wp["duration"] = None
                elif wp.get("duration") is None:
                    wp["duration"] = 24 * 60 * 60
                settings["warn_punish"] = wp
                ch["settings"] = settings
                _mod_save()
                page = "punish"
        elif action == "dur":
            try:
                _, chat_id_s, dur_s = data.split(":", 2)
                duration = int(dur_s)
            except Exception:
                bot.answer_callback_query(c.id)
                return
            duration = max(MIN_PUNISH_SECONDS, min(MAX_PUNISH_SECONDS, duration))
            wp = settings.get("warn_punish") or {}
            if (wp.get("type") or "mute").lower() in ("mute", "ban"):
                wp["duration"] = duration
                settings["warn_punish"] = wp
                ch["settings"] = settings
                _mod_save()
                page = "punish"
        elif action == "limit":
            try:
                _, chat_id_s, delta_s = data.split(":", 2)
                delta = int(delta_s)
            except Exception:
                bot.answer_callback_query(c.id)
                return
            cur = int(settings.get("warn_limit") or 3)
            settings["warn_limit"] = max(2, min(10, cur + delta))
            ch["settings"] = settings
            _mod_save()
        elif action == "setlimit":
            try:
                _, chat_id_s, value_s = data.split(":", 2)
                value = int(value_s)
            except Exception:
                bot.answer_callback_query(c.id)
                return
            settings["warn_limit"] = max(2, min(10, value))
            ch["settings"] = settings
            _mod_save()
        elif action == "page":
            try:
                _, chat_id_s, page_s = data.split(":", 2)
                page = "punish" if page_s == "punish" else "main"
            except Exception:
                bot.answer_callback_query(c.id)
                return

        text = _render_warn_settings(chat_id, page=page)
        kb = _build_warn_settings_keyboard(chat_id, page=page)
        edited = _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
        if not edited:
            try:
                bot.send_message(
                    msg_chat.id,
                    text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            except Exception:
                bot.answer_callback_query(c.id, "Не удалось открыть раздел предупреждений.", show_alert=True)
                return
        bot.answer_callback_query(c.id)
        return

    # ✅ cleanup как секция
    if sec == "cleanup":
        cl = _cleanup_get(chat_id)

        if action == "cmds":
            text = _render_cleanup_commands(chat_id)
            kb = _build_cleanup_commands_keyboard(chat_id)
            _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
            bot.answer_callback_query(c.id)
            return

        if action == "sys":
            text = _render_cleanup_system(chat_id)
            kb = _build_cleanup_system_keyboard(chat_id, selected_idx=None)
            _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
            bot.answer_callback_query(c.id)
            return

        if action == "syspick":
            try:
                _, chat_id_s, idx_s = (c.data or "").split(":", 2)
                idx = int(idx_s)
            except Exception:
                bot.answer_callback_query(c.id)
                return

            if idx < 0 or idx >= len(CLEANUP_SYSTEM_TYPES_ORDER):
                idx = None

            text = _render_cleanup_system(chat_id)
            kb = _build_cleanup_system_keyboard(chat_id, selected_idx=idx)
            _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
            bot.answer_callback_query(c.id)
            return

        if action in ("cmdnoop", "sysnoop"):
            bot.answer_callback_query(c.id)
            return

        if action == "cmdset":
            try:
                _, chat_id_s, sign, val_s = (c.data or "").split(":", 3)
                sign = sign.strip()
                val = (val_s.strip() == "1")
            except Exception:
                bot.answer_callback_query(c.id)
                return

            if sign in CLEANUP_CMD_SIGNS:
                cmds = cl.get("commands") or {}
                cmds[sign] = bool(val)
                cl["commands"] = cmds
                cl["updated_at"] = _now_ts()
                _cleanup_save(chat_id, cl)

            text = _render_cleanup_commands(chat_id)
            kb = _build_cleanup_commands_keyboard(chat_id)
            _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
            bot.answer_callback_query(c.id)
            return

        if action == "sysset":
            try:
                _, chat_id_s, idx_s, val_s = (c.data or "").split(":", 3)
                idx = int(idx_s)
                val = (val_s.strip() == "1")
            except Exception:
                bot.answer_callback_query(c.id)
                return

            if 0 <= idx < len(CLEANUP_SYSTEM_TYPES_ORDER):
                ct = CLEANUP_SYSTEM_TYPES_ORDER[idx]
                sysd = cl.get("system") or {}
                sysd[ct] = bool(val)
                cl["system"] = sysd
                cl["updated_at"] = _now_ts()
                _cleanup_save(chat_id, cl)

            text = _render_cleanup_system(chat_id)
            kb = _build_cleanup_system_keyboard(chat_id, selected_idx=idx)
            _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)
            bot.answer_callback_query(c.id)
            return

        bot.answer_callback_query(c.id)
        return



    # ✅ иначе — welcome/farewell/rules
    st = get_chat_settings(chat_id)
    sc = st.get(sec) or _default_section(False)

    # toggle
    if action == "toggle":
        sc["enabled"] = not bool(sc.get("enabled"))
        sc["updated_at"] = _now_ts()
        st[sec] = sc
        CHAT_SETTINGS[str(chat_id)] = st
        save_chat_settings()

        text = _render_section_preview(chat_id, sec)
        kb = _build_section_keyboard(chat_id, sec)
        _safe_edit_message_html(msg_chat.id, c.message.message_id, text, kb)

        bot.answer_callback_query(c.id)
        return

    # show current full (как увидит пользователь) — 2 сообщения
    if action == "show":
        html_no_subs = build_html_from_text_custom(sc.get("text_custom") or "")
        html_with_subs = _apply_vars(html_no_subs, chat_id, c.message.chat.title or "", user)

        media = sc.get("media") or []
        rows = ((sc.get("buttons") or {}).get("rows")) or []
        popups = ((sc.get("buttons") or {}).get("popups")) or []
        kb_payload = build_inline_keyboard_for_payload(sec, chat_id, rows, popups, user.id)

        bot.answer_callback_query(c.id)
        bot.send_message(
            msg_chat.id,
            f"<b>{_html.escape(_section_title(sec).capitalize())} (как увидит пользователь):</b>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        _send_payload(msg_chat.id, html_with_subs, media, reply_markup=kb_payload)
        return

    # ---------------- TEXT UI ----------------
    if action == "text":
        _pending_put(f"pending_{sec}_text", user.id, chat_id)

        # удаляем старую UI-мессагу для этого pending (если была) + текущее сообщение
        _delete_pending_ui(msg_chat.id, f"pending_{sec}_text_msg", user.id, also_msg_id=c.message.message_id)

        emoji_text = f'<tg-emoji emoji-id="{EMOJI_WELCOME_TEXT_ID}">📝</tg-emoji>'
        body = (
            f"{emoji_text} <b>Пришлите новый текст для {_section_title(sec)}.</b>\n\n"
            "<blockquote expandable=\"true\">"
            "<b>Доступные переменные:</b>\n"
            "[NAME] — полное имя пользователя\n"
            "[ID] — ID пользователя\n"
            "[GROUP_NAME] — название группы\n"
            "[NAME_LINK] — полное имя пользователя с ссылкой на профиль\n"
            "[MENTION] — упоминание пользователя"
            "</blockquote>\n\n"
            "<b>Поддерживается:</b>\n"
            "• обычное форматирование Telegram\n"
            "• и/или наш кастомный HTML\n\n"
            "<blockquote expandable=\"true\">"
            "<b>Кастомный HTML:</b>\n"
            "<code>&lt;b&gt;жирный&lt;/&gt;</code>\n"
            "<code>&lt;i&gt;курсив&lt;/&gt;</code>\n"
            "<code>&lt;u&gt;подчёркнутый&lt;/&gt;</code>\n"
            "<code>&lt;s&gt;зачёркнутый&lt;/&gt;</code>\n"
            "<code>&lt;code&gt;моноширинный&lt;/&gt;</code>\n"
            "<code>&lt;pre&gt;код&lt;/&gt;</code>\n"
            "<code>&lt;sp&gt;спойлер&lt;/&gt;</code>\n"
            "<code>&lt;quote&gt;цитата&lt;/&gt;</code>\n"
            "<code>&lt;quote exp&gt;свёрнутая цитата&lt;/&gt;</code>\n"
            "<code>&lt;emoji id='123'&gt;😀&lt;/&gt;</code>\n"
            "<code>&lt;a href='https://example.com'&gt;ссылка&lt;/&gt;</code>\n"
            "<code>&lt;br&gt;</code> — перенос строки"
            "</blockquote>\n\n"
            "<i>Важно:</i> если Telegram-выделение захватит символы &lt; или &gt;, "
            "то Telegram-форматирование может быть проигнорировано."
        )

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("Показать текущий текст", callback_data=f"st_{sec}_text_show:{chat_id}"))
        kb.add(InlineKeyboardButton("Удалить текст", callback_data=f"st_{sec}_text_del:{chat_id}"))
        kb.add(_build_cancel_btn(f"st_{sec}_text_cancel:{chat_id}"))

        sent = bot.send_message(msg_chat.id, body, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        _pending_msg_set(f"pending_{sec}_text_msg", user.id, sent.message_id)

        bot.answer_callback_query(c.id)
        return

    if action == "text_cancel":
        _pending_pop(f"pending_{sec}_text", user.id)
        msg_id = _pending_msg_pop(f"pending_{sec}_text_msg", user.id)
        _try_delete_private_prompt(msg_chat.id, msg_id)

        text = _render_section_preview(chat_id, sec)
        kb = _build_section_keyboard(chat_id, sec)
        bot.send_message(msg_chat.id, text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        bot.answer_callback_query(c.id)
        return

    if action == "text_del":
        sc["text_custom"] = ""
        sc["source"] = "plain"
        sc["entities"] = []
        sc["updated_at"] = _now_ts()
        st[sec] = sc
        CHAT_SETTINGS[str(chat_id)] = st
        save_chat_settings()

        # FIX #3: prompt исчезает, "удалено" приходит с Назад + Отмена
        _delete_pending_ui(msg_chat.id, f"pending_{sec}_text_msg", user.id, also_msg_id=c.message.message_id)
        _pending_put(f"pending_{sec}_text", user.id, chat_id)

        sent = bot.send_message(
            msg_chat.id,
            f"{emoji_ok} <b>Текст удалён.</b>\n\nНажмите «Назад», чтобы снова увидеть инструкцию и прислать новый текст.",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=_kb_deleted(
                back_cb=f"st_{sec}_text:{chat_id}",
                cancel_cb=f"st_{sec}_text_cancel:{chat_id}",
            ),
        )
        _pending_msg_set(f"pending_{sec}_text_msg", user.id, sent.message_id)

        bot.answer_callback_query(c.id)
        return

    if action == "text_show":
        tc = (sc.get("text_custom") or "").strip()
        bot.answer_callback_query(c.id)
        bot.send_message(msg_chat.id, "<b>Текущий текст (как увидит пользователь):</b>", parse_mode="HTML", disable_web_page_preview=True)
        if not tc:
            bot_raw.send_message(msg_chat.id, "Текст не задан.", disable_web_page_preview=True)
            return
        html_no_subs = build_html_from_text_custom(tc)
        bot.send_message(msg_chat.id, html_no_subs, parse_mode="HTML", disable_web_page_preview=True)
        return

    # ---------------- MEDIA UI ----------------
    
    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    
    if action == "media":
        _pending_put(f"pending_{sec}_media", user.id, chat_id)

        _delete_pending_ui(msg_chat.id, f"pending_{sec}_media_msg", user.id, also_msg_id=c.message.message_id)

        emoji_media = f'<tg-emoji emoji-id="{EMOJI_WELCOME_MEDIA_ID}">🖼</tg-emoji>'
        body = (
            f"{emoji_media} <b>Пришлите медиа для {_section_title(sec)}.</b>\n\n"
            "<b>Поддерживается:</b>\n"
            "• Фото\n• Видео\n• Файл\n• Музыка\n• GIF\n\n"
            "<i>Подпись отдельно не задаётся.</i>\n"
            "Если у вас есть текст — он будет автоматически использоваться как описание, когда медиа есть."
        )

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("Показать текущее медиа", callback_data=f"st_{sec}_media_show:{chat_id}"))
        kb.add(InlineKeyboardButton("Удалить медиа", callback_data=f"st_{sec}_media_del:{chat_id}"))
        kb.add(_build_cancel_btn(f"st_{sec}_media_cancel:{chat_id}"))

        sent = bot.send_message(msg_chat.id, body, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        _pending_msg_set(f"pending_{sec}_media_msg", user.id, sent.message_id)

        bot.answer_callback_query(c.id)
        return

    if action == "media_cancel":
        _pending_pop(f"pending_{sec}_media", user.id)
        msg_id = _pending_msg_pop(f"pending_{sec}_media_msg", user.id)
        _try_delete_private_prompt(msg_chat.id, msg_id)

        text = _render_section_preview(chat_id, sec)
        kb = _build_section_keyboard(chat_id, sec)
        bot.send_message(msg_chat.id, text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        bot.answer_callback_query(c.id)
        return

    if action == "media_del":
        sc["media"] = []
        sc["updated_at"] = _now_ts()
        st[sec] = sc
        CHAT_SETTINGS[str(chat_id)] = st
        save_chat_settings()

        _delete_pending_ui(msg_chat.id, f"pending_{sec}_media_msg", user.id, also_msg_id=c.message.message_id)
        _pending_put(f"pending_{sec}_media", user.id, chat_id)

        sent = bot.send_message(
            msg_chat.id,
            f"{emoji_ok} <b>Медиа удалено.</b>\n\nНажмите «Назад», чтобы снова увидеть инструкцию и прислать новое медиа.",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=_kb_deleted(
                back_cb=f"st_{sec}_media:{chat_id}",
                cancel_cb=f"st_{sec}_media_cancel:{chat_id}",
            ),
        )
        _pending_msg_set(f"pending_{sec}_media_msg", user.id, sent.message_id)

        bot.answer_callback_query(c.id)
        return

    if action == "media_show":
        bot.answer_callback_query(c.id)
        bot.send_message(msg_chat.id, "<b>Текущее медиа:</b>", parse_mode="HTML", disable_web_page_preview=True)
        media = sc.get("media") or []
        if not media:
            bot_raw.send_message(msg_chat.id, "Медиа не задано.", disable_web_page_preview=True)
            return
        _send_media_only(msg_chat.id, media)  # ВАЖНО: без кнопок
        return

    # ---------------- BUTTONS UI ----------------

    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    
    if action == "buttons":
        _pending_put(f"pending_{sec}_buttons", user.id, chat_id)

        _delete_pending_ui(msg_chat.id, f"pending_{sec}_buttons_msg", user.id, also_msg_id=c.message.message_id)

        emoji_btn = f'<tg-emoji emoji-id="{EMOJI_WELCOME_BUTTONS_ID}">🔘</tg-emoji>'
        body = (
            f"{emoji_btn} <b>Пришлите кнопки для {_section_title(sec)}.</b>\n\n"
            "<b>Формат:</b>\n"
            "<code>Название - example.com</code>\n"
            "<code>Название - popup: текст</code>\n"
            "<code>Название - rules</code>\n"
            "<code>Название - del</code>\n\n"
            "<b>Несколько в одном ряду:</b>\n"
            "<code>Кнопка1 - example.com & Кнопка2 - example.com</code>\n\n"
            "<b>Цвет:</b>\n"
            "<code>#r Название - example.com</code> (красный)\n"
            "<code>#g Название - example.com</code> (зелёный)\n"
            "<code>#b Название - example.com</code> (цвет, зависящий от темы пользователя)\n\n"
            "<b>Лимиты:</b>\n"
            f"• 1–{MAX_PER_ROW} кнопки в ряду\n"
            f"• до {MAX_ROWS} рядов\n"
            f"• до {MAX_TOTAL_BTNS} кнопок всего\n"
            "• до 1 премиум-эмодзи в кнопке (эмодзи может быть только в начале названия)"  
        )

        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("Показать текущие кнопки", callback_data=f"st_{sec}_buttons_show:{chat_id}"))
        kb.add(InlineKeyboardButton("Удалить кнопки", callback_data=f"st_{sec}_buttons_del:{chat_id}"))
        kb.add(_build_cancel_btn(f"st_{sec}_buttons_cancel:{chat_id}"))

        sent = bot.send_message(msg_chat.id, body, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        _pending_msg_set(f"pending_{sec}_buttons_msg", user.id, sent.message_id)

        bot.answer_callback_query(c.id)
        return

    if action == "buttons_cancel":
        _pending_pop(f"pending_{sec}_buttons", user.id)
        msg_id = _pending_msg_pop(f"pending_{sec}_buttons_msg", user.id)
        _try_delete_private_prompt(msg_chat.id, msg_id)

        text = _render_section_preview(chat_id, sec)
        kb = _build_section_keyboard(chat_id, sec)
        bot.send_message(msg_chat.id, text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb)
        bot.answer_callback_query(c.id)
        return

    if action == "buttons_del":
        sc["buttons"] = {"rows": [], "popups": []}
        sc["updated_at"] = _now_ts()
        st[sec] = sc
        CHAT_SETTINGS[str(chat_id)] = st
        save_chat_settings()

        _delete_pending_ui(msg_chat.id, f"pending_{sec}_buttons_msg", user.id, also_msg_id=c.message.message_id)
        _pending_put(f"pending_{sec}_buttons", user.id, chat_id)

        sent = bot.send_message(
            msg_chat.id,
             f"{emoji_ok} <b>Кнопки удалены.</b>\n\nНажмите «Назад», чтобы снова увидеть инструкцию и прислать кнопки заново.",
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=_kb_deleted(
                back_cb=f"st_{sec}_buttons:{chat_id}",
                cancel_cb=f"st_{sec}_buttons_cancel:{chat_id}",
            ),
        )
        _pending_msg_set(f"pending_{sec}_buttons_msg", user.id, sent.message_id)

        bot.answer_callback_query(c.id)
        return

    if action == "buttons_show":
        rows = ((sc.get("buttons") or {}).get("rows")) or []
        popups = ((sc.get("buttons") or {}).get("popups")) or []

        bot.answer_callback_query(c.id)
        bot.send_message(msg_chat.id, "<b>Текущие кнопки:</b>", parse_mode="HTML", disable_web_page_preview=True)

        kb_show = build_inline_keyboard_for_payload(sec, chat_id, rows, popups, user.id)
        if not kb_show:
            bot_raw.send_message(msg_chat.id, "Кнопки не заданы.", disable_web_page_preview=True)
            return

        bot.send_message(msg_chat.id, "\u2063", disable_web_page_preview=True, reply_markup=kb_show)
        return

    bot.answer_callback_query(c.id)
  
# ------------------------------------------------------------
# PRIVATE handler: принимает ТЕКСТ / МЕДИА / КНОПКИ (welcome/farewell/rules)
# ------------------------------------------------------------

@bot.message_handler(func=lambda m: m.chat.type == "private", content_types=[
    "text", "photo", "video", "document", "audio", "animation"
])
def on_settings_private_input(m: types.Message):
    user_id = m.from_user.id
    ct = getattr(m, "content_type", "text")

    pending_bc = BROADCAST_PENDING_INPUT.get(user_id)
    if pending_bc and is_owner(m.from_user):
        draft = BROADCAST_DRAFTS.get(user_id)
        if not draft or int(draft.get("id") or 0) != int(pending_bc.get("draft_id") or 0):
            BROADCAST_PENDING_INPUT.pop(user_id, None)
            return

        mode = str(pending_bc.get("mode") or "")
        prompt_id = int(pending_bc.get("prompt_message_id") or 0)

        if mode == "text":
            if ct != "text":
                bot.send_message(m.chat.id, premium_prefix("Для текста рассылки пришлите текстовое сообщение."), parse_mode='HTML', disable_web_page_preview=True)
                return
            text_custom, source, entities_ser = convert_section_text_from_message(m)
            draft["text_custom"] = text_custom
            draft["source"] = source
            draft["entities"] = entities_ser
            draft["updated_at"] = int(time.time())
        elif mode == "media":
            if ct == "text":
                bot.send_message(m.chat.id, premium_prefix("Для медиа рассылки пришлите фото/видео/файл/музыку/gif."), parse_mode='HTML', disable_web_page_preview=True)
                return
            payload = _extract_media_payload(m)
            if not payload:
                bot.send_message(m.chat.id, premium_prefix("Этот тип медиа не поддерживается для рассылки."), parse_mode='HTML', disable_web_page_preview=True)
                return
            draft["media"] = [payload]
            draft["updated_at"] = int(time.time())
        elif mode == "buttons":
            if ct != "text":
                bot.send_message(m.chat.id, premium_prefix("Кнопки для рассылки нужно отправлять текстом."), parse_mode='HTML', disable_web_page_preview=True)
                return
            try:
                rows, popups = parse_buttons_text(m.text or "", m.entities or [])
            except ButtonSyntaxError as err:
                bot.send_message(m.chat.id, premium_prefix(_format_button_syntax_error(err)), parse_mode='HTML', disable_web_page_preview=True)
                return
            draft["buttons"] = {"rows": rows, "popups": popups}
            draft["updated_at"] = int(time.time())
        else:
            BROADCAST_PENDING_INPUT.pop(user_id, None)
            return

        BROADCAST_DRAFTS[user_id] = draft
        BROADCAST_PENDING_INPUT.pop(user_id, None)

        if prompt_id > 0:
            try:
                bot.delete_message(m.chat.id, prompt_id)
            except Exception:
                pass

        panel_text = _broadcast_render_panel_text(user_id)
        kb = _build_broadcast_panel_keyboard(int(draft.get("id") or 0))
        bot.send_message(m.chat.id, panel_text, parse_mode='HTML', disable_web_page_preview=True, reply_markup=kb)
        return

    # helper: check allowed
    def _check_allowed(chat_id: int) -> bool:
        allowed, _ = _user_can_open_settings(chat_id, m.from_user)
        return bool(allowed)

    # ---------------- CUSTOM WARN DURATION ----------------
    warn_pending_cid = _pending_get("pending_warn_duration").get(str(user_id))
    if warn_pending_cid:
        if ct != "text":
            kb_err = InlineKeyboardMarkup(row_width=1)
            kb_err.add(InlineKeyboardButton("Назад", callback_data=f"stw:open:{warn_pending_cid}"))
            _replace_pending_ui(
                m.chat.id,
                "pending_warn_duration_msg",
                user_id,
                premium_prefix("Пришлите длительность текстом: 30m, 2h, 3д, 1н или 'навсегда'."),
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

        try:
            chat_id = int(warn_pending_cid)
        except Exception:
            _pending_pop("pending_warn_duration", user_id)
            return

        if not _check_allowed(chat_id):
            _pending_pop("pending_warn_duration", user_id)
            return

        raw = (m.text or "").strip()
        parsed_duration, consumed_tokens, invalid = _parse_duration_prefix(
            raw,
            allow_russian_duration=True,
            max_parts=3,
        )
        total_tokens = len(raw.split()) if raw else 0
        if invalid or parsed_duration is None or consumed_tokens == 0 or consumed_tokens != total_tokens:
            kb_err = InlineKeyboardMarkup(row_width=1)
            kb_err.add(InlineKeyboardButton("Назад", callback_data=f"stw:open:{chat_id}"))
            _replace_pending_ui(
                m.chat.id,
                "pending_warn_duration_msg",
                user_id,
                premium_prefix("Неверный формат. Используйте до 3 интервалов: 30m, 1h 2m, 2mou 1d, навсегда."),
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

        duration = int(parsed_duration)

        if duration != 0 and (duration < MIN_PUNISH_SECONDS or duration > MAX_PUNISH_SECONDS):
            kb_err = InlineKeyboardMarkup(row_width=1)
            kb_err.add(InlineKeyboardButton("Назад", callback_data=f"stw:open:{chat_id}"))
            _replace_pending_ui(
                m.chat.id,
                "pending_warn_duration_msg",
                user_id,
                premium_prefix("Длительность должна быть от 1 минуты до 365 дней, либо 'навсегда'."),
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

        ch = _mod_get_chat(chat_id)
        settings = ch.get("settings") or {}
        wp = settings.get("warn_punish") or {}
        ptype = (wp.get("type") or "mute").lower()
        if ptype not in ("mute", "ban"):
            _pending_pop("pending_warn_duration", user_id)
            _try_delete_private_prompt(m.chat.id, _pending_msg_pop("pending_warn_duration_msg", user_id))
            bot.send_message(
                m.chat.id,
                premium_prefix("Для типа наказания 'Кик' длительность не используется."),
                parse_mode='HTML',
                disable_web_page_preview=True,
            )
            return

        wp["duration"] = int(duration)
        settings["warn_punish"] = wp
        ch["settings"] = settings
        _mod_save()
        _pending_pop("pending_warn_duration", user_id)
        prompt_id = _pending_msg_pop("pending_warn_duration_msg", user_id)

        _try_delete_private_prompt(m.chat.id, prompt_id)
        _try_delete_private_prompt(m.chat.id, m.message_id)

        ok_text = premium_prefix("✅ Время установлено.")
        kb_ok = InlineKeyboardMarkup()
        b_back = InlineKeyboardButton("Назад", callback_data=f"stw:open:{chat_id}")
        try:
            b_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
            b_back.style = "primary"
        except Exception:
            pass
        kb_ok.add(b_back)
        bot.send_message(
            m.chat.id,
            ok_text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb_ok,
        )
        return

    # =========================================================
    # FIX #3:
    # - Любое сообщение "пришлите ..." / "ошибка" / "удалено" всегда заменяет предыдущее UI-сообщение
    # - Ошибки приходят с кнопкой "Отмена"
    # =========================================================

    # ---------------- MEDIA message ----------------
    emoji_x = '<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    
    if ct != "text":
        # 1) если есть pending_media — принимаем/ругаемся по медиа
        for sec in SECTION_KEYS:
            cid = _pending_get(f"pending_{sec}_media").get(str(user_id))
            if not cid:
                continue
            try:
                chat_id = int(cid)
            except Exception:
                _pending_pop(f"pending_{sec}_media", user_id)
                return

            if not _check_allowed(chat_id):
                _pending_pop(f"pending_{sec}_media", user_id)

                return

            payload = _extract_media_payload(m)
            if not payload:
                # удаляем prompt и показываем ошибку + cancel
                kb_err = _kb_error_cancel(f"st_{sec}_media_cancel:{chat_id}")
                _replace_pending_ui(
                    m.chat.id,
                    f"pending_{sec}_media_msg",
                    user_id,
                    f"{emoji_x} <b>Это медиа не поддерживается.</b>\nПришлите фото/видео/файл/музыку/gif.",
                    reply_markup=kb_err,
                    parse_mode="HTML",
                )
                return

            st = get_chat_settings(chat_id)
            sc = st.get(sec) or _default_section(False)

            # альбомы: пока упрощённо (как было у тебя)
            sc["media"] = [payload]
            sc["updated_at"] = _now_ts()

            st[sec] = sc
            CHAT_SETTINGS[str(chat_id)] = st
            save_chat_settings()

            _pending_pop(f"pending_{sec}_media", user_id)
            msg_id = _pending_msg_pop(f"pending_{sec}_media_msg", user_id)
            _try_delete_private_prompt(m.chat.id, msg_id)

            bot.reply_to(
                m,
                f"{emoji_ok} <b>Медиа {_section_title(sec)} установлено.</b>",
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_only_back_kb(chat_id, sec),
            )
            return

        # 2) если медиа прислали, а ожидается текст/кнопки — показываем ошибку и заменяем prompt
        for sec in SECTION_KEYS:
            cid = _pending_get(f"pending_{sec}_text").get(str(user_id))
            if cid:
                try:
                    chat_id = int(cid)
                except Exception:
                    _pending_pop(f"pending_{sec}_text", user_id)
                    return
                kb_err = _kb_error_cancel(f"st_{sec}_text_cancel:{chat_id}")
                _replace_pending_ui(
                    m.chat.id,
                    f"pending_{sec}_text_msg",
                    user_id,
                    f"{emoji_x} <b>Это не текст.</b>\nПришлите текстовое сообщение.",
                    reply_markup=kb_err,
                    parse_mode="HTML",
                )
                return

        for sec in SECTION_KEYS:
            cid = _pending_get(f"pending_{sec}_buttons").get(str(user_id))
            if cid:
                try:
                    chat_id = int(cid)
                except Exception:
                    _pending_pop(f"pending_{sec}_buttons", user_id)
                    return
                kb_err = _kb_error_cancel(f"st_{sec}_buttons_cancel:{chat_id}")
                _replace_pending_ui(
                    m.chat.id,
                    f"pending_{sec}_buttons_msg",
                    user_id,
                    f"{emoji_x} <b>Это не текст.</b>\nПришлите кнопки текстом по формату из инструкции.",
                    reply_markup=kb_err,
                    parse_mode="HTML",
                )
                return

        return

    # ---------------- TEXT message ----------------
    
    emoji_ok = '<tg-emoji emoji-id="5427009714745517609">✅</tg-emoji>'
    emoji_x = '<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
    
    # 1) если есть pending_text — принимаем
    for sec in SECTION_KEYS:
        cid = _pending_get(f"pending_{sec}_text").get(str(user_id))
        if cid:
            try:
                chat_id = int(cid)
            except Exception:
                _pending_pop(f"pending_{sec}_text", user_id)
                return

            if not _check_allowed(chat_id):
                _pending_pop(f"pending_{sec}_text", user_id)
                return

            text_custom, source, entities_ser = convert_section_text_from_message(m)

            st = get_chat_settings(chat_id)
            sc = st.get(sec) or _default_section(False)

            sc["text_custom"] = text_custom
            sc["source"] = source
            sc["entities"] = entities_ser
            sc["updated_at"] = _now_ts()

            st[sec] = sc
            CHAT_SETTINGS[str(chat_id)] = st
            save_chat_settings()

            _pending_pop(f"pending_{sec}_text", user_id)
            msg_id = _pending_msg_pop(f"pending_{sec}_text_msg", user_id)
            _try_delete_private_prompt(m.chat.id, msg_id)

            bot.reply_to(
                m,
                f"{emoji_ok} <b>Текст {_section_title(sec)} установлен.</b>",
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_only_back_kb(chat_id, sec),
            )
            return

    # 2) если есть pending_buttons — принимаем
    for sec in SECTION_KEYS:
        cid = _pending_get(f"pending_{sec}_buttons").get(str(user_id))
        if cid:
            try:
                chat_id = int(cid)
            except Exception:
                _pending_pop(f"pending_{sec}_buttons", user_id)
                return

            if not _check_allowed(chat_id):
                _pending_pop(f"pending_{sec}_buttons", user_id)
                return

            # FIX #2: передаём entities, чтобы подхватить premium/custom emoji как icon
            try:
                rows, popups = parse_buttons_text(m.text or "", m.entities or [])
            except ButtonSyntaxError as err:
                kb_err = _kb_error_cancel(f"st_{sec}_buttons_cancel:{chat_id}")
                _replace_pending_ui(
                    m.chat.id,
                    f"pending_{sec}_buttons_msg",
                    user_id,
                    premium_prefix(_format_button_syntax_error(err)),
                    reply_markup=kb_err,
                    parse_mode="HTML",
                )
                return

            st = get_chat_settings(chat_id)
            sc = st.get(sec) or _default_section(False)

            sc["buttons"] = {"rows": rows, "popups": popups}
            sc["updated_at"] = _now_ts()

            st[sec] = sc
            CHAT_SETTINGS[str(chat_id)] = st
            save_chat_settings()

            _pending_pop(f"pending_{sec}_buttons", user_id)
            msg_id = _pending_msg_pop(f"pending_{sec}_buttons_msg", user_id)
            _try_delete_private_prompt(m.chat.id, msg_id)

            bot.reply_to(
                m,
                f"{emoji_ok} <b>Кнопки {_section_title(sec)} установлены.</b>",
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_only_back_kb(chat_id, sec),
            )
            return

    # 3) если ожидается медиа, а пришёл текст — ошибка + cancel (и удаляем prompt)
    for sec in SECTION_KEYS:
        cid = _pending_get(f"pending_{sec}_media").get(str(user_id))
        if cid:
            try:
                chat_id = int(cid)
            except Exception:
                _pending_pop(f"pending_{sec}_media", user_id)
                return

            kb_err = _kb_error_cancel(f"st_{sec}_media_cancel:{chat_id}")
            _replace_pending_ui(
                m.chat.id,
                f"pending_{sec}_media_msg",
                user_id,
                f"{emoji_x} <b>Это не медиа.</b>\nПришлите фото/видео/файл/музыку/gif.",
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

    return


# ------------------------------------------------------------
# ВЫЗОВ WELCOME / FAREWELL / RULES (group/supergroup)
# ------------------------------------------------------------

_BOT_USERNAME_LC: Optional[str] = None
_BOT_ID: Optional[int] = None

_RULES_ALIASES = {"rules", "правила"}


def _get_bot_username_lower() -> str:
    global _BOT_USERNAME_LC
    if _BOT_USERNAME_LC is None:
        try:
            me = bot.get_me()
            _BOT_USERNAME_LC = (getattr(me, "username", "") or "").lower()
        except Exception:
            _BOT_USERNAME_LC = ""
    return _BOT_USERNAME_LC or ""


def _get_bot_id() -> int:
    global _BOT_ID
    if _BOT_ID is None:
        try:
            me = bot.get_me()
            _BOT_ID = int(getattr(me, "id", 0) or 0)
        except Exception:
            _BOT_ID = 0
    return _BOT_ID or 0


def _is_rules_trigger(text: Optional[str]) -> bool:
    """
    Триггеры:
      /rules, rules, /правила, правила, .правила, .rules, !rules, !правила
    + поддержка /rules@MyBot (игнорируем команды для других ботов)
    """
    if not text:
        return False

    t = text.strip()
    if not t:
        return False

    tl = t.lower()

    # /rules or /rules@botusername
    if tl.startswith("/"):
        first = tl.split()[0]           # "/rules@xxx"
        cmd = first[1:]                 # "rules@xxx"
        cmd_name, sep, cmd_target = cmd.partition("@")

        if sep and cmd_target:
            my = _get_bot_username_lower()
            # если не смогли узнать username, лучше не реагировать на @команды
            if not my:
                return False
            if cmd_target.lower() != my:
                return False

        return cmd_name in _RULES_ALIASES

    # .rules / !rules (берём только первый токен)
    if tl[0] in (".", "!"):
        first = tl.split()[0]
        return first[1:] in _RULES_ALIASES

    # plain: "rules" / "правила" (только если сообщение состоит из одного слова)
    if " " in tl:
        return False

    return tl in _RULES_ALIASES


def _send_section_payload(chat_id: int, sec: str, viewer_user, chat_title: str, viewer_uid_for_buttons: int) -> bool:
    """
    Унифицированная отправка секции:
      - конвертируем text_custom -> Telegram HTML
      - применяем переменные под viewer_user
      - добавляем медиа
      - строим inline-клаву (popup/rules/del будут доступны только viewer_uid_for_buttons)
    """
    st = get_chat_settings(chat_id)
    sc = st.get(sec) or _default_section(False)

    html_text = build_html_from_text_custom(sc.get("text_custom") or "")
    if html_text:
        html_text = _apply_vars(html_text, chat_id, chat_title or str(chat_id), viewer_user)

    media = sc.get("media") or []
    rows = ((sc.get("buttons") or {}).get("rows")) or []
    popups = ((sc.get("buttons") or {}).get("popups")) or []
    kb = build_inline_keyboard_for_payload(sec, chat_id, rows, popups, viewer_uid_for_buttons)

    # если вообще пусто — не шлём ничего
    if not html_text and not media and not rows:
        return False

    _send_payload(chat_id, html_text, media, reply_markup=kb)
    return True


# ---------------- WELCOME ----------------

@bot.message_handler(content_types=["new_chat_members"])
def on_welcome_new_members(m: types.Message):
    if m.chat.type not in ("group", "supergroup"):
        return ContinueHandling()

    chat_id = m.chat.id
    st = get_chat_settings(chat_id)
    sc = st.get("welcome") or _default_section(False)

    if not bool(sc.get("enabled")):
        return ContinueHandling()

    bot_id = _get_bot_id()
    title = m.chat.title or ""

    for u in (m.new_chat_members or []):
        try:
            if bot_id and u.id == bot_id:
                continue  # не приветствуем сами себя
        except Exception:
            pass

        _send_section_payload(
            chat_id=chat_id,
            sec="welcome",
            viewer_user=u,
            chat_title=title,
            viewer_uid_for_buttons=int(getattr(u, "id", 0) or 0),
        )

    # ВАЖНО: даём дойти до cleanup_delete_system_runtime (удаление system messages по типам)
    return ContinueHandling()

# ---------------- FAREWELL ----------------

@bot.message_handler(content_types=["left_chat_member"])
def on_farewell_left_member(m: types.Message):
    if m.chat.type not in ("group", "supergroup"):
        return ContinueHandling()

    chat_id = m.chat.id
    st = get_chat_settings(chat_id)
    sc = st.get("farewell") or _default_section(False)

    if not bool(sc.get("enabled")):
        return ContinueHandling()

    left = getattr(m, "left_chat_member", None) or getattr(m, "from_user", None)
    if not left:
        return ContinueHandling()

    left_id = int(getattr(left, "id", 0) or 0)
    if left_id and _is_farewell_suppressed(chat_id, left_id):
        return ContinueHandling()

    bot_id = _get_bot_id()
    try:
        if bot_id and left.id == bot_id:
            return ContinueHandling()  # если выгнали бота — не пытаемся слать farewell
    except Exception:
        pass

    _send_section_payload(
        chat_id=chat_id,
        sec="farewell",
        viewer_user=left,
        chat_title=m.chat.title or "",
        viewer_uid_for_buttons=left_id,
    )

    return ContinueHandling()

# ---------------- RULES triggers ----------------

@bot.message_handler(
    content_types=["text"],
    func=lambda m: (m.chat.type in ("group", "supergroup")) and _is_rules_trigger(getattr(m, "text", None))
)
def on_rules_trigger(m: types.Message):
    chat_id = m.chat.id
    st = get_chat_settings(chat_id)
    rules = st.get("rules") or _default_section(False)

    # Правила по команде/словам — только если включены в настройках
    if not bool(rules.get("enabled")):
        return

    ok = _send_section_payload(
        chat_id=chat_id,
        sec="rules",
        viewer_user=m.from_user,
        chat_title=m.chat.title or "",
        viewer_uid_for_buttons=int(getattr(m.from_user, "id", 0) or 0),
    )

    if not ok:
        # Если включены, но пустые — даём понятный ответ
        bot.reply_to(m, "Правила не заданы.", disable_web_page_preview=True)
        

def _cleanup_cmd_enabled(chat_id: int, sign: str) -> bool:
    try:
        st = get_chat_settings(chat_id)
        cl = st.get("cleanup") or {}
        cmds = cl.get("commands") or {}
        return bool(cmds.get(sign))
    except Exception:
        return False


def _cleanup_sys_enabled(chat_id: int, ct: str) -> bool:
    try:
        st = get_chat_settings(chat_id)
        cl = st.get("cleanup") or {}

        # новый формат
        sysd = cl.get("system") or {}
        if isinstance(sysd, dict):
            return bool(sysd.get(ct))

        # fallback на старый формат (если вдруг остался)
        sm = cl.get("system_messages", False)
        if isinstance(sm, bool):
            return sm

        return False
    except Exception:
        return False


@bot.message_handler(content_types=["text"], func=lambda m: m.chat.type in ("group", "supergroup"))
def cleanup_delete_commands_runtime(m: types.Message):
    try:
        chat_id = m.chat.id
        
        # Проверка одобрения группы
        if not is_group_approved(chat_id):
            return ContinueHandling()
        
        if not _bot_can_delete_messages(chat_id):
            return ContinueHandling()

        txt = (m.text or "")
        s = txt.lstrip()
        if not s:
            return ContinueHandling()

        sign = s[0]
        if sign not in CLEANUP_CMD_SIGNS:
            return ContinueHandling()

        if not _cleanup_cmd_enabled(chat_id, sign):
            return ContinueHandling()

        # не трогаем сообщения самого бота (на всякий)
        try:
            if getattr(m.from_user, "is_bot", False) and int(getattr(m.from_user, "id", 0) or 0) == _get_bot_id():
                return ContinueHandling()
        except Exception:
            pass

        try:
            bot.delete_message(chat_id, m.message_id)
        except Exception:
            pass

    except Exception:
        pass

    return ContinueHandling()    

@bot.message_handler(content_types=CLEANUP_SYSTEM_CONTENT_TYPES, func=lambda m: m.chat.type in ("group", "supergroup"))
def cleanup_delete_system_runtime(m: types.Message):
    try:
        chat_id = m.chat.id
        
        # Проверка одобрения группы
        if not is_group_approved(chat_id):
            return ContinueHandling()
        
        ct = getattr(m, "content_type", "") or ""
        if ct not in CLEANUP_SYSTEM_LABELS:
            return ContinueHandling()

        if not _cleanup_sys_enabled(chat_id, ct):
            return ContinueHandling()

        if not _bot_can_delete_messages(chat_id):
            return ContinueHandling()

        if ct == "pinned_message" and _should_keep_pin_service_message(chat_id):
            return ContinueHandling()

        # 1) пробуем Bot API
        try:
            bot.delete_message(chat_id, m.message_id)
            return ContinueHandling()
        except Exception:
            pass

        # 2) fallback для pinned_message (если у тебя Telethon уже подключён)
        if ct == "pinned_message":
            try:
                _try_delete_last_bot_service_pin(chat_id)
            except Exception:
                pass

    except Exception:
        pass

    return ContinueHandling()



# ==== СТАТИСТИКА ПО ГРУППЕ ====

STATS_PAGES = {}
GROUP_STATS_PAGE_SIZE = 30

def build_message_link(chat: types.Chat, msg_id: int) -> str:
    if not msg_id:
        return ""
    if chat.username:
        return f"https://t.me/{chat.username}/{msg_id}"
    cid = str(chat.id)
    if cid.startswith("-100"):
        internal = cid[4:]
    elif cid.startswith("-"):
        internal = cid[1:]
    else:
        internal = cid
    return f"https://t.me/c/{internal}/{msg_id}"

def stats_user_link_html(chat: types.Chat, user_id: int, display_name: str) -> str:
    chat_id_s = str(chat.id)
    user_id_s = str(user_id)

    chat_users = USERS.get(chat_id_s) or {}
    data = chat_users.get(user_id_s) or {}

    username = data.get("username") or ""
    if username:
        url = f"https://t.me/{username}"
    else:
        url = f"tg://openmessage?user_id={user_id}"

    return f'<a href="{url}"><b>{display_name}</b></a>'

def build_group_stats_pages(chat: types.Chat):
    chat_id = str(chat.id)
    chat_stats = GROUP_STATS.get(chat_id, {})

    if not chat_stats:
        return [premium_prefix("В этой группе пока нет данных для статистики.")]

    items: list[str] = []
    sorted_items = sorted(
        chat_stats.items(),
        key=lambda item: item[1].get("count", 0),
        reverse=True
    )

    for user_id, data in sorted_items:
        count = data.get("count", 0)
        last_msg_id = data.get("last_msg_id")
        link = build_message_link(chat, last_msg_id)

        try:
            u = bot.get_chat_member(chat.id, int(user_id)).user
            display_name = u.full_name or u.first_name or u.username or "Пользователь"
        except Exception:
            display_name = "Пользователь"

        name_html = stats_user_link_html(chat, int(user_id), display_name)

        base = (
            f'<tg-emoji emoji-id="{PREMIUM_USER_EMOJI_ID}">👤</tg-emoji> '
            f'{name_html} — <b>{count}</b>'
        )
        if link:
            base += f' ( <a href="{link}">последнее сообщение</a> )'
        items.append(base)

    total_pages = max(1, (len(items) + GROUP_STATS_PAGE_SIZE - 1) // GROUP_STATS_PAGE_SIZE)
    pages: list[str] = []
    for page_idx in range(total_pages):
        start = page_idx * GROUP_STATS_PAGE_SIZE
        end = start + GROUP_STATS_PAGE_SIZE
        chunk = items[start:end]

        header = (
            f'<tg-emoji emoji-id="{PREMIUM_STATS_EMOJI_ID}">📊</tg-emoji> <b>Статистика:</b>\n\n'
        )
        body = "\n".join(chunk)
        page_text = (header + body).strip()
        if len(page_text) > MAX_MSG_LEN:
            page_text = page_text[:MAX_MSG_LEN - 3] + "..."
        pages.append(page_text)

    return pages


def _build_group_stats_keyboard(page: int, total_pages: int) -> dict | None:
    if total_pages <= 1:
        return None

    row = []
    if page > 0:
        row.append({"text": "⬅ Предыдущая страница", "callback_data": f"gstats_prev_{page}"})
    if page < total_pages - 1:
        row.append({"text": "Следующая страница ➡", "callback_data": f"gstats_next_{page}"})

    if not row:
        return None
    return {"inline_keyboard": [row]}

def send_group_stats(chat: types.Chat, manual: bool = False):
    pages = build_group_stats_pages(chat)
    if not pages:
        return
    if len(pages) == 1:
        bot.send_message(
            chat.id,
            pages[0],
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        return

    keyboard = _build_group_stats_keyboard(0, len(pages))
    resp = raw_send_with_inline_keyboard(chat.id, pages[0], keyboard)
    if not resp or not resp.get("ok"):
        print(f"[RAW] Ошибка отправки статистики: {resp}")
        return

    msg_id = resp["result"]["message_id"]
    key = (chat.id, msg_id)
    STATS_PAGES[key] = {"pages": pages, "current": 0}

@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and is_exact_stat(m.text))
def cmd_group_stats_manual(m: types.Message):
    add_stat_message(m)
    add_stat_command('group_stat')

    # Проверка одобрения группы
    if not is_group_approved(m.chat.id):
        return bot.reply_to(
            m,
            "⏳ Бот находится на модерации. Ожидание подтверждения от разработчика.",
            parse_mode='HTML'
        )

    wait_seconds = cooldown_hit('chat', int(m.chat.id), 'group_stat', 30)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='group_stat')

    # не реагируем, если команда введена ответом на сообщение
    if m.reply_to_message:
        return

    pages = build_group_stats_pages(m.chat)
    if not pages:
        return

    # если одна страница — отправляем как reply на команду
    if len(pages) == 1:
        return bot.reply_to(
            m,
            pages[0],
            parse_mode="HTML",
            disable_web_page_preview=True
        )

    # если страниц несколько — первая как reply на команду
    keyboard = _build_group_stats_keyboard(0, len(pages))
    resp = raw_send_with_inline_keyboard(m.chat.id, pages[0], keyboard)
    if not resp or not resp.get("ok"):
        print(f"[RAW] Ошибка отправки статистики: {resp}")
        return

    msg_id = resp["result"]["message_id"]
    key = (m.chat.id, msg_id)
    STATS_PAGES[key] = {"pages": pages, "current": 0}


@bot.callback_query_handler(func=lambda call: bool(call.data) and call.data.startswith("gstats_"))
def cb_group_stats_pagination(call: types.CallbackQuery):
    if _is_duplicate_callback_query(call):
        return
    data = call.data or ""
    m = call.message
    key = (m.chat.id, m.message_id)
    state = STATS_PAGES.get(key)
    if not state:
        return bot.answer_callback_query(call.id, "Эта статистика устарела, откройте новую.", show_alert=True)

    pages = state.get("pages") or []
    if not pages:
        STATS_PAGES.pop(key, None)
        return bot.answer_callback_query(call.id)

    match = re.match(r"^gstats_(next|prev)_(\d+)$", data)
    if not match:
        return bot.answer_callback_query(call.id)

    action = match.group(1)
    current = int(match.group(2))
    total_pages = len(pages)

    if action == "next":
        target = min(total_pages - 1, current + 1)
    else:
        target = max(0, current - 1)

    keyboard = _build_group_stats_keyboard(target, total_pages)

    try:
        raw_edit_message_with_keyboard(
            m.chat.id,
            m.message_id,
            pages[target],
            keyboard,
        )
        state["current"] = target
        STATS_PAGES[key] = state
    except Exception:
        pass

    return bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("pm_settings_open:"))
def cb_pm_settings_open(call: types.CallbackQuery):
    """Открытие настроек конкретной группы из ЛС (по кнопке из /settings в ЛС)."""
    if _is_duplicate_callback_query(call):
        return
    bot.answer_callback_query(call.id)
    try:
        chat_id = int(call.data.split(":")[1])
    except (ValueError, IndexError):
        return

    if call.message.chat.type != 'private':
        return

    user = call.from_user
    allowed, err = _user_can_open_settings(chat_id, user)
    if not allowed:
        return bot.answer_callback_query(call.id, err or "Нет прав.", show_alert=True)

    try:
        chat = bot.get_chat(chat_id)
        title = chat.title or str(chat_id)
    except Exception:
        title = str(chat_id)

    get_chat_settings(chat_id)

    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    text = (
        f"{emoji_settings} <b>Настройки чата</b>\n"
        f"<b>Чат:</b> {_html.escape(title)}\n\n"
        "Выберите раздел для настройки:"
    )
    kb = _build_settings_main_keyboard(chat_id, viewer_user=user)

    raw_edit_message_with_keyboard(
        call.message.chat.id,
        call.message.message_id,
        text,
        kb
    )


@bot.callback_query_handler(func=lambda call: bool(call.data) and not call.data.startswith((
    "gstats_",
    "punish_un:",
    "modlist:",
    "astchat:",
    "astnav:",
    "astclose:",
    "st_",
    "stw:",
    "p:",
    "rules:",
    "del:",
    "approve_group:",
    "deny_group:",
    "pm_settings_open:",
    "pm_settings_back",
)))
def callback_handler(call: types.CallbackQuery):
    if _is_duplicate_callback_query(call):
        return
    add_stat_message(call.message)
    _remember_owner_user_id(call.from_user)

    data = call.data or ""
    chat_id = call.message.chat.id
    msg_id = call.message.message_id

    if data.startswith("devcontact:"):
        action = data.split(":", 1)[1]

        if action == "back":
            try:
                raw_delete_message(chat_id, msg_id)
            except Exception:
                pass
            _send_start_menu(chat_id, call.from_user)
            return bot.answer_callback_query(call.id)

        if action == "send":
            try:
                raw_delete_message(chat_id, msg_id)
            except Exception:
                pass

            prompt = bot.send_message(
                chat_id,
                _dev_contact_prompt_text(),
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_markup=_dev_contact_prompt_kb(),
            )
            PENDING_DEV_CONTACT_FROM_USER[call.from_user.id] = {
                "prompt_message_id": prompt.message_id,
                "created_at": int(time.time()),
            }
            return bot.answer_callback_query(call.id)

        if action == "cancel":
            PENDING_DEV_CONTACT_FROM_USER.pop(call.from_user.id, None)
            PENDING_DEV_REPLY_FROM_OWNER.pop(call.from_user.id, None)
            try:
                raw_delete_message(chat_id, msg_id)
            except Exception:
                pass
            emoji_cancel = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
            bot.send_message(chat_id, f"<i>{emoji_cancel} Отправка отменена.</i>", parse_mode='HTML')
            return bot.answer_callback_query(call.id)

        return bot.answer_callback_query(call.id)

    if data.startswith("devmsg:"):
        if not is_owner(call.from_user):
            return bot.answer_callback_query(call.id, "Недоступно.", show_alert=True)

        parts = data.split(":")
        if len(parts) != 3:
            return bot.answer_callback_query(call.id)

        action = parts[1]
        try:
            item_id = int(parts[2])
        except Exception:
            return bot.answer_callback_query(call.id)

        item = _dev_contact_find_item(item_id)
        if item is None:
            return bot.answer_callback_query(call.id, "Сообщение не найдено.", show_alert=False)

        if action == "ignore":
            item["status"] = "ignored"
            save_dev_contact_inbox()
            try:
                raw_delete_message(chat_id, msg_id)
            except Exception:
                pass
            return bot.answer_callback_query(call.id, "Сообщение проигнорировано.", show_alert=False)

        if action == "reply":
            status = item.get("status") or "new"
            if status != "new":
                return bot.answer_callback_query(call.id, "Это сообщение уже обработано.", show_alert=False)

            try:
                raw_delete_message(chat_id, msg_id)
            except Exception:
                pass

            prompt = bot.send_message(
                chat_id,
                _dev_contact_prompt_text(),
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_markup=_dev_contact_prompt_kb(),
            )

            PENDING_DEV_REPLY_FROM_OWNER[call.from_user.id] = {
                "item_id": item_id,
                "target_user_id": int(item.get("user_id") or 0),
                "prompt_message_id": prompt.message_id,
                "created_at": int(time.time()),
            }
            return bot.answer_callback_query(call.id)

        return bot.answer_callback_query(call.id)

    if data.startswith("bc2:"):
        if not is_owner(call.from_user):
            return bot.answer_callback_query(call.id, "Недоступно.", show_alert=True)
        if call.message.chat.type != 'private':
            return bot.answer_callback_query(call.id)

        parts = data.split(":")
        if len(parts) != 3:
            return bot.answer_callback_query(call.id)

        action = parts[1]
        try:
            draft_id = int(parts[2])
        except Exception:
            return bot.answer_callback_query(call.id)

        draft = BROADCAST_DRAFTS.get(call.from_user.id)
        if not draft or int(draft.get("id") or 0) != draft_id:
            return bot.answer_callback_query(call.id, "Черновик не найден или устарел.", show_alert=False)

        if action in ("text", "media", "buttons"):
            old_pending = BROADCAST_PENDING_INPUT.pop(call.from_user.id, None)
            if old_pending:
                try:
                    old_prompt = int(old_pending.get("prompt_message_id") or 0)
                    if old_prompt > 0:
                        bot.delete_message(chat_id, old_prompt)
                except Exception:
                    pass

            if action == "text":
                prompt_text = (
                    "<b>Отправьте текст рассылки.</b>\n\n"
                    "Поддерживается обычное форматирование Telegram и кастомные теги бота."
                )
            elif action == "media":
                prompt_text = "<b>Отправьте медиа для рассылки.</b>\n\nПоддерживается фото/видео/файл/музыка/gif."
            else:
                prompt_text = (
                    "<b>Отправьте кнопки для рассылки.</b>\n\n"
                    "Формат: <code>Текст - ссылка</code> или <code>Текст - popup: сообщение</code>."
                )

            prompt = bot.send_message(chat_id, prompt_text, parse_mode='HTML', disable_web_page_preview=True)
            BROADCAST_PENDING_INPUT[call.from_user.id] = {
                "mode": action,
                "draft_id": draft_id,
                "prompt_message_id": prompt.message_id,
                "created_at": int(time.time()),
            }
            return bot.answer_callback_query(call.id, "Ожидаю сообщение в чате.", show_alert=False)

        if action == "preview":
            html_text = build_html_from_text_custom(draft.get("text_custom") or "")
            media = draft.get("media") or []
            buttons = draft.get("buttons") or {"rows": [], "popups": []}

            rows = buttons.get("rows") or []
            popups = buttons.get("popups") or []
            kb_preview = build_inline_keyboard_for_payload("broadcast", call.message.chat.id, rows, popups, call.from_user.id)

            if not html_text and not media and not rows:
                return bot.answer_callback_query(call.id, "Черновик пустой.", show_alert=True)

            bot.send_message(chat_id, "<b>Предпросмотр текущего сообщения:</b>", parse_mode='HTML', disable_web_page_preview=True)
            _send_payload(chat_id, html_text, media, reply_markup=kb_preview)
            return bot.answer_callback_query(call.id)

        if action == "reset":
            BROADCAST_DRAFTS[call.from_user.id] = _broadcast_new_draft()
            draft = BROADCAST_DRAFTS[call.from_user.id]

            text = _broadcast_render_panel_text(call.from_user.id)
            kb = _build_broadcast_panel_keyboard(int(draft.get("id") or 0))
            try:
                bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode='HTML', disable_web_page_preview=True, reply_markup=kb)
            except Exception:
                pass
            return bot.answer_callback_query(call.id, "Черновик очищен.", show_alert=False)

        if action == "cancel":
            BROADCAST_DRAFTS.pop(call.from_user.id, None)
            pending = BROADCAST_PENDING_INPUT.pop(call.from_user.id, None)
            if pending:
                try:
                    prompt_id = int(pending.get("prompt_message_id") or 0)
                    if prompt_id > 0:
                        bot.delete_message(chat_id, prompt_id)
                except Exception:
                    pass
            try:
                bot.edit_message_text(
                    "<i>Рассылка отменена.</i>",
                    chat_id=chat_id,
                    message_id=msg_id,
                    parse_mode='HTML',
                )
            except Exception:
                pass
            return bot.answer_callback_query(call.id, "Отменено", show_alert=False)

        if action == "send":
            html_text = build_html_from_text_custom(draft.get("text_custom") or "")
            media = draft.get("media") or []
            buttons = draft.get("buttons") or {"rows": [], "popups": []}
            rows = buttons.get("rows") or []

            if not html_text and not media and not rows:
                return bot.answer_callback_query(call.id, "Черновик пустой.", show_alert=True)

            targets = _broadcast_collect_targets()

            BROADCAST_DRAFTS.pop(call.from_user.id, None)
            BROADCAST_PENDING_INPUT.pop(call.from_user.id, None)

            operation_id, queue_size = enqueue_operation(
                "broadcast_send",
                {
                    "panel_chat_id": chat_id,
                    "panel_message_id": msg_id,
                    "owner_user_id": call.from_user.id,
                    "html_text": html_text,
                    "media": media,
                    "buttons": buttons,
                    "targets": targets,
                },
            )

            summary = (
                f'<tg-emoji emoji-id="{EMOJI_LOG_PM_ID}">📢</tg-emoji> <b>Рассылка поставлена в очередь.</b>\n\n'
                f"<b>Операция:</b> <code>#{operation_id}</code>\n"
                f"<b>Получателей:</b> <code>{len(targets)}</code>\n"
                f"<b>Позиция в очереди:</b> <code>{queue_size}</code>"
            )

            try:
                bot.edit_message_text(
                    summary,
                    chat_id=chat_id,
                    message_id=msg_id,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                )
            except Exception:
                bot.send_message(chat_id, summary, parse_mode='HTML', disable_web_page_preview=True)

            return bot.answer_callback_query(call.id, "Рассылка поставлена в очередь.", show_alert=False)

        return bot.answer_callback_query(call.id)

    if data.startswith("bc:"):
        return bot.answer_callback_query(call.id, "Используйте новый интерфейс /broadcast", show_alert=False)

    if data == 'start:close':
        START_MENU_STATE.pop((chat_id, msg_id), None)
        try:
            raw_delete_message(chat_id, msg_id)
        finally:
            return bot.answer_callback_query(call.id)

    if data in ('start:home', 'start:commands', 'start:about', 'start:usage'):
        state = START_MENU_STATE.get((chat_id, msg_id))
        if not state:
            return bot.answer_callback_query(call.id, "Меню устарело, открой /start заново.", show_alert=False)

        owner_id = int(state.get('user_id') or 0)
        if call.from_user.id != owner_id:
            return bot.answer_callback_query(call.id, "Это меню не для вас.", show_alert=False)

        show_owner_button = bool(state.get('show_owner_button'))

        if data == 'start:home':
            text = _build_start_home_text(call.from_user)
            kb = _build_start_home_keyboard(show_owner_button=show_owner_button)
        elif data == 'start:commands':
            text = _build_start_commands_text(call.from_user)
            kb = _build_start_commands_keyboard()
        elif data == 'start:about':
            text = _build_start_about_text()
            kb = _build_start_back_keyboard('start:home')
        else:
            text = _build_start_usage_text()
            kb = _build_start_back_keyboard('start:commands')

        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
        return bot.answer_callback_query(call.id)

    if data == 'start:contact':
        state = START_MENU_STATE.get((chat_id, msg_id))
        if not state:
            return bot.answer_callback_query(call.id, "Меню устарело, открой /start заново.", show_alert=False)

        owner_id = int(state.get('user_id') or 0)
        if call.from_user.id != owner_id:
            return bot.answer_callback_query(call.id, "Это меню не для вас.", show_alert=False)

        START_MENU_STATE.pop((chat_id, msg_id), None)
        try:
            raw_delete_message(chat_id, msg_id)
        except Exception:
            pass

        bot.send_message(
            chat_id,
            _dev_contact_intro_text(),
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=_dev_contact_intro_kb(),
        )
        return bot.answer_callback_query(call.id)

    if data == 'start:newmsgs':
        state = START_MENU_STATE.get((chat_id, msg_id))
        if not state:
            return bot.answer_callback_query(call.id, "Меню устарело, открой /start заново.", show_alert=False)

        owner_id = int(state.get('user_id') or 0)
        if call.from_user.id != owner_id:
            return bot.answer_callback_query(call.id, "Это меню не для вас.", show_alert=False)

        if not is_owner(call.from_user):
            return bot.answer_callback_query(call.id, "Кнопка доступна только разработчику.", show_alert=False)

        _show_dev_contact_new_messages(call.from_user.id)
        return bot.answer_callback_query(call.id, "Отправляю новые сообщения…", show_alert=False)

    bot.answer_callback_query(call.id)


# ==== ОДОБРЕНИЕ / ОТКАЗ ГРУПП ====

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("approve_group:"))
def cb_approve_group(call: types.CallbackQuery):
    """Разработчик одобрил группу."""
    if _is_duplicate_callback_query(call):
        return
    if not is_owner(call.from_user):
        return bot.answer_callback_query(call.id, "Только разработчик может одобрять группы.", show_alert=True)
    
    try:
        chat_id = int(call.data.split(":")[1])
    except (ValueError, IndexError):
        return bot.answer_callback_query(call.id, "Ошибка обработки данных.", show_alert=True)
    
    pending = PENDING_GROUPS.get(str(chat_id))
    if not pending:
        return bot.answer_callback_query(call.id, "Группа уже обработана.", show_alert=True)
    
    # Одобряем группу
    approve_pending_group(chat_id)
    
    emoji_ok = f'<tg-emoji emoji-id="{EMOJI_SENT_OK_ID}">✅</tg-emoji>'
    
    # Обновляем сообщение в ЛС разработчика
    try:
        bot.edit_message_text(
            f"<b>{emoji_ok} Группа одобрена</b>\n\n"
            f"<b>Группа:</b> {_html.escape(pending.get('title', 'Unknown'))}\n"
            f"<b>ID:</b> <code>{chat_id}</code>",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            parse_mode='HTML'
        )
    except Exception:
        pass
    
    # Отправляем сообщение в группу
    try:
        bot.send_message(
            chat_id,
            f"{emoji_ok} <b>Группа одобрена!</b>\n\n"
            f"Бот готов к работе. Для <b>стабильной работы всех функций</b> выдайте боту права администратора:\n\n"
            f"• <b>Удалять сообщения</b>\n"
            f"• <b>Закреплять сообщения</b>\n"
            f"• <b>Ограничивать участников</b>\n"
            f"• <b>Приглашать пользователей</b>\n"
            f"• <b>Управлять ссылками-приглашениями</b>\n"
            f"• <b>Назначать администраторов</b>\n\n"
            f"<i>Для этого откройте настройки группы → Администраторы → выберите бота и выдайте права.</i>",
            parse_mode='HTML'
        )
    except Exception as e:
        print(f"[ERROR] Не удалось отправить сообщение в группу {chat_id}: {e}")
    
    return bot.answer_callback_query(call.id, "Группа одобрена!", show_alert=False)


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("deny_group:"))
def cb_deny_group(call: types.CallbackQuery):
    """Разработчик запретил группе доступ."""
    if _is_duplicate_callback_query(call):
        return
    if not is_owner(call.from_user):
        return bot.answer_callback_query(call.id, "Только разработчик может отказывать группам.", show_alert=True)
    
    try:
        chat_id = int(call.data.split(":")[1])
    except (ValueError, IndexError):
        return bot.answer_callback_query(call.id, "Ошибка обработки данных.", show_alert=True)
    
    pending = PENDING_GROUPS.get(str(chat_id))
    if not pending:
        return bot.answer_callback_query(call.id, "Группа уже обработана.", show_alert=True)
    
    # Запрещаем доступ
    deny_pending_group(chat_id)
    
    emoji_cancel = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'
    
    # Обновляем сообщение в ЛС разработчика
    try:
        bot.edit_message_text(
            f"<b>{emoji_cancel} Группе запрещен доступ</b>\n\n"
            f"<b>Группа:</b> {_html.escape(pending.get('title', 'Unknown'))}\n"
            f"<b>ID:</b> <code>{chat_id}</code>",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            parse_mode='HTML'
        )
    except Exception:
        pass
    
    # Пытаемся выйти из группы
    try:
        bot.leave_chat(chat_id)
    except Exception as e:
        print(f"[ERROR] Не удалось выйти из группы {chat_id}: {e}")
        # Отправляем сообщение в группу, чтобы не работать
        try:
            bot.send_message(
                chat_id,
                f"<b>{emoji_cancel} Доступ запрещен</b>\n\nБот не имеет разрешения на работу в этой группе.",
                parse_mode='HTML'
            )
        except Exception:
            pass
    
    return bot.answer_callback_query(call.id, "Группе запрещен доступ. Бот пытается выйти.", show_alert=False)


@bot.my_chat_member_handler()
def handle_my_chat_member(update: types.ChatMemberUpdated):
    """Обработчик событий когда бот добавляется/удаляется из групп."""
    chat_id = update.chat.id
    
    # Если бот был удален из группы (и это неподтвержденная группа)
    if update.new_chat_member.status == "left":
        deny_pending_group(chat_id)
        print(f"[INFO] Бот удален из неподтвержденной группы {chat_id}")

# ==== ФОЛЛБЭК ====

@bot.message_handler(
    func=lambda m: True,
    content_types=['text', 'photo', 'video', 'document', 'audio', 'animation', 'voice', 'video_note', 'sticker'],
)
def all_other(m: types.Message):
    add_stat_message(m)
    # всё остальное учитываем в статистике и БД пользователей

print("Бот запущен, infinity polling...")
bot.infinity_polling(timeout=60, long_polling_timeout=60)

