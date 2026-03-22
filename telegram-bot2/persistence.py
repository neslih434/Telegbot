"""
persistence.py — SQLite/JSON, throttled saves, TG кеш, callback-дедупликация,
загрузка всех state-словарей, STATS.
Импортирует только из config. Бизнес-логика — в helpers.py.
"""
from __future__ import annotations

import time
import threading
import atexit
import queue

from config import (
    # stdlib re-exports нужны для typing внутри этого модуля
    os, json, sqlite3, Any,
    # настройки
    DATA_DIR, SQLITE_DB_FILE, SQLITE_JSON_FALLBACK_WRITE,
    DB_FLUSH_INTERVAL_SECONDS, GLOBAL_LAST_SEEN_UPDATE_SECONDS,
    TG_CACHE_MEMBER_TTL, TG_CACHE_CHAT_TTL,
    # bot для кеша
    bot,
    # file paths
    GROUP_STATS_FILE, GROUP_SETTINGS_FILE, PROFILES_FILE, USERS_FILE,
    VERIFY_ADMINS_FILE, VERIFY_DEV_FILE, GLOBAL_USERS_FILE,
    CHAT_SETTINGS_FILE, MODERATION_FILE, DEV_CONTACT_INBOX_FILE,
    DEV_CONTACT_META_FILE, PENDING_GROUPS_FILE,
    CLOSE_CHAT_FILE, CHAT_ROLES_FILE, ROLE_PERMS_FILE,
    # types
    types,
)

# ─────────────────────────── SQLite / JSON ───────────────────────────────────

_DB_LOCK = threading.RLock()
_DB_CONN: sqlite3.Connection | None = None


def _stats_increment(key: str, delta: int = 1) -> None:
    STATS[key] = int(STATS.get(key) or 0) + delta


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
    """Атомарный fallback-save в legacy JSON."""
    try:
        tmp = path + ".tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"Ошибка fallback-сохранения JSON {path}: {e}")


def _known_json_store_paths() -> list[str]:
    import config as _cfg
    out: list[str] = []
    for name in dir(_cfg):
        if not name.endswith("_FILE"):
            continue
        value = getattr(_cfg, name, None)
        if not isinstance(value, str) or not value.endswith(".json"):
            continue
        out.append(value)
    return list(dict.fromkeys(out))


def migrate_legacy_json_to_sqlite() -> dict[str, int]:
    migrated = skipped = failed = 0
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
            key = _db_key(path)
            conn = _db_connect()
            with _DB_LOCK:
                row = conn.execute(
                    "SELECT payload_json FROM kv_store WHERE store_key = ?", (key,)
                ).fetchone()
            if not row:
                failed += 1
                continue
            migrated += 1
        except Exception:
            failed += 1
    return {"migrated": migrated, "skipped": skipped, "failed": failed, "total": len(paths)}


def get_sqlite_status() -> dict[str, Any]:
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
            status["keys"] = [
                r[0] for r in conn.execute(
                    "SELECT store_key FROM kv_store ORDER BY updated_at DESC LIMIT 10"
                ).fetchall()
            ]
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
                    "SELECT payload_json FROM kv_store WHERE store_key = ?", (key,)
                ).fetchone()
            if row:
                return json.loads(row[0])
            break
        except Exception:
            _stats_increment("sqlite_errors")
            if attempt == 0:
                _reset_db_connection()
                continue
            break

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


# ─────────────────────────── Throttled saves ─────────────────────────────────

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
    force_flush_all_saves()
    close_sqlite_connection()


_FLUSH_THREAD = threading.Thread(target=_periodic_flush_worker, daemon=True)
_FLUSH_THREAD.start()
atexit.register(_shutdown_persistence)

# ─────────────────────────── TG кеш / дедупликация ──────────────────────────

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
    key = (int(chat_id), int(user_id))
    with _TG_CACHE_LOCK:
        _TG_MEMBER_CACHE.pop(key, None)


def tg_invalidate_chat_cache(chat_ref: Any) -> None:
    key = _tg_chat_cache_key(chat_ref)
    with _TG_CACHE_LOCK:
        _TG_CHAT_CACHE.pop(key, None)


def tg_invalidate_chat_member_caches(chat_id: int, user_id: int) -> None:
    tg_invalidate_member_cache(chat_id, user_id)
    tg_invalidate_chat_cache(chat_id)


# ─────────── Кеш bot.get_chat(user_id) на время одной задачи воркера ─────────
# Сбрасывается в начале каждой задачи TeleBot (см. install_telebot_user_fetch_cache_hooks).
# Не TTL: в рамках одного апдейта повторные запросы того же user_id не бьют API.
_USER_FETCH_TLS = threading.local()


def tg_user_fetch_scope_reset() -> None:
    """Очистить кеш get_user для текущего потока (новая задача воркера / новый апдейт)."""
    d = getattr(_USER_FETCH_TLS, "by_id", None)
    if d is not None:
        d.clear()


def tg_get_user_by_id_cached(user_id: int) -> Any:
    """
    bot.get_chat(user_id) для числового ID с дедупликацией в рамках текущей задачи воркера.
    При ошибке API — тот же fallback, что и раньше в helpers (минимальный User).
    """
    uid = int(user_id)
    d = getattr(_USER_FETCH_TLS, "by_id", None)
    if d is None:
        d = {}
        _USER_FETCH_TLS.by_id = d
    hit = uid in d
    if hit:
        _stats_increment("tg_user_fetch_hits")
        return d[uid]
    _stats_increment("tg_user_fetch_misses")
    try:
        obj = bot.get_chat(uid)
    except Exception:
        obj = types.User(uid, False, first_name="", last_name=None, username=None)
    d[uid] = obj
    return obj


def install_telebot_user_fetch_cache_hooks() -> None:
    """
    Патчит TeleBot._exec_task: перед каждым обработчиком в воркере сбрасывается кеш пользователей.
    Вызывается автоматически при импорте persistence (после загрузки state).
    """
    from telebot import TeleBot

    if getattr(TeleBot, "_user_fetch_cache_patched", False):
        return

    def _exec_task(self, task, *args, **kwargs):
        if self.threaded:

            def wrapped(*a, **kw):
                tg_user_fetch_scope_reset()
                return task(*a, **kw)

            self.worker_pool.put(wrapped, *args, **kwargs)
        else:
            try:
                tg_user_fetch_scope_reset()
                task(*args, **kwargs)
            except Exception as e:
                handled = self._handle_exception(e)
                if not handled:
                    raise e

    TeleBot._exec_task = _exec_task  # type: ignore[assignment]
    TeleBot._user_fetch_cache_patched = True


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
        'total_misses': (
            int(STATS.get('tg_cache_member_misses') or 0)
            + int(STATS.get('tg_cache_chat_misses') or 0)
        ),
        'user_fetch_hits': int(STATS.get('tg_user_fetch_hits') or 0),
        'user_fetch_misses': int(STATS.get('tg_user_fetch_misses') or 0),
    }


def _is_duplicate_callback_query(call: types.CallbackQuery) -> bool:
    data = (call.data or "").strip()
    if not data:
        return False
    user_id = int(getattr(call.from_user, "id", 0) or 0)
    bucket = int(time.time() // CALLBACK_DEDUPE_BUCKET_SECONDS)
    min_bucket = bucket - CALLBACK_DEDUPE_KEEP_BUCKETS
    key = (user_id, data, bucket)
    with _CALLBACK_DEDUPE_LOCK:
        stale_keys = [k for k in _CALLBACK_DEDUPE if k[2] < min_bucket]
        for sk in stale_keys:
            _CALLBACK_DEDUPE.pop(sk, None)
        if key in _CALLBACK_DEDUPE:
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            return True
        _CALLBACK_DEDUPE[key] = time.monotonic()
    return False

# ─────────────────────────── Загрузка state-словарей ─────────────────────────

VERIFY_ADMINS: dict = load_json_file(VERIFY_ADMINS_FILE, {})
VERIFY_DEV: set = set(load_json_file(VERIFY_DEV_FILE, []))

DEV_CONTACT_INBOX: dict = load_json_file(DEV_CONTACT_INBOX_FILE, {"last_id": 0, "items": []})
if not isinstance(DEV_CONTACT_INBOX, dict):
    DEV_CONTACT_INBOX = {"last_id": 0, "items": []}
DEV_CONTACT_INBOX.setdefault("last_id", 0)
DEV_CONTACT_INBOX.setdefault("items", [])

DEV_CONTACT_META: dict = load_json_file(DEV_CONTACT_META_FILE, {})
if not isinstance(DEV_CONTACT_META, dict):
    DEV_CONTACT_META = {}

CLOSE_CHAT_STATE: dict = load_json_file(CLOSE_CHAT_FILE, {})

GROUP_STATS: dict = load_json_file(GROUP_STATS_FILE, {})
GROUP_SETTINGS: dict = load_json_file(GROUP_SETTINGS_FILE, {})

CHAT_SETTINGS: dict = load_json_file(CHAT_SETTINGS_FILE, {})
MODERATION: dict = load_json_file(MODERATION_FILE, {})
PENDING_GROUPS: dict = load_json_file(PENDING_GROUPS_FILE, {})

USERS: dict = load_json_file(USERS_FILE, {})
GLOBAL_USERS: dict = load_json_file(GLOBAL_USERS_FILE, {})
PROFILES: dict = load_json_file(PROFILES_FILE, {})

CHAT_ROLES: dict = load_json_file(CHAT_ROLES_FILE, {})
ROLE_PERMS: dict = load_json_file(ROLE_PERMS_FILE, {})

# Volatile state (не персистируется)
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

# STATS (определён здесь, _stats_increment использует его напрямую)
STATS: dict[str, Any] = {
    'users': set(),
    'chats': set(),
    'messages': 0,
    'commands_used': {},
    'start_time': time.time(),
    'sqlite_errors': 0,
    'tg_cache_chat_misses': 0,
    'tg_cache_member_misses': 0,
    'tg_user_fetch_hits': 0,
    'tg_user_fetch_misses': 0,
}

# ─────────────────────────── Чистые save-функции ─────────────────────────────

def save_verify_admins():
    throttled_save_json_file(VERIFY_ADMINS_FILE, VERIFY_ADMINS, "verify_admins")

def save_verify_dev():
    save_json_file(VERIFY_DEV_FILE, list(VERIFY_DEV))

def save_dev_contact_inbox():
    save_json_file(DEV_CONTACT_INBOX_FILE, DEV_CONTACT_INBOX)

def save_dev_contact_meta():
    save_json_file(DEV_CONTACT_META_FILE, DEV_CONTACT_META)

def save_close_chat_state():
    save_json_file(CLOSE_CHAT_FILE, CLOSE_CHAT_STATE)

def save_group_stats():
    throttled_save_json_file(GROUP_STATS_FILE, GROUP_STATS, "group_stats")

def save_group_settings():
    throttled_save_json_file(GROUP_SETTINGS_FILE, GROUP_SETTINGS, "group_settings")

def save_chat_settings():
    throttled_save_json_file(CHAT_SETTINGS_FILE, CHAT_SETTINGS, "chat_settings")

def save_moderation():
    throttled_save_json_file(MODERATION_FILE, MODERATION, "moderation")

def save_pending_groups():
    save_json_file(PENDING_GROUPS_FILE, PENDING_GROUPS)

def save_users():
    throttled_save_json_file(USERS_FILE, USERS, "users")

def save_global_users():
    throttled_save_json_file(GLOBAL_USERS_FILE, GLOBAL_USERS, "global_users")

def save_profiles():
    throttled_save_json_file(PROFILES_FILE, PROFILES, "profiles")

def save_chat_roles():
    throttled_save_json_file(CHAT_ROLES_FILE, CHAT_ROLES, "chat_roles")

def save_role_perms():
    throttled_save_json_file(ROLE_PERMS_FILE, ROLE_PERMS, "role_perms")


install_telebot_user_fetch_cache_hooks()
