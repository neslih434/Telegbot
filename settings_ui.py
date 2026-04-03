"""
settings_ui.py — Настройки чата:
  /settings, welcome/farewell/rules/cleanup UI,
  on_welcome_new_members (new_chat_members — ПОСЛЕ helpers.on_new_members),
  left_chat_member, rules trigger,
  cleanup_delete_commands_runtime, cleanup_delete_system_runtime.
"""
from __future__ import annotations
import time
import threading
import asyncio
import re as _re
import html as _html

from config import (
    os, json, re, random, datetime,
    Any, Dict, List, Optional, Tuple,
    types, apihelper, telebot, ContinueHandling,
    ApiTelegramException, InlineKeyboardMarkup, InlineKeyboardButton,
    bot, bot_raw, tg_client,
    TOKEN, OWNER_USERNAME, DATA_DIR, API_BASE_URL,
    COMMAND_PREFIXES, MAX_MSG_LEN,
    PREMIUM_PREFIX_EMOJI_ID, EMOJI_RATE_LIMIT_ID,
    EMOJI_DEV_ID, EMOJI_MEMBER_ID, EMOJI_ADMIN_ID, EMOJI_OWNER_ID,
    EMOJI_PROFILE_ID, EMOJI_MSG_COUNT_ID, EMOJI_DESC_ID,
    EMOJI_AWARDS_BLOCK_ID, EMOJI_PREMIUM_STATUS_ID,
    EMOJI_VERIFY_ADMIN_ID, EMOJI_VERIFY_DEV_ID,
    EMOJI_ROLE_OWNER_ID, EMOJI_ROLE_CHIEF_ADMIN_ID, EMOJI_ROLE_ADMIN_ID,
    EMOJI_ROLE_MOD_ID, EMOJI_ROLE_TRAINEE_ID,
    EMOJI_USER_ROLE_TEXT_ID, EMOJI_ROLE_ACTION_ID,
    EMOJI_SCOPE_GROUP_ID, EMOJI_SCOPE_PM_ID, EMOJI_SCOPE_ALL_ID,
    EMOJI_LIST_ID, EMOJI_ADMIN_RIGHTS_ID, EMOJI_BTN_UNADMIN_ID, EMOJI_BTN_KICK_ID,
    EMOJI_PING_ID, EMOJI_LOG_ID, EMOJI_LOG_PM_ID,
    EMOJI_CHAT_CLOSED_ID, EMOJI_CHAT_OPEN_BTN_ID,
    EMOJI_SENT_OK_ID, EMOJI_NEW_MSG_OWNER_ID, EMOJI_BOT_VERSION_ID,
    EMOJI_CONTACT_DEV_ID, EMOJI_SEND_TEXT_PROMPT_ID,
    EMOJI_REPLY_BTN_ID, EMOJI_IGNORE_BTN_ID, EMOJI_REPLY_RECEIVED_ID,
    EMOJI_LEGEND_ANYWHERE_ID, EMOJI_LEGEND_DEV_ONLY_ID,
    EMOJI_LEGEND_DEV_OR_VERIFIED_ID, EMOJI_LEGEND_GROUP_ADMIN_ID,
    EMOJI_LEGEND_PM_ONLY_ID, EMOJI_LEGEND_GROUP_ONLY_ID,
    EMOJI_LEGEND_ALL_USERS_ID,
    AWARD_EMOJI_IDS,
    EMOJI_WELCOME_TEXT_ID, EMOJI_WELCOME_MEDIA_ID, EMOJI_WELCOME_BUTTONS_ID,
    EMOJI_LEFT_ID,
    get_user_id_by_username_mtproto,
)
from persistence import (
    VERIFY_ADMINS, VERIFY_DEV,
    DEV_CONTACT_INBOX, DEV_CONTACT_META,
    PENDING_DEV_CONTACT_FROM_USER, PENDING_DEV_REPLY_FROM_OWNER,
    BROADCAST_DRAFTS, BROADCAST_PENDING_INPUT,
    CLOSE_CHAT_STATE, GROUP_STATS, GROUP_SETTINGS,
    CHAT_SETTINGS, MODERATION, PENDING_GROUPS,
    USERS, GLOBAL_USERS, PROFILES,
    CHAT_ROLES, ROLE_PERMS,
    STATS,
    save_verify_admins, save_verify_dev,
    save_dev_contact_inbox, save_dev_contact_meta,
    save_close_chat_state,
    save_group_stats, save_group_settings,
    save_chat_settings, save_moderation, save_pending_groups,
    save_users, save_global_users, save_profiles,
    save_chat_roles, save_role_perms,
    tg_get_chat, tg_get_chat_member,
    tg_invalidate_member_cache, tg_invalidate_chat_cache,
    tg_invalidate_chat_member_caches,
    load_json_file, save_json_file, throttled_save_json_file,
    get_sqlite_status, migrate_legacy_json_to_sqlite,
    _is_duplicate_callback_query,
    get_tg_cache_stats,
    GLOBAL_LAST_SEEN_UPDATE_SECONDS,
)
from helpers import *
from helpers import _user_can_open_settings, _user_can_edit_now, _build_ranks_keyboard

# Константы наказаний (дублируем из moderation.py, чтобы не создавать цикличных импортов)
MIN_PUNISH_SECONDS = 60
MAX_PUNISH_SECONDS = 365 * 24 * 60 * 60
from moderation import (
    _mod_get_chat, _mod_save, _mod_duration_text,
    _parse_duration_prefix, _build_open_pm_markup,
    _is_farewell_suppressed,
    _mark_farewell_suppressed,
    _mod_new_action_id, _mod_log_append, _mod_warn_add,
    _auto_punish_for_warns,
    _apply_mute, _apply_ban,
)
from pin import _should_keep_pin_service_message, _try_delete_last_bot_service_pin
from cmd_basic import _broadcast_render_panel_text, _build_broadcast_panel_keyboard

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

    section_desc = {
        "welcome": "Отправляет приветственное сообщение, когда пользователь входит в группу.",
        "farewell": "Отправляет прощальное сообщение, когда пользователь выходит из группы.",
        "rules": "Показывает правила группы по кнопке или команде.",
    }.get(sec, "")

    desc_block = f"{_html.escape(section_desc)}\n\n" if section_desc else ""

    return (
        f"{emoji_settings} <b>Настройки {_section_title(sec)}</b>\n\n"
        f"{desc_block}"
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
    enabled_sys_txt = str(len(enabled_sys)) if enabled_sys else "нет"

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
        f"<b>Команды:</b> <code>{_html.escape(enabled_cmds_txt)}</code>\n"
        f"<b>Системные сообщения:</b> <code>{_html.escape(enabled_sys_txt)}</code>\n"
        "<i>Если у бота нет права “Удалять сообщения”, удаление работать не будет.</i>"
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
        "Удаляет сообщения, которые начинаются с выбранного знака.\n"
        f"\n<b>Включены:</b> <code>{_html.escape(enabled_txt)}</code>"
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
        "Удаляет выбранные системные сообщения.\n"
        f"\n<b>Включено: количество включенных</b> <code>{len(enabled)}</code>"
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
        btn_kick = InlineKeyboardButton("Исключение", callback_data=f"st_warn_ptype:{chat_id}:kick")
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


ANTIFLOOD_TIME_PRESETS = (3, 4, 5, 6, 7, 8, 9, 10, 15, 20)
ANTIFLOOD_MESSAGES_PRESETS = (3, 4, 5, 6, 7, 8, 9, 10, 15, 20)
ANTIFLOOD_DURATION_PRESETS = (
    (60 * 10, "10м"),
    (60 * 30, "30м"),
    (60 * 60, "1ч"),
    (6 * 60 * 60, "6ч"),
    (12 * 60 * 60, "12ч"),
    (24 * 60 * 60, "1д"),
    (3 * 24 * 60 * 60, "3д"),
    (7 * 24 * 60 * 60, "7д"),
)


def _antiflood_type_label(ptype: str) -> str:
    return {
        "mute": "Ограничение",
        "ban": "Блокировка",
        "kick": "Исключение",
        "warn": "Предупреждение",
    }.get((ptype or "").lower(), "Ограничение")


def _antiflood_get_settings(chat_id: int) -> dict:
    settings = (_mod_get_chat(chat_id).get("settings") or {})
    af = settings.get("antiflood") or {}
    return {
        "enabled": bool(af.get("enabled", False)),
        "delete_messages": bool(af.get("delete_messages", False)),
        "period": int(af.get("period") or 10),
        "messages": int(af.get("messages") or 6),
        "punish": af.get("punish") or {"type": "mute", "duration": 30 * 60, "reason": ""},
    }


def _render_antiflood_settings_local(chat_id: int, page: str = "main") -> str:
    af = _antiflood_get_settings(chat_id)
    enabled = bool(af.get("enabled"))
    delete_messages = bool(af.get("delete_messages"))
    period = int(af.get("period") or 10)
    messages = int(af.get("messages") or 6)
    punish = af.get("punish") or {}
    ptype = (punish.get("type") or "mute").lower()
    duration = punish.get("duration")

    emoji_settings = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_SENT_PM_ID}">⚙️</tg-emoji>'
    emoji_ok = f'<tg-emoji emoji-id="{EMOJI_UNPUNISH_ID}">✅</tg-emoji>'
    emoji_x = f'<tg-emoji emoji-id="{EMOJI_ROLE_SETTINGS_CANCEL_ID}">❌</tg-emoji>'

    status_line = f"{emoji_ok} Включён" if enabled else f"{emoji_x} Выключен"
    delete_line = f"{emoji_ok} Включено" if delete_messages else f"{emoji_x} Выключено"
    ptype_line = _antiflood_type_label(ptype)
    duration_line = "Не используется" if ptype == "kick" else _mod_duration_text(int(duration or 0))

    hint = ""
    if page == "time":
        hint = "\n\n<i>Установите временное окно (в секундах), за которое считаются сообщения.</i>"
    elif page == "messages":
        hint = "\n\n<i>Установите лимит сообщений в выбранном временном окне.</i>"
    elif page == "punish":
        hint = "\n\n<i>Выберите наказание за превышение лимита антифлуда.</i>"
    elif page == "duration":
        if ptype == "kick":
            hint = "\n\nДля выбранного типа наказания длительность не используется."
        else:
            hint = "\n\n<i>Установите длительность наказания.</i>"

    return (
        f"{emoji_settings} <b>Настройки антифлуда</b>\n\n"
        "Автоматически наказывает пользователя, если он отправит определённое количество сообщений за заданный период.\n\n"
        f"<b>Статус:</b> {status_line}\n"
        f"<b>Удаление сообщений:</b> {delete_line}\n"
        f"<b>Время:</b> <code>{period}</code> сек\n"
        f"<b>Сообщения:</b> <code>{messages}</code>\n"
        f"<b>Наказание</b> <code>{_html.escape(ptype_line)}</code>\n"
        f"<b>Длительность:</b> <code>{_html.escape(duration_line)}</code>"
        f"{hint}"
    )


def _build_antiflood_settings_keyboard_local(chat_id: int, page: str = "main") -> InlineKeyboardMarkup:
    af = _antiflood_get_settings(chat_id)
    enabled = bool(af.get("enabled"))
    delete_messages = bool(af.get("delete_messages"))
    period = int(af.get("period") or 10)
    messages = int(af.get("messages") or 6)
    punish = af.get("punish") or {}
    ptype = (punish.get("type") or "mute").lower()
    duration = int(punish.get("duration") or 30 * 60)

    kb = InlineKeyboardMarkup(row_width=3)

    b_status = InlineKeyboardButton("Статус", callback_data=f"stf:toggle:{chat_id}")
    try:
        b_status.icon_custom_emoji_id = str(EMOJI_UNPUNISH_ID if enabled else EMOJI_ROLE_SETTINGS_CANCEL_ID)
        b_status.style = "success" if enabled else "danger"
    except Exception:
        pass
    kb.add(b_status)

    b_delete = InlineKeyboardButton("Удаление сообщений", callback_data=f"stf:deltoggle:{chat_id}")
    try:
        b_delete.style = "success" if delete_messages else "danger"
    except Exception:
        pass
    kb.add(b_delete)

    b_messages_text = "»Сообщения«" if page == "messages" else "Сообщения"
    b_time_text = "»Время«" if page == "time" else "Время"
    b_punish_text = "»Наказание«" if page == "punish" else "Наказание"
    b_duration_text = "»Длительность«" if page == "duration" else "Длительность"

    b_messages = InlineKeyboardButton(b_messages_text, callback_data=f"stf:page:{chat_id}:messages")
    b_time = InlineKeyboardButton(b_time_text, callback_data=f"stf:page:{chat_id}:time")
    b_punish = InlineKeyboardButton(b_punish_text, callback_data=f"stf:page:{chat_id}:punish")
    b_duration = InlineKeyboardButton(b_duration_text, callback_data=f"stf:page:{chat_id}:duration")

    try:
        if page == "messages":
            b_messages.style = "primary"
        if page == "time":
            b_time.style = "primary"
        if page == "punish":
            b_punish.style = "primary"
        if page == "duration":
            b_duration.style = "primary"
    except Exception:
        pass

    kb.row(b_messages, b_time)

    if page == "time":
        row: list[InlineKeyboardButton] = []
        for sec in ANTIFLOOD_TIME_PRESETS:
            b = InlineKeyboardButton(str(sec), callback_data=f"stf:time:{chat_id}:{sec}")
            try:
                if period == sec:
                    b.style = "primary"
            except Exception:
                pass
            row.append(b)
        for i in range(0, len(row), 5):
            kb.row(*row[i:i + 5])

    if page == "messages":
        row = []
        for count in ANTIFLOOD_MESSAGES_PRESETS:
            b = InlineKeyboardButton(str(count), callback_data=f"stf:msgs:{chat_id}:{count}")
            try:
                if messages == count:
                    b.style = "primary"
            except Exception:
                pass
            row.append(b)
        for i in range(0, len(row), 5):
            kb.row(*row[i:i + 5])

    kb.row(b_punish, b_duration)

    if page == "punish":
        b_mute = InlineKeyboardButton("Ограничение", callback_data=f"stf:ptype:{chat_id}:mute")
        b_ban = InlineKeyboardButton("Блокировка", callback_data=f"stf:ptype:{chat_id}:ban")
        b_kick = InlineKeyboardButton("Исключение", callback_data=f"stf:ptype:{chat_id}:kick")
        b_warn = InlineKeyboardButton("Предупреждение", callback_data=f"stf:ptype:{chat_id}:warn")
        for btn, p_key in ((b_mute, "mute"), (b_ban, "ban"), (b_kick, "kick"), (b_warn, "warn")):
            try:
                if ptype == p_key:
                    btn.style = "primary"
            except Exception:
                pass
        kb.row(b_mute, b_ban)
        kb.row(b_kick, b_warn)

    if page == "duration":
        b_set = InlineKeyboardButton("Установить длительность", callback_data=f"stf:dur_prompt:{chat_id}")
        try:
            b_set.style = "primary"
        except Exception:
            pass
        kb.add(b_set)

    b_back = InlineKeyboardButton("Назад", callback_data=f"st_back_main:{chat_id}")
    try:
        b_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
        b_back.style = "primary"
    except Exception:
        pass
    kb.add(b_back)

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

    btn_antiflood = InlineKeyboardButton("Антифлуд", callback_data=f"stf:open:{chat_id}")
    try:
        btn_antiflood.icon_custom_emoji_id = "5451732530048802485"
    except Exception:
        pass

    btn_antispam = InlineKeyboardButton("Анти-спам", callback_data=f"stas:open:{chat_id}")
    try:
        btn_antispam.icon_custom_emoji_id = "5467666648016358327"
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
        kb.add(btn_warns, btn_antiflood)
        kb.add(btn_antispam, btn_roles)
    else:
        kb.add(btn_warns, btn_antiflood)
        kb.add(btn_antispam)

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


def _is_antiflood_settings_callback_data(data: str) -> bool:
    return bool(data) and data.startswith("stf:")


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
            hint = "\n\nДля наказания «Исключение» длительность не устанавливается."
        else:
            hint = "\n\n<i>Установите время наказания.</i>"

    return (
        f"{emoji_settings} <b>Настройки предупреждений</b>\n\n"
        "Автоматически применяет наказание, когда пользователь достигает лимита предупреждений.\n\n"
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

    b_count_title = "»Количество«" if page == "count" else "Количество"
    b_punish_title = "»Наказание«" if page == "punish" else "Наказание"
    b_duration_title = "»Длительность«" if page == "duration" else "Длительность"

    b_count = InlineKeyboardButton(b_count_title, callback_data=f"stw:page:{chat_id}:count")
    b_punish = InlineKeyboardButton(b_punish_title, callback_data=f"stw:page:{chat_id}:punish")
    b_duration = InlineKeyboardButton(b_duration_title, callback_data=f"stw:page:{chat_id}:duration")

    try:
        if page == "count":
            b_count.style = "primary"
        if page == "punish":
            b_punish.style = "primary"
        if page == "duration":
            b_duration.style = "primary"
    except Exception:
        pass

    kb.row(b_count)

    if page == "count":
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

    kb.row(b_punish, b_duration)

    if page == "punish":
        b_mute = InlineKeyboardButton("Ограничение", callback_data=f"stw:ptype:{chat_id}:mute")
        b_ban = InlineKeyboardButton("Блокировка", callback_data=f"stw:ptype:{chat_id}:ban")
        b_kick = InlineKeyboardButton("Исключение", callback_data=f"stw:ptype:{chat_id}:kick")
        for btn, p_key in ((b_mute, "mute"), (b_ban, "ban"), (b_kick, "kick")):
            try:
                if ptype == p_key:
                    btn.style = "primary"
            except Exception:
                pass
        kb.row(b_mute, b_ban, b_kick)

    if page == "duration" and ptype in ("mute", "ban"):
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
            bot.answer_callback_query(c.id, "Для исключения длительность не используется.", show_alert=True)
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


@bot.callback_query_handler(func=lambda c: _is_antiflood_settings_callback_data(c.data or ""))
def cb_antiflood_settings_only(c: types.CallbackQuery):
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
    af = settings.get("antiflood") or {}
    punish = af.get("punish") or {}

    page = "main"

    if action != "dur_prompt":
        _pending_pop("pending_antiflood_duration", user.id)
        _pending_msg_pop("pending_antiflood_duration_msg", user.id)

    if action == "open":
        page = "main"
    elif action == "toggle":
        af["enabled"] = not bool(af.get("enabled", False))
        settings["antiflood"] = af
        ch["settings"] = settings
        _mod_save()
    elif action == "deltoggle":
        af["delete_messages"] = not bool(af.get("delete_messages", False))
        settings["antiflood"] = af
        ch["settings"] = settings
        _mod_save()
    elif action == "time":
        try:
            sec = int(extra)
        except Exception:
            bot.answer_callback_query(c.id)
            return
        af["period"] = max(3, min(300, sec))
        settings["antiflood"] = af
        ch["settings"] = settings
        _mod_save()
        page = "time"
    elif action == "msgs":
        try:
            count = int(extra)
        except Exception:
            bot.answer_callback_query(c.id)
            return
        af["messages"] = max(2, min(50, count))
        settings["antiflood"] = af
        ch["settings"] = settings
        _mod_save()
        page = "messages"
    elif action == "ptype":
        ptype = (extra or "").strip().lower()
        if ptype in ("mute", "ban", "kick", "warn"):
            punish["type"] = ptype
            if ptype == "kick":
                punish["duration"] = None
            elif punish.get("duration") is None:
                punish["duration"] = 30 * 60
            af["punish"] = punish
            settings["antiflood"] = af
            ch["settings"] = settings
            _mod_save()
        page = "punish"
    elif action == "dur":
        try:
            sec = int(extra)
        except Exception:
            bot.answer_callback_query(c.id)
            return
        ptype = (punish.get("type") or "mute").lower()
        if ptype != "kick":
            punish["duration"] = max(MIN_PUNISH_SECONDS, min(MAX_PUNISH_SECONDS, sec))
            af["punish"] = punish
            settings["antiflood"] = af
            ch["settings"] = settings
            _mod_save()
        page = "duration"
    elif action == "dur_prompt":
        ptype = (punish.get("type") or "mute").lower()
        if ptype == "kick":
            bot.answer_callback_query(c.id, "Для исключения длительность не используется.", show_alert=True)
            return

        _pending_put("pending_antiflood_duration", user.id, chat_id)
        _delete_pending_ui(msg_chat.id, "pending_antiflood_duration_msg", user.id, also_msg_id=c.message.message_id)

        kb_prompt = InlineKeyboardMarkup(row_width=1)
        b_back = InlineKeyboardButton("Назад", callback_data=f"stf:open:{chat_id}")
        try:
            b_back.icon_custom_emoji_id = str(EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID)
            b_back.style = "primary"
        except Exception:
            pass
        kb_prompt.add(b_back)

        prompt_text = (
            "<b>Установите длительность наказания для антифлуда</b>\n\n"
            "<b>Подсказка по интервалам:</b>\n"
            "<code>m</code> - минуты, <code>h</code> - часы, <code>d</code> - дни, <code>w</code> - недели, <code>mou</code> - месяцы, <code>y</code> - годы\n"
            "<code>м</code> - минуты, <code>мин</code> - минуты, <code>ч</code> - часы, <code>д</code> - дни, <code>н</code> - недели, <code>мес</code> - месяцы, <code>г</code> - годы\n"
            "Можно комбинировать до <b>3</b> интервалов.\n\n"
            "<b>Примеры:</b> <code>10m</code>, <code>1h 30m</code>, <code>2д</code>, <code>навсегда</code>."
        )

        sent = bot.send_message(
            msg_chat.id,
            prompt_text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb_prompt,
        )
        _pending_msg_set("pending_antiflood_duration_msg", user.id, sent.message_id)
        bot.answer_callback_query(c.id)
        return
    elif action == "page":
        if extra in ("time", "messages", "punish", "duration"):
            page = extra
        else:
            page = "main"
    else:
        bot.answer_callback_query(c.id)
        return

    text = _render_antiflood_settings_local(chat_id, page=page)
    kb = _build_antiflood_settings_keyboard_local(chat_id, page=page)
    if not _show_warn_settings_ui(msg_chat.id, c.message.message_id, text, kb):
        bot.answer_callback_query(c.id, "Не удалось открыть раздел антифлуда.", show_alert=True)
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
        elif sec == "antiflood":
            try:
                text = _render_antiflood_settings_local(chat_id, page="main")
                kb = _build_antiflood_settings_keyboard_local(chat_id, page="main")
            except Exception:
                bot.answer_callback_query(c.id, "Не удалось открыть раздел антифлуда.", show_alert=True)
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

    # ---------------- CUSTOM ANTIFLOOD DURATION ----------------
    antiflood_pending_cid = _pending_get("pending_antiflood_duration").get(str(user_id))
    if antiflood_pending_cid:
        if ct != "text":
            kb_err = InlineKeyboardMarkup(row_width=1)
            kb_err.add(InlineKeyboardButton("Назад", callback_data=f"stf:open:{antiflood_pending_cid}"))
            _replace_pending_ui(
                m.chat.id,
                "pending_antiflood_duration_msg",
                user_id,
                premium_prefix("Пришлите длительность текстом: 30m, 2h, 3д, 1н или 'навсегда'."),
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

        try:
            chat_id = int(antiflood_pending_cid)
        except Exception:
            _pending_pop("pending_antiflood_duration", user_id)
            return

        if not _check_allowed(chat_id):
            _pending_pop("pending_antiflood_duration", user_id)
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
            kb_err.add(InlineKeyboardButton("Назад", callback_data=f"stf:open:{chat_id}"))
            _replace_pending_ui(
                m.chat.id,
                "pending_antiflood_duration_msg",
                user_id,
                premium_prefix("Неверный формат. Используйте до 3 интервалов: 30m, 1h 2m, 2mou 1d, навсегда."),
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

        duration = int(parsed_duration)
        if duration != 0 and (duration < MIN_PUNISH_SECONDS or duration > MAX_PUNISH_SECONDS):
            kb_err = InlineKeyboardMarkup(row_width=1)
            kb_err.add(InlineKeyboardButton("Назад", callback_data=f"stf:open:{chat_id}"))
            _replace_pending_ui(
                m.chat.id,
                "pending_antiflood_duration_msg",
                user_id,
                premium_prefix("Длительность должна быть от 1 минуты до 365 дней, либо 'навсегда'."),
                reply_markup=kb_err,
                parse_mode="HTML",
            )
            return

        ch = _mod_get_chat(chat_id)
        settings = ch.get("settings") or {}
        af = settings.get("antiflood") or {}
        punish = af.get("punish") or {}
        ptype = (punish.get("type") or "mute").lower()
        if ptype == "kick":
            _pending_pop("pending_antiflood_duration", user_id)
            _try_delete_private_prompt(m.chat.id, _pending_msg_pop("pending_antiflood_duration_msg", user_id))
            bot.send_message(
                m.chat.id,
                premium_prefix("Для исключения длительность не используется."),
                parse_mode='HTML',
                disable_web_page_preview=True,
            )
            return

        punish["duration"] = int(duration)
        af["punish"] = punish
        settings["antiflood"] = af
        ch["settings"] = settings
        _mod_save()

        _pending_pop("pending_antiflood_duration", user_id)
        prompt_id = _pending_msg_pop("pending_antiflood_duration_msg", user_id)
        _try_delete_private_prompt(m.chat.id, prompt_id)
        _try_delete_private_prompt(m.chat.id, m.message_id)

        ok_text = premium_prefix("✅ Время наказания антифлуда установлено.")
        kb_ok = InlineKeyboardMarkup()
        b_back = InlineKeyboardButton("Назад", callback_data=f"stf:open:{chat_id}")
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
                premium_prefix("Для типа наказания 'Исключение' длительность не используется."),
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

    # Delegate to antispam module for its pending states
    try:
        from antispam import handle_antispam_private_pending
        if handle_antispam_private_pending(m):
            return
    except ImportError:
        pass

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


_ANTIFLOOD_LOCK = threading.Lock()
_ANTIFLOOD_TIMELINE: dict[tuple[int, int], list[tuple[int, int]]] = {}
_ANTIFLOOD_LAST_PUNISH: dict[tuple[int, int], int] = {}
ANTIFLOOD_TRACK_CONTENT_TYPES = [
    "text", "photo", "video", "document", "audio", "animation",
    "sticker", "voice", "video_note",
]


def _antiflood_get_effective_settings(chat_id: int) -> dict:
    af = ((_mod_get_chat(chat_id).get("settings") or {}).get("antiflood") or {})
    punish = af.get("punish") or {}
    try:
        period = int(af.get("period") or 10)
    except Exception:
        period = 10
    try:
        messages = int(af.get("messages") or 6)
    except Exception:
        messages = 6

    ptype = str(punish.get("type") or "mute").strip().lower()
    if ptype not in ("mute", "ban", "kick", "warn"):
        ptype = "mute"

    return {
        "enabled": bool(af.get("enabled", False)),
        "delete_messages": bool(af.get("delete_messages", False)),
        "period": max(3, min(300, period)),
        "messages": max(2, min(50, messages)),
        "punish": {
            "type": ptype,
            "duration": punish.get("duration"),
            "reason": str(punish.get("reason") or "").strip(),
        },
    }


def _antiflood_target_allowed(chat_id: int, user_obj: types.User) -> bool:
    if not user_obj:
        return False

    uid = int(getattr(user_obj, "id", 0) or 0)
    if uid <= 0:
        return False

    # Разработчик бота и dev-пользователи не попадают под антифлуд.
    if is_owner(user_obj) or is_dev(user_obj):
        return False

    # Пользователи с назначенными ролями (1-5) не попадают под антифлуд.
    try:
        if int(get_user_rank(chat_id, uid) or 0) > 0:
            return False
    except Exception:
        pass

    try:
        if bool(getattr(user_obj, "is_bot", False)):
            return False
    except Exception:
        return False

    try:
        if uid == _get_bot_id():
            return False
    except Exception:
        return False

    try:
        member = bot.get_chat_member(chat_id, uid)
        if getattr(member, "status", "") in ("administrator", "creator"):
            return False
    except Exception:
        pass

    return True


def _antiflood_send_punish_message(
    chat_id: int,
    action_kind: str,
    action_id: str,
    target_id: int,
    actor_id: int,
    until_ts: int | None,
) -> None:
    punish_label = {
        "mute": "Ограничение",
        "ban": "Блокировка",
        "kick": "Исключение",
        "warn": "Предупреждение",
    }.get(action_kind, "Наказание")

    target_name = link_for_user(chat_id, target_id)
    actor_name = link_for_user(chat_id, actor_id)

    until_line = "Не используется"
    if action_kind in ("mute", "ban"):
        if until_ts and int(until_ts) > 0:
            try:
                until_line = datetime.fromtimestamp(int(until_ts)).strftime("%Y-%m-%d %H:%M")
            except Exception:
                until_line = "навсегда"
        else:
            until_line = "навсегда"

    text = (
        f"<b>Пользователь</b> {target_name} <b>автоматически наказан за флуд.</b>\n"
        f"<b>Наказание:</b> {punish_label}\n"
        f"<b>Истекает:</b> {until_line}\n\n"
        f"<b>Администратор:</b> {actor_name}"
    )

    kb = None
    if action_kind in ("mute", "ban", "warn"):
        btn_text = {
            "mute": "Снять ограничение",
            "ban": "Разблокировать",
            "warn": "Снять предупреждение",
        }[action_kind]
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(
            btn_text,
            callback_data=f"punish_un:{chat_id}:{action_kind}:{target_id}:{action_id}",
            icon_custom_emoji_id=str(EMOJI_UNPUNISH_ID),
        ))

    try:
        bot.send_message(
            chat_id,
            text,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
    except Exception:
        pass


def _antiflood_try_delete_messages(chat_id: int, message_ids: list[int]) -> int:
    if not message_ids:
        return 0
    if not _bot_can_delete_messages(chat_id):
        return 0

    deleted = 0
    uniq_ids = list(dict.fromkeys(int(mid) for mid in message_ids if int(mid) > 0))
    if len(uniq_ids) > 80:
        uniq_ids = uniq_ids[-80:]

    for mid in uniq_ids:
        try:
            bot.delete_message(chat_id, mid)
            deleted += 1
        except Exception:
            pass
    return deleted


def _antiflood_apply_punishment(
    chat_id: int,
    target_user: types.User,
    af: dict,
    message_ids: list[int] | None = None,
) -> bool:
    target_id = int(getattr(target_user, "id", 0) or 0)
    if target_id <= 0:
        return False

    punish = af.get("punish") or {}
    ptype = str(punish.get("type") or "mute").lower()
    duration_raw = punish.get("duration")
    reason_custom = str(punish.get("reason") or "").strip()
    reason = reason_custom or (
        f"Антифлуд: отправлено {int(af['messages'])}+ сообщений за {int(af['period'])} сек."
    )

    actor_id = _get_bot_id()
    if actor_id <= 0:
        try:
            actor_id = int(getattr(bot.get_me(), "id", 0) or 0)
        except Exception:
            actor_id = 0
    if actor_id <= 0:
        actor_id = target_id

    if bool(af.get("delete_messages")) and message_ids:
        _antiflood_try_delete_messages(chat_id, message_ids)

    if ptype == "warn":
        action_id, count_after, _ = _mod_warn_add(chat_id, actor_id, target_id, reason)
        warn_limit = int((_mod_get_chat(chat_id).get("settings") or {}).get("warn_limit", 3))
        if count_after >= warn_limit:
            try:
                _auto_punish_for_warns(chat_id, bot.get_me(), target_id)
            except Exception:
                pass
        _antiflood_send_punish_message(
            chat_id=chat_id,
            action_kind="warn",
            action_id=action_id,
            target_id=target_id,
            actor_id=actor_id,
            until_ts=None,
        )
        return True

    if ptype == "kick":
        try:
            bot.ban_chat_member(chat_id, target_id)
            bot.unban_chat_member(chat_id, target_id, only_if_banned=True)
            _mark_farewell_suppressed(chat_id, target_id)
        except AttributeError:
            try:
                bot.kick_chat_member(chat_id, target_id)
                bot.unban_chat_member(chat_id, target_id, only_if_banned=True)
                _mark_farewell_suppressed(chat_id, target_id)
            except Exception:
                return False
        except Exception:
            return False

        row = {
            "id": _mod_new_action_id(),
            "target_id": target_id,
            "actor_id": actor_id,
            "created_at": time.time(),
            "duration": 0,
            "until": 0,
            "reason": reason,
            "active": True,
            "auto": True,
            "source": "antiflood",
        }
        _mod_log_append(chat_id, "kick", row)
        _antiflood_send_punish_message(
            chat_id=chat_id,
            action_kind="kick",
            action_id=str(row["id"]),
            target_id=target_id,
            actor_id=actor_id,
            until_ts=None,
        )
        return True

    try:
        duration = int(duration_raw) if duration_raw is not None else 30 * 60
    except Exception:
        duration = 30 * 60
    if duration != 0:
        duration = max(MIN_PUNISH_SECONDS, min(MAX_PUNISH_SECONDS, duration))

    until_ts = None
    if ptype == "ban":
        ok, _, until_ts = _apply_ban(chat_id, target_id, duration)
    else:
        ok, _, until_ts = _apply_mute(chat_id, target_id, duration)
        ptype = "mute"
    if not ok:
        return False

    action_id = _mod_new_action_id()
    row = {
        "id": action_id,
        "target_id": target_id,
        "actor_id": actor_id,
        "created_at": time.time(),
        "duration": int(duration or 0),
        "until": int(until_ts or 0),
        "reason": reason,
        "active": True,
        "auto": True,
        "source": "antiflood",
    }
    _mod_log_append(chat_id, ptype, row)

    ch = _mod_get_chat(chat_id)
    ch.setdefault("active", {}).setdefault(ptype, {})[str(target_id)] = {
        "id": action_id,
        "actor_id": actor_id,
        "created_at": row["created_at"],
        "duration": row["duration"],
        "until": row["until"],
        "reason": row["reason"],
    }
    _mod_save()

    _antiflood_send_punish_message(
        chat_id=chat_id,
        action_kind=ptype,
        action_id=action_id,
        target_id=target_id,
        actor_id=actor_id,
        until_ts=int(until_ts or 0),
    )
    return True


def _antiflood_runtime_check(m: types.Message):
    chat_id = int(m.chat.id)
    if not is_group_approved(chat_id):
        return

    user = getattr(m, "from_user", None)
    if not _antiflood_target_allowed(chat_id, user):
        return

    af = _antiflood_get_effective_settings(chat_id)
    if not af["enabled"]:
        return

    user_id = int(user.id)
    period = int(af["period"])
    msg_limit = int(af["messages"])
    now_ts = _now_ts()
    msg_id = int(getattr(m, "message_id", 0) or 0)
    key = (chat_id, user_id)

    should_punish = False
    punish_message_ids: list[int] = []
    with _ANTIFLOOD_LOCK:
        timeline = _ANTIFLOOD_TIMELINE.get(key) or []
        keep_from = now_ts - period
        timeline = [(ts, mid) for ts, mid in timeline if ts >= keep_from]
        timeline.append((now_ts, msg_id))
        if len(timeline) > max(200, msg_limit * 4):
            timeline = timeline[-max(200, msg_limit * 4):]
        _ANTIFLOOD_TIMELINE[key] = timeline

        last_punish = int(_ANTIFLOOD_LAST_PUNISH.get(key) or 0)
        if len(timeline) >= msg_limit and (now_ts - last_punish) >= max(3, period):
            should_punish = True
            _ANTIFLOOD_LAST_PUNISH[key] = now_ts
            punish_message_ids = [mid for _, mid in timeline if int(mid) > 0]
            _ANTIFLOOD_TIMELINE[key] = []

    if should_punish:
        _antiflood_apply_punishment(chat_id, user, af, message_ids=punish_message_ids)


@bot.message_handler(content_types=ANTIFLOOD_TRACK_CONTENT_TYPES, func=lambda m: m.chat.type in ("group", "supergroup"))
def antiflood_runtime_handler(m: types.Message):
    try:
        _antiflood_runtime_check(m)
    except Exception:
        pass
    return ContinueHandling()


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


__all__ = [name for name in globals() if not name.startswith('__')]



