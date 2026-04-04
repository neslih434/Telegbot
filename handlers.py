"""
handlers.py — Завершающие обработчики:
  group stats UI, gstats/pm_settings_open callbacks,
  главный callback_handler (маршрутизатор),
  approve_group / deny_group,
  my_chat_member_handler,
  all_other — ДОЛЖЕН БЫТЬ ЗАРЕГИСТРИРОВАН ПОСЛЕДНИМ.
"""
from __future__ import annotations
import time
import threading
import asyncio
import queue as _queue
import io as _io
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
    PREMIUM_STATS_EMOJI_ID, PREMIUM_USER_EMOJI_ID,
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
    # message-event stats
    get_stats_for_period,
    get_stats_by_day,
)
from helpers import *
from helpers import _dev_contact_find_item, _remember_owner_user_id, _user_can_open_settings
from cmd_basic import *
from cmd_basic import (
    START_MENU_STATE,
    _broadcast_collect_targets,
    _broadcast_new_draft,
    _broadcast_render_panel_text,
    _build_broadcast_panel_keyboard,
    _build_start_about_text,
    _build_start_back_keyboard,
    _build_start_commands_keyboard,
    _build_start_commands_text,
    _build_start_home_keyboard,
    _build_start_home_text,
    _build_start_usage_text,
    _dev_contact_intro_kb,
    _dev_contact_intro_text,
    _dev_contact_prompt_kb,
    _dev_contact_prompt_text,
    _send_start_menu,
    _show_dev_contact_new_messages,
)
from settings_ui import *
from settings_ui import _build_settings_main_keyboard, _send_payload
from pin import *

# ==== СТАТИСТИКА ПО ГРУППЕ ====

STATS_PAGES = {}
GROUP_STATS_PAGE_SIZE = 30

# Метки периодов статистики
PERIOD_LABELS: dict[str, str] = {
    "all": "Всё время",
    "1d":  "1 день",
    "7d":  "7 дней",
    "30d": "30 дней",
}

# ==== ПРОФИЛЬ-КЭШ ИМЁН ====
# Кешируем отображаемые имена пользователей на 5 минут чтобы не бить TG API
# каждый раз при построении страницы статистики.

_PROFILE_NAME_CACHE: dict[tuple[int, int], tuple[float, str]] = {}
_PROFILE_NAME_CACHE_LOCK = threading.Lock()
_PROFILE_NAME_CACHE_TTL: int = 300  # секунды


def _get_cached_display_name(chat_id: int, user_id: int) -> str | None:
    """Return cached display name if still fresh, else None."""
    key = (int(chat_id), int(user_id))
    now = time.monotonic()
    with _PROFILE_NAME_CACHE_LOCK:
        cached = _PROFILE_NAME_CACHE.get(key)
        if cached and (now - cached[0]) < _PROFILE_NAME_CACHE_TTL:
            return cached[1]
    return None


def _set_cached_display_name(chat_id: int, user_id: int, name: str) -> None:
    key = (int(chat_id), int(user_id))
    with _PROFILE_NAME_CACHE_LOCK:
        _PROFILE_NAME_CACHE[key] = (time.monotonic(), name)

def _get_period_since(period: str) -> int:
    """Return unix timestamp marking the start of the requested period (0 = all time)."""
    offsets = {"1d": 86400, "7d": 7 * 86400, "30d": 30 * 86400}
    if period not in offsets:
        return 0
    return int(time.time()) - offsets[period]


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

def build_group_stats_pages(chat: types.Chat, period: str = "all", max_items: int | None = None) -> list[str]:
    """Build paginated stats text for the given period.

    period='all'  → uses in-memory GROUP_STATS (historical all-time data).
    period='1d'|'7d'|'30d' → queries msg_events SQLite table.
    max_items — if set, truncate the full list to this many entries (one page only).
    """
    period_label = PERIOD_LABELS.get(period, period)

    if period == "all":
        chat_id = str(chat.id)
        chat_stats = GROUP_STATS.get(chat_id, {})

        if not chat_stats:
            return [premium_prefix("В этой группе пока нет данных для статистики.")]

        sorted_items = sorted(
            chat_stats.items(),
            key=lambda item: item[1].get("count", 0),
            reverse=True,
        )
        rows_data = [
            (int(uid), data.get("count", 0), data.get("last_msg_id"))
            for uid, data in sorted_items
        ]
    else:
        since_ts = _get_period_since(period)
        rows_data = get_stats_for_period(int(chat.id), since_ts)
        if not rows_data:
            return [premium_prefix(
                f"В этой группе пока нет данных за {period_label}.\n"
                f"<i>Данные накапливаются с момента последнего обновления бота.</i>"
            )]

    if max_items is not None:
        rows_data = rows_data[:max_items]

    chat_title_esc = _html.escape(getattr(chat, "title", None) or str(chat.id))

    items: list[str] = []
    for user_id, count, last_msg_id in rows_data:
        # Use profile name cache to avoid repeated TG API calls
        display_name = _get_cached_display_name(chat.id, user_id)
        if display_name is None:
            try:
                u = tg_get_chat_member(chat.id, int(user_id)).user
                display_name = u.full_name or u.first_name or u.username or "Пользователь"
            except Exception:
                display_name = "Пользователь"
            _set_cached_display_name(chat.id, user_id, display_name)

        name_html = stats_user_link_html(chat, int(user_id), display_name)

        base = (
            f'<tg-emoji emoji-id="{PREMIUM_USER_EMOJI_ID}">👤</tg-emoji> '
            f'{name_html} [<code>{user_id}</code>] — <b>{count}</b>'
        )
        items.append(base)

    page_size = GROUP_STATS_PAGE_SIZE
    total_pages = max(1, (len(items) + page_size - 1) // page_size)
    pages: list[str] = []
    for page_idx in range(total_pages):
        start = page_idx * page_size
        end = start + page_size
        chunk = items[start:end]

        header = (
            f'<tg-emoji emoji-id="{PREMIUM_STATS_EMOJI_ID}">📊</tg-emoji>'
            f' <b>Статистика за {_html.escape(period_label)} для чата "{chat_title_esc}"</b>\n\n'
        )
        body = "\n".join(chunk)
        page_text = (header + body).strip()
        if len(page_text) > MAX_MSG_LEN:
            page_text = page_text[:MAX_MSG_LEN - 3] + "..."
        pages.append(page_text)

    return pages


def _build_group_stats_keyboard(
    page: int, total_pages: int, period: str = "all"
) -> dict:
    """Build inline keyboard with period tabs, optional nav row and image button."""
    rows: list[list[dict]] = []

    # Row 1: period selector tabs
    tab_row: list[dict] = []
    for p, label in PERIOD_LABELS.items():
        btn_label = f"✅ {label}" if p == period else label
        tab_row.append({"text": btn_label, "callback_data": f"gstats_period:{p}"})
    rows.append(tab_row)

    # Row 2: pagination (only if there are multiple pages)
    if total_pages > 1:
        nav_row: list[dict] = []
        if page > 0:
            nav_row.append({"text": "⬅ Предыдущая", "callback_data": f"gstats_prev_{page}"})
        if page < total_pages - 1:
            nav_row.append({"text": "Следующая ➡", "callback_data": f"gstats_next_{page}"})
        if nav_row:
            rows.append(nav_row)

    # Row 3: image button
    rows.append([{"text": "📸 Картинка", "callback_data": "gstats_img"}])

    return {"inline_keyboard": rows}


def _send_stats_message(
    chat: types.Chat,
    period: str = "all",
    reply_to_message_id: int | None = None,
) -> None:
    """Generate stats pages and send (or edit) the stats message with period keyboard."""
    pages = build_group_stats_pages(chat, period)
    keyboard = _build_group_stats_keyboard(0, len(pages), period)
    resp = raw_send_with_inline_keyboard(
        chat.id, pages[0], keyboard,
        reply_to_message_id=reply_to_message_id,
    )
    if not resp or not resp.get("ok"):
        print(f"[STATS] Ошибка отправки: {resp}")
        return
    msg_id = resp["result"]["message_id"]
    STATS_PAGES[(chat.id, msg_id)] = {"pages": pages, "current": 0, "period": period}


def send_group_stats(chat: types.Chat, manual: bool = False):
    _IMG_TASK_QUEUE.put({
        "chat_id": chat.id,
        "period": "all",
        "chart_type": "users",
        "reply_msg_id": None,
    })


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

    # не реагируем, если команда введена ответом на сообщение
    if m.reply_to_message:
        return

    # Антиспам: лимит per-user (60 с) и per-chat (30 с)
    if m.from_user:
        uid_int = int(m.from_user.id)
        wait_u = cooldown_hit('user', uid_int, 'group_stat', 60)
        if wait_u > 0:
            return reply_cooldown_message(m, wait_u, scope='user', bucket=uid_int, action='group_stat')

    wait_seconds = cooldown_hit('chat', int(m.chat.id), 'group_stat', 30)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='group_stat')

    _IMG_TASK_QUEUE.put({
        "chat_id": m.chat.id,
        "period": "all",
        "chart_type": "users",
        "reply_msg_id": m.message_id,
    })


# ---- Новые команды: статистика день / неделя / месяц / вся ----

def _send_stats_limited(
    chat: types.Chat,
    period: str,
    chart_type: str,
    reply_to_message_id: int | None = None,
) -> None:
    """Send a single-page stats message (top 30) with just the image button."""
    pages = build_group_stats_pages(chat, period, max_items=30)
    keyboard = {"inline_keyboard": [[{"text": "📸 Картинка", "callback_data": "gstats_img"}]]}
    resp = raw_send_with_inline_keyboard(
        chat.id, pages[0], keyboard,
        reply_to_message_id=reply_to_message_id,
    )
    if not resp or not resp.get("ok"):
        return
    msg_id = resp["result"]["message_id"]
    STATS_PAGES[(chat.id, msg_id)] = {
        "pages": pages,
        "current": 0,
        "period": period,
        "chart_type": chart_type,
    }


def _stats_limited_guard(m: types.Message, period: str, chart_type: str) -> None:
    """Common guard + dispatch for the 4 limited stats commands."""
    add_stat_message(m)
    add_stat_command('group_stat')

    if not is_group_approved(m.chat.id):
        return bot.reply_to(
            m,
            "⏳ Бот находится на модерации. Ожидание подтверждения от разработчика.",
            parse_mode='HTML',
        )

    if m.reply_to_message:
        return

    # Антиспам: лимит per-user (60 с) и per-chat (30 с)
    if m.from_user:
        uid_int = int(m.from_user.id)
        wait_u = cooldown_hit('user', uid_int, 'group_stat', 60)
        if wait_u > 0:
            return reply_cooldown_message(m, wait_u, scope='user', bucket=uid_int, action='group_stat')

    wait_seconds = cooldown_hit('chat', int(m.chat.id), 'group_stat', 30)
    if wait_seconds > 0:
        return reply_cooldown_message(m, wait_seconds, scope='chat', bucket=int(m.chat.id), action='group_stat')

    _IMG_TASK_QUEUE.put({
        "chat_id": m.chat.id,
        "period": period,
        "chart_type": chart_type,
        "reply_msg_id": m.message_id,
    })


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and is_exact_stat_day(m.text))
def cmd_stats_day(m: types.Message):
    _stats_limited_guard(m, period='1d', chart_type='users')


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and is_exact_stat_week(m.text))
def cmd_stats_week(m: types.Message):
    _stats_limited_guard(m, period='7d', chart_type='daily_7')


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and is_exact_stat_month(m.text))
def cmd_stats_month(m: types.Message):
    _stats_limited_guard(m, period='30d', chart_type='users')


@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and is_exact_stat_all(m.text))
def cmd_stats_all(m: types.Message):
    _stats_limited_guard(m, period='all', chart_type='daily_100')


@bot.callback_query_handler(func=lambda call: bool(call.data) and call.data.startswith("gstats_"))
def cb_group_stats_pagination(call: types.CallbackQuery):
    if _is_duplicate_callback_query(call):
        return
    data = call.data or ""
    m = call.message
    key = (m.chat.id, m.message_id)
    state = STATS_PAGES.get(key)

    # ── Period tab selected ──────────────────────────────────────────────────
    if data.startswith("gstats_period:"):
        new_period = data.split(":", 1)[1]
        if new_period not in PERIOD_LABELS:
            return bot.answer_callback_query(call.id)
        if not state:
            return bot.answer_callback_query(
                call.id, "Эта статистика устарела, откройте новую.", show_alert=True
            )
        new_pages = build_group_stats_pages(m.chat, new_period)
        new_keyboard = _build_group_stats_keyboard(0, len(new_pages), new_period)
        try:
            raw_edit_message_with_keyboard(
                m.chat.id, m.message_id, new_pages[0], new_keyboard
            )
            STATS_PAGES[key] = {"pages": new_pages, "current": 0, "period": new_period}
        except Exception:
            pass
        return bot.answer_callback_query(call.id)

    # ── Image button ─────────────────────────────────────────────────────────
    if data == "gstats_img":
        if not state:
            return bot.answer_callback_query(
                call.id, "Эта статистика устарела, откройте новую.", show_alert=True
            )
        period = state.get("period", "all")
        chart_type = state.get("chart_type", "users")
        _IMG_TASK_QUEUE.put({
            "chat_id": m.chat.id,
            "period": period,
            "chart_type": chart_type,
            "reply_msg_id": m.message_id,
        })
        return bot.answer_callback_query(call.id, "⏳ Генерирую картинку…")

    # ── Page navigation ──────────────────────────────────────────────────────
    if not state:
        return bot.answer_callback_query(
            call.id, "Эта статистика устарела, откройте новую.", show_alert=True
        )

    pages = state.get("pages") or []
    if not pages:
        STATS_PAGES.pop(key, None)
        return bot.answer_callback_query(call.id)

    match = re.match(r"^gstats_(next|prev)_(\d+)$", data)
    if not match:
        return bot.answer_callback_query(call.id)

    action = match.group(1)
    current = int(match.group(2))
    period = state.get("period", "all")
    total_pages = len(pages)

    if action == "next":
        target = min(total_pages - 1, current + 1)
    else:
        target = max(0, current - 1)

    keyboard = _build_group_stats_keyboard(target, total_pages, period)

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


# ==== ГЕНЕРАЦИЯ ИЗОБРАЖЕНИЯ СТАТИСТИКИ ====

_IMG_TASK_QUEUE: _queue.Queue = _queue.Queue()

# Максимальная длина отображаемого имени (в картинке и в подписи)
_STATS_NAME_MAXLEN = 20

# ─── TrueType-шрифты с фолбэком ──────────────────────────────────────────────
_FONT_BOLD_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
    "/app/fonts/DejaVuSans-Bold.ttf",
]
_FONT_REG_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
    "/app/fonts/DejaVuSans.ttf",
]


def _load_font(paths: list, size: int):
    """Load TrueType font from the first available path; fall back to default."""
    from PIL import ImageFont
    for fp in paths:
        try:
            return ImageFont.truetype(fp, size)
        except Exception:
            pass
    return ImageFont.load_default()



def _lookup_user_display_name(chat_id: int, user_id: int) -> str:
    """Return a display name for the user. Tries profile name cache, then local data, then TG API."""
    # Fast path: in-memory TTL cache
    cached = _get_cached_display_name(chat_id, user_id)
    if cached is not None:
        return cached

    chat_id_s = str(chat_id)
    user_id_s = str(user_id)

    # Chat-specific USERS cache
    data = (USERS.get(chat_id_s) or {}).get(user_id_s) or {}
    name = (
        data.get("full_name")
        or (
            (data.get("first_name") or "")
            + (" " + data.get("last_name", "") if data.get("last_name") else "")
        ).strip()
    )
    if name:
        result = name[:30]
        _set_cached_display_name(chat_id, user_id, result)
        return result

    # Global users cache
    gdata = GLOBAL_USERS.get(user_id_s) or {}
    gname = (
        gdata.get("full_name")
        or (
            (gdata.get("first_name") or "")
            + (" " + gdata.get("last_name", "") if gdata.get("last_name") else "")
        ).strip()
    )
    if gname:
        result = gname[:30]
        _set_cached_display_name(chat_id, user_id, result)
        return result

    # Fall back to TG API (with cache)
    try:
        member = tg_get_chat_member(chat_id, user_id)
        u = member.user
        n = (u.full_name or u.first_name or u.username or "").strip()
        result = (n or f"ID {user_id}")[:30]
    except Exception:
        result = f"ID {user_id}"
    _set_cached_display_name(chat_id, user_id, result)
    return result


def _render_stats_image(
    rows: list[tuple[int, int]],
    chat_title: str,
    period_label: str,
    users_map: dict[int, str],
    max_users: int = 30,
) -> bytes:
    """Render a fixed 640×1024 horizontal bar-chart PNG (per-user).

    Draws at 2× then downscales with LANCZOS.
    Layout: pill labels on the left, horizontal bars to the right, count values at end.
    """
    from PIL import Image, ImageDraw

    top_n = rows[:max_users]
    if not top_n:
        raise ValueError("No rows to render")

    # ── Palette ───────────────────────────────────────────────────────────────
    C_BG       = (0x30, 0x3f, 0x54, 255)
    C_STRIPE_A = (0x34, 0x46, 0x5c)
    C_STRIPE_B = (0x38, 0x4b, 0x63)
    C_BAR      = (0xa9, 0xde, 0xe6)
    C_BAR_EDGE = (0xd8, 0xf4, 0xf7)
    C_GRID     = (0x50, 0x64, 0x7b, 255)
    C_MUTED    = (0xb8, 0xc9, 0xd6, 255)
    C_VALUE    = (0xdf, 0xf8, 0xfb, 255)
    C_TITLE    = (0xe5, 0xed, 0xf5, 255)
    C_PILL_FG  = (0x1f, 0x29, 0x33, 255)

    # ── Fixed output size & supersampling ─────────────────────────────────────
    W_OUT, H_OUT = 640, 1024
    S = 2

    # ── Layout (final-pixel coordinates) ─────────────────────────────────────
    N = len(top_n)
    margin_l = 178  # pill label area
    margin_r = 58   # count text on right
    margin_t = 100  # header
    margin_b = 52   # scale labels at bottom

    plot_x0 = margin_l
    plot_y0 = margin_t
    plot_x1 = W_OUT - margin_r
    plot_y1 = H_OUT - margin_b
    plot_w  = plot_x1 - plot_x0
    plot_h  = plot_y1 - plot_y0

    # Adaptive row gap and bar height
    if N <= 8:
        row_gap = 10
    elif N <= 15:
        row_gap = 7
    elif N <= 20:
        row_gap = 5
    else:
        row_gap = 3

    bar_h = max(18, int((plot_h - row_gap * (N - 1)) / N))

    # Pill dimensions (left label)
    pill_w = 164
    pill_x = 6
    pill_h = max(14, bar_h - 4)

    # ── Draw at 2× ───────────────────────────────────────────────────────────
    W, H = W_OUT * S, H_OUT * S

    def sc(v: float) -> int:
        return int(v * S)

    img  = Image.new('RGBA', (W, H), C_BG)
    draw = ImageDraw.Draw(img, 'RGBA')

    label_font_pt = max(8, min(12, bar_h - 7))
    fnt_title = _load_font(_FONT_BOLD_PATHS, sc(18))
    fnt_sub   = _load_font(_FONT_REG_PATHS,  sc(11))
    fnt_label = _load_font(_FONT_REG_PATHS,  sc(label_font_pt))
    fnt_value = _load_font(_FONT_BOLD_PATHS, sc(label_font_pt))
    fnt_scale = _load_font(_FONT_REG_PATHS,  sc(9))

    # Title + subtitle (centered horizontally)
    for txt, yy, font, fill in [
        (f"Статистика за {period_label}", 14, fnt_title, C_TITLE),
        (f'для чата "{chat_title[:38]}"', 40, fnt_sub,   C_MUTED),
    ]:
        bbox = draw.textbbox((0, 0), txt, font=font)
        tw   = bbox[2] - bbox[0]
        draw.text((sc(W_OUT / 2) - tw // 2, sc(yy)), txt, fill=fill, font=font)

    # Scale (vertical grid lines)
    max_val   = top_n[0][1] if top_n else 1
    max_scale = max(max_val, 1)
    # Pick a nice step so we get ~4-6 gridlines
    for step in (1, 2, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 5000):
        if max_scale / step <= 6:
            scale_step = step
            break
    else:
        scale_step = max(1, max_scale // 5)

    steps = list(range(0, max_scale + 1, scale_step))
    if max_scale not in steps:
        steps.append(max_scale)

    for s_val in steps:
        x_f = plot_x0 + (s_val / max_scale) * plot_w
        draw.line(
            [(sc(x_f), sc(plot_y0 - 6)), (sc(x_f), sc(plot_y1))],
            fill=C_GRID, width=max(1, S),
        )
        lbl = str(s_val)
        bbox = draw.textbbox((0, 0), lbl, font=fnt_scale)
        tw   = bbox[2] - bbox[0]
        draw.text((sc(x_f) - tw // 2, sc(plot_y1 + 4)), lbl, fill=C_MUTED, font=fnt_scale)

    # Alternating row backgrounds
    for i in range(N):
        y = plot_y0 + i * (bar_h + row_gap)
        fill = C_STRIPE_A if i % 2 == 0 else C_STRIPE_B
        draw.rounded_rectangle(
            (sc(plot_x0), sc(y), sc(plot_x1), sc(y + bar_h)),
            radius=sc(6), fill=fill + (255,),
        )

    # Bars + pills + counts
    for i, (user_id, count) in enumerate(top_n):
        y   = plot_y0 + i * (bar_h + row_gap)
        y_m = y + bar_h / 2

        # ── Pill (semi-transparent label on left) ─────────────────────────
        pill_y       = y_m - pill_h / 2
        pill_fill    = C_BAR + (70,)
        pill_outline = C_BAR + (150,)
        draw.rounded_rectangle(
            (sc(pill_x), sc(pill_y), sc(pill_x + pill_w), sc(pill_y + pill_h)),
            radius=sc(min(pill_h // 2, 14)),
            fill=pill_fill, outline=pill_outline, width=max(1, S),
        )

        # Pill text: truncate name to fit, always show [ID]
        name    = users_map.get(user_id, f"ID {user_id}")
        id_tag  = f"[{user_id}]"
        max_pill_tw = sc(pill_w - 8)

        # Try "Name [ID]", shrink name until it fits or only show [ID]
        trunc = name[:_STATS_NAME_MAXLEN]
        while True:
            label = f"{trunc} {id_tag}" if trunc else id_tag
            bbox  = draw.textbbox((0, 0), label, font=fnt_label)
            if (bbox[2] - bbox[0]) <= max_pill_tw or len(trunc) <= 0:
                break
            trunc = trunc[:-2] + "…"

        bbox = draw.textbbox((0, 0), label, font=fnt_label)
        tw_l = bbox[2] - bbox[0]
        th_l = bbox[3] - bbox[1]
        draw.text(
            (sc(pill_x) + (sc(pill_w) - tw_l) // 2,
             sc(pill_y) + (sc(pill_h) - th_l) // 2 - S),
            label, fill=C_PILL_FG, font=fnt_label,
        )

        # ── Horizontal bar ─────────────────────────────────────────────────
        bar_w = max(sc(2), int(sc(plot_w) * count / max_scale))
        draw.rounded_rectangle(
            (sc(plot_x0), sc(y), sc(plot_x0) + bar_w, sc(y + bar_h)),
            radius=sc(6),
            fill=C_BAR + (255,), outline=C_BAR_EDGE + (255,), width=max(1, S),
        )

        # ── Count value (right of bar, clamped within plot) ────────────────
        val_txt = str(count)
        bbox_v  = draw.textbbox((0, 0), val_txt, font=fnt_value)
        th_v    = bbox_v[3] - bbox_v[1]
        val_x   = min(sc(plot_x0) + bar_w + sc(5), sc(plot_x1) - (bbox_v[2] - bbox_v[0]) - S)
        draw.text(
            (val_x, sc(y_m) - th_v // 2 - S),
            val_txt, fill=C_VALUE, font=fnt_value,
        )

    # Downscale to final size
    img_out = img.convert('RGB').resize((W_OUT, H_OUT), Image.LANCZOS)
    buf = _io.BytesIO()
    img_out.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _render_daily_chart(
    days_data: list[tuple[int, int]],
    chat_title: str,
    period_label: str,
) -> bytes:
    """Render a fixed 640×1024 vertical bar chart grouped by day.

    Draws at 2× then downscales with LANCZOS.
    days_data: list of (day_ts_utc, count) ordered ASC (oldest → newest).
    """
    from PIL import Image, ImageDraw

    if not days_data:
        raise ValueError("No data")

    # ── Palette (shared with user chart) ─────────────────────────────────────
    C_BG       = (0x30, 0x3f, 0x54, 255)
    C_GRID     = (0x50, 0x64, 0x7b, 255)
    C_MUTED    = (0xb8, 0xc9, 0xd6, 255)
    C_TITLE    = (0xe5, 0xed, 0xf5, 255)
    C_BAR_DEF  = (0xa9, 0xde, 0xe6, 255)   # regular day
    C_BAR_SUN  = (0xd8, 0xf4, 0xf7, 255)   # Sunday highlight

    # ── Fixed output size & supersampling ─────────────────────────────────────
    W_OUT, H_OUT = 640, 1024
    S = 2

    # ── Layout (final-pixel coordinates) ─────────────────────────────────────
    margin_t  = 100   # header
    margin_b  = 52    # date labels
    margin_lr = 28    # left/right

    chart_x0 = margin_lr
    chart_y0 = margin_t
    chart_x1 = W_OUT - margin_lr
    chart_y1 = H_OUT - margin_b
    chart_w  = chart_x1 - chart_x0
    chart_h  = chart_y1 - chart_y0

    n = len(days_data)
    gap  = max(1, min(4, chart_w // (n * 4)))
    bar_w = max(2, (chart_w - gap * (n - 1)) // n)

    # ── Draw at 2× ───────────────────────────────────────────────────────────
    W, H = W_OUT * S, H_OUT * S

    def sc(v: float) -> int:
        return int(v * S)

    img  = Image.new('RGBA', (W, H), C_BG)
    draw = ImageDraw.Draw(img, 'RGBA')

    fnt_title = _load_font(_FONT_BOLD_PATHS, sc(18))
    fnt_sub   = _load_font(_FONT_REG_PATHS,  sc(11))
    fnt_label = _load_font(_FONT_REG_PATHS,  sc(8))

    # Title + subtitle (centered)
    for txt, yy, font, fill in [
        (f"Статистика за {period_label}", 14, fnt_title, C_TITLE),
        (f'для чата "{chat_title[:38]}"', 40, fnt_sub,   C_MUTED),
    ]:
        bbox = draw.textbbox((0, 0), txt, font=font)
        tw   = bbox[2] - bbox[0]
        draw.text((sc(W_OUT / 2) - tw // 2, sc(yy)), txt, fill=fill, font=font)

    # Horizontal grid lines at 25 %, 50 %, 75 %, 100 %
    max_count = max(c for _, c in days_data) or 1
    for frac in (0.25, 0.50, 0.75, 1.00):
        gy = chart_y1 - int(chart_h * frac)
        draw.rectangle(
            [sc(chart_x0), sc(gy), sc(chart_x1), sc(gy) + max(1, S)],
            fill=C_GRID,
        )

    # Label density: avoid crowding when many bars
    label_step = 1 if n <= 10 else 3 if n <= 21 else 7 if n <= 50 else 10

    for i, (day_ts, count) in enumerate(days_data):
        x     = chart_x0 + i * (bar_w + gap)
        bh    = max(1, int(chart_h * count / max_count))
        # Sundays (epoch day % 7 == 3) get a lighter shade
        dow   = (day_ts // 86400) % 7
        color = C_BAR_SUN if dow == 3 else C_BAR_DEF
        draw.rectangle(
            [sc(x), sc(chart_y1 - bh), sc(x + bar_w), sc(chart_y1)],
            fill=color,
        )
        if i % label_step == 0:
            try:
                day_str = datetime.utcfromtimestamp(day_ts).strftime("%d.%m")
            except Exception:
                day_str = ""
            draw.text((sc(x), sc(chart_y1 + 4)), day_str, fill=C_MUTED, font=fnt_label)

    # Downscale to final size
    img_out = img.convert('RGB').resize((W_OUT, H_OUT), Image.LANCZOS)
    buf = _io.BytesIO()
    img_out.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


_TG_CAPTION_LIMIT = 1024   # Telegram caption character limit


def _build_stats_caption(
    chat_title: str,
    period_label: str,
    rows: list[tuple[int, int]],
    users_map: dict[int, str],
    max_entries: int = 30,
) -> str:
    """Build a compact photo caption: title + top-N user list.

    Stays within Telegram's 1024-character caption limit by truncating the
    list if necessary (the chart image itself always shows the full top-N).
    """
    header = f"📊 Статистика за {period_label} для чата \"{chat_title}\"\n\n"
    budget = _TG_CAPTION_LIMIT - len(header)
    lines: list[str] = []
    for i, (user_id, count) in enumerate(rows[:max_entries]):
        name = users_map.get(user_id, f"ID {user_id}")
        line = f"{i + 1}. {name[:_STATS_NAME_MAXLEN]} [{user_id}] — {count}\n"
        if budget - len(line) < 0:
            break
        lines.append(line)
        budget -= len(line)
    return header + "".join(lines)


def _process_image_task(task: dict) -> None:
    """Generate the appropriate chart, build caption, and send a single photo."""
    chat_id: int = task["chat_id"]
    period: str  = task.get("period", "all")
    chart_type: str = task.get("chart_type", "users")
    reply_msg_id: int | None = task.get("reply_msg_id")
    period_label = PERIOD_LABELS.get(period, period)

    try:
        chat_obj  = tg_get_chat(chat_id)
        chat_title = getattr(chat_obj, "title", None) or str(chat_id)
    except Exception:
        chat_title = str(chat_id)

    def _send_error(text: str) -> None:
        try:
            bot.send_message(chat_id, text, reply_to_message_id=reply_msg_id)
        except Exception:
            pass

    # ── Daily chart (weekly 7d or all-time 100d) ─────────────────────────────
    if chart_type in ("daily_7", "daily_100"):
        max_days  = 7 if chart_type == "daily_7" else 100
        since_ts  = _get_period_since("7d") if chart_type == "daily_7" else 0
        if since_ts == 0:
            since_ts = int(time.time()) - 100 * 86400
        days_data = get_stats_by_day(chat_id, since_ts, max_days=max_days)

        if not days_data:
            _send_error("📊 Нет данных для отображения за выбранный период.")
            return

        try:
            img_bytes = _render_daily_chart(days_data, chat_title, period_label)
        except ImportError:
            _send_error("❌ Генерация изображений недоступна (Pillow не установлен).")
            return
        except Exception as exc:
            print(f"[IMAGE RENDER daily] {exc}")
            _send_error("❌ Ошибка при генерации изображения статистики.")
            return

        # Build top-users caption for the same period
        if period == "all":
            chat_stats = GROUP_STATS.get(str(chat_id), {})
            top_rows: list[tuple[int, int]] = sorted(
                [(int(uid), d.get("count", 0)) for uid, d in chat_stats.items()],
                key=lambda x: x[1],
                reverse=True,
            )[:30]
        else:
            raw = get_stats_for_period(chat_id, since_ts)
            top_rows = [(r[0], r[1]) for r in raw][:30]
        users_map = {uid: _lookup_user_display_name(chat_id, uid) for uid, _ in top_rows}
        caption = _build_stats_caption(chat_title, period_label, top_rows, users_map)

        try:
            bot.send_photo(
                chat_id,
                _io.BytesIO(img_bytes),
                caption=caption,
                reply_to_message_id=reply_msg_id,
            )
        except Exception as exc:
            print(f"[IMAGE SEND daily] {exc}")
        return

    # ── User bar chart ────────────────────────────────────────────────────────
    if period == "all":
        chat_stats = GROUP_STATS.get(str(chat_id), {})
        rows: list[tuple[int, int]] = sorted(
            [(int(uid), d.get("count", 0)) for uid, d in chat_stats.items()],
            key=lambda x: x[1],
            reverse=True,
        )[:30]
    else:
        since_ts = _get_period_since(period)
        raw = get_stats_for_period(chat_id, since_ts)
        rows = [(r[0], r[1]) for r in raw][:30]

    if not rows:
        _send_error("📊 Нет данных для отображения за выбранный период.")
        return

    users_map = {uid: _lookup_user_display_name(chat_id, uid) for uid, _ in rows}

    try:
        img_bytes = _render_stats_image(rows, chat_title, period_label, users_map)
    except ImportError:
        _send_error("❌ Генерация изображений недоступна (Pillow не установлен).")
        return
    except Exception as exc:
        print(f"[IMAGE RENDER] {exc}")
        _send_error("❌ Ошибка при генерации изображения статистики.")
        return

    caption = _build_stats_caption(chat_title, period_label, rows, users_map)

    try:
        bot.send_photo(
            chat_id,
            _io.BytesIO(img_bytes),
            caption=caption,
            reply_to_message_id=reply_msg_id,
        )
    except Exception as exc:
        print(f"[IMAGE SEND] {exc}")


def _image_worker() -> None:
    """Daemon thread: consumes image-generation tasks from the queue."""
    while True:
        task = _IMG_TASK_QUEUE.get()
        try:
            _process_image_task(task)
        except Exception as e:
            print(f"[IMAGE WORKER] Unexpected error: {e}")


_IMAGE_THREAD = threading.Thread(target=_image_worker, daemon=True)
_IMAGE_THREAD.start()


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


__all__ = [name for name in globals() if not name.startswith('__')]

