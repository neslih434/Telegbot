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


__all__ = [name for name in globals() if not name.startswith('__')]

