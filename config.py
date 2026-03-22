"""
config.py — константы, переменные окружения, инициализация bot/tg_client, emoji IDs.
Импортируется всеми остальными модулями. Не импортирует ничего из нашего кода.
"""
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
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
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
USERS_FILE = os.path.join(DATA_DIR, 'users.json')
VERIFY_ADMINS_FILE = os.path.join(DATA_DIR, 'verify_admins.json')
VERIFY_DEV_FILE = os.path.join(DATA_DIR, 'verify_dev.json')
GLOBAL_USERS_FILE = os.path.join(DATA_DIR, "global_users.json")
CHAT_SETTINGS_FILE = os.path.join(DATA_DIR, 'chat_settings.json')
MODERATION_FILE = os.path.join(DATA_DIR, 'moderation.json')
DEV_CONTACT_INBOX_FILE = os.path.join(DATA_DIR, 'dev_contact_inbox.json')
DEV_CONTACT_META_FILE = os.path.join(DATA_DIR, 'dev_contact_meta.json')
PENDING_GROUPS_FILE = os.path.join(DATA_DIR, 'pending_groups.json')

# Файлы, отсутствующие в верхней секции оригинала, но нужные модулям:
CLOSE_CHAT_FILE   = os.path.join(DATA_DIR, 'closechat.json')
CHAT_ROLES_FILE   = os.path.join(DATA_DIR, 'chat_roles.json')
ROLE_PERMS_FILE   = os.path.join(DATA_DIR, 'role_perms.json')
SQLITE_DB_FILE    = os.path.join(DATA_DIR, "bot_data.sqlite3")

os.makedirs(DATA_DIR, exist_ok=True)

BOT_THREADS = max(2, int(os.getenv("BOT_THREADS", "8")))
DB_FLUSH_INTERVAL_SECONDS = max(1, int(os.getenv("DB_FLUSH_INTERVAL_SECONDS", "2")))
GLOBAL_LAST_SEEN_UPDATE_SECONDS = max(15, int(os.getenv("GLOBAL_LAST_SEEN_UPDATE_SECONDS", "60")))
TG_CACHE_MEMBER_TTL = max(1, int(os.getenv("TG_CACHE_MEMBER_TTL", "15")))
TG_CACHE_CHAT_TTL = max(5, int(os.getenv("TG_CACHE_CHAT_TTL", "60")))
SQLITE_JSON_FALLBACK_WRITE = os.getenv("SQLITE_JSON_FALLBACK_WRITE", "0").strip().lower() in {"1", "true", "yes", "on"}

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
API_BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

# Shared HTTP session для raw-запросов
_HTTP_SESSION = requests.Session()


# ==== ЭМОДЗИ ====

PREMIUM_PREFIX_EMOJI_ID = "5447644880824181073"
PREMIUM_STATS_EMOJI_ID  = "5431577498364158238"
PREMIUM_USER_EMOJI_ID   = "5373012449597335010"
PREMIUM_CLOSE_EMOJI_ID  = "5465665476971471368"

# профиль
EMOJI_PROFILE_ID        = "5226512880362332956"
EMOJI_MSG_COUNT_ID      = "5431577498364158238"
EMOJI_DESC_ID           = "5334673106202010226"
EMOJI_AWARDS_BLOCK_ID   = "5332547853304734597"

# статусы
EMOJI_OWNER_ID          = "5958376256788502078"
EMOJI_ADMIN_ID          = "5377754411319698237"
EMOJI_DEV_ID            = "5390851716520353647"
EMOJI_MEMBER_ID         = "5373012449597335010"
EMOJI_LEFT_ID           = "5906995262378741881"
EMOJI_PREMIUM_STATUS_ID = "5438496463044752972"
EMOJI_VERIFY_ADMIN_ID   = "5370941588165893740"
EMOJI_VERIFY_DEV_ID     = "5370661904190544678"

# роли
EMOJI_ROLE_ALL_ID         = "5908808657700655253"
EMOJI_ROLE_DEV_ID         = "5951665890079544884"
EMOJI_ROLE_OWNER_ID       = "5397796867616546218"
EMOJI_ROLE_CHIEF_ADMIN_ID = "5397754265835938409"
EMOJI_ROLE_ADMIN_ID       = "5397646938898178715"
EMOJI_ROLE_MOD_ID         = "5397653273974939567"
EMOJI_ROLE_TRAINEE_ID     = "5398049016556560225"

EMOJI_USER_ROLE_TEXT_ID = "5418010521309815154"
EMOJI_ROLE_ACTION_ID    = "5418010521309815154"

# интерфейс прав должностей
EMOJI_ROLE_SETTINGS_CHAT_ID       = 5287238684226104614
EMOJI_ROLE_SETTINGS_SENT_PM_ID    = 5341715473882955310
EMOJI_ROLE_SETTINGS_CANCEL_ID     = 5465665476971471368
EMOJI_ROLE_SETTINGS_SAVE_ID       = 5454096630372379732
EMOJI_ROLE_SETTINGS_OPEN_AGAIN_ID = 5264727218734524899
EMOJI_ROLE_SETTINGS_CHOOSE_RANK_ID = 5472308992514464048
EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID = 5963223853231509569

EMOJI_SCOPE_GROUP_ID = "5942877472163892475"
EMOJI_SCOPE_PM_ID    = "5967548335542767952"
EMOJI_SCOPE_ALL_ID   = "5944940516754853337"

# доп эмодзи
EMOJI_LIST_ID           = "5334882760735598374"
EMOJI_ADMIN_RIGHTS_ID   = "5454096630372379732"
EMOJI_BTN_UNADMIN_ID    = "5465665476971471368"
EMOJI_BTN_KICK_ID       = "5467928559664242360"
EMOJI_REASON_ID         = "5465143921912846619"
EMOJI_PING_ID           = "5472146462362048818"
EMOJI_LOG_ID            = "5433653135799228968"
EMOJI_LOG_PM_ID         = "5427009714745517609"
EMOJI_CHAT_CLOSED_ID    = "5472308992514464048"
EMOJI_CHAT_OPEN_BTN_ID  = "5427009714745517609"
EMOJI_PIN_NOTIFY_ID     = 5242628160297641831
EMOJI_PIN_SILENT_ID     = 5244807637157029775
EMOJI_PIN_REPIN_ID      = 5264727218734524899
EMOJI_DELETED_REASON_ID = "5467519850576354798"
EMOJI_RATE_LIMIT_ID     = "5451732530048802485"

# настройки приветствия
EMOJI_WELCOME_TEXT_ID    = "5334882760735598374"
EMOJI_WELCOME_MEDIA_ID   = "5431783411981228752"
EMOJI_WELCOME_BUTTONS_ID = "5363850326577259091"

# модерация
EMOJI_PUNISHMENT_ID     = "5467928559664242360"
EMOJI_UNPUNISH_ID       = "5427009714745517609"
EMOJI_PAGINATION_NEXT_ID = "5963179889946268318"
EMOJI_PAGINATION_PREV_ID = "5963223853231509569"

# связь с разработчиком
EMOJI_CONTACT_DEV_ID      = "5406631276042002796"
EMOJI_SEND_TEXT_PROMPT_ID = "5334673106202010226"
EMOJI_SENT_OK_ID          = "5427009714745517609"
EMOJI_NEW_MSG_OWNER_ID    = "5361979468887893611"
EMOJI_REPLY_BTN_ID        = "5433614747381538714"
EMOJI_IGNORE_BTN_ID       = "5454096630372379732"
EMOJI_REPLY_RECEIVED_ID   = "5433811242135331842"
EMOJI_BOT_VERSION_ID      = "5021712394259268143"

# легенда стартового меню
EMOJI_LEGEND_ANYWHERE_ID        = "5287238684226104614"
EMOJI_LEGEND_DEV_ONLY_ID        = "5390851716520353647"
EMOJI_LEGEND_DEV_OR_VERIFIED_ID = "5370661904190544678"
EMOJI_LEGEND_GROUP_ADMIN_ID     = "5377754411319698237"
EMOJI_LEGEND_PM_ONLY_ID         = "5373012449597335010"
EMOJI_LEGEND_GROUP_ONLY_ID      = "5372926953978341366"
EMOJI_LEGEND_ALL_USERS_ID       = "5411285332668720752"

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
