"""
main.py — Точка входа бота.

Порядок импорта КРИТИЧЕСКИ ВАЖЕН для pyTelegramBotAPI:
первый зарегистрированный обработчик имеет приоритет.

  config        — только константы, нет обработчиков
  persistence   — слой данных, нет обработчиков
  helpers       — инфраструктура + ранние обработчики:
                    dbg_users, rs_* callbacks, verify-команды,
                    on_new_members (ContinueHandling — должен быть ПЕРВЫМ из new_chat_members)
  cmd_basic     — /start, pm, /ping, /log, /broadcast, профили, награды,
                    теги, должности, /closechat, /openchat
  moderation    — /мут, /бан, /кик, /варн, punish_un, modlist, /adminstats
  pin           — /pin, /spin, /npin, /unpin, pin-callbacks
  settings_ui   — /settings, welcome/farewell/rules/cleanup,
                    on_welcome_new_members (new_chat_members — ПОСЛЕ helpers.on_new_members),
                    left_chat_member, rules trigger, cleanup handlers
  handlers      — group stats UI, approve/deny_group, my_chat_member,
                    главный callback_handler, all_other (ДОЛЖЕН БЫТЬ ПОСЛЕДНИМ)
"""

import config       # noqa: F401
import persistence  # noqa: F401 — при импорте подключается кеш get_chat(user_id)
import helpers      # noqa: F401
import cmd_basic    # noqa: F401
import moderation   # noqa: F401
import pin          # noqa: F401
import settings_ui  # noqa: F401
import antispam     # noqa: F401
import banned_words  # noqa: F401
import handlers     # noqa: F401
from dotenv import load_dotenv
load_dotenv()

from config import bot

if __name__ == "__main__":
    print("Запуск бота...")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
