"""
tests/test_banned_words.py — Unit-тесты для ядра проверки запрещённых слов.

Тестируются чистые функции нормализации и матчинга — без Telegram API.
"""
import sys
import os
import re
import pytest

# Добавляем корень проекта в sys.path, чтобы импортировать модуль без Telegram-зависимостей.
# Мы импортируем только чистые функции напрямую, не весь модуль banned_words.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Импортируем только нужные чистые функции, минуя инициализацию бота.
# Используем importlib для изоляции от Telegram-зависимостей.
import importlib.util
import types as _types

_spec = importlib.util.spec_from_file_location(
    "_bw_core",
    os.path.join(_PROJECT_ROOT, "banned_words.py"),
)

# Перед загрузкой модуля подставляем mock-объекты для Telegram-зависимостей
import unittest.mock as _mock

_telegram_mocks = {
    "config": _mock.MagicMock(),
    "persistence": _mock.MagicMock(),
    "moderation": _mock.MagicMock(),
    "helpers": _mock.MagicMock(),
    "settings_ui": _mock.MagicMock(),
}

# Добавляем необходимые атрибуты к mock-модулям
_telegram_mocks["config"].ContinueHandling = Exception
_telegram_mocks["config"].types = _mock.MagicMock()
_telegram_mocks["config"].InlineKeyboardMarkup = _mock.MagicMock()
_telegram_mocks["config"].InlineKeyboardButton = _mock.MagicMock()
_telegram_mocks["config"].bot = _mock.MagicMock()
_telegram_mocks["config"].EMOJI_ROLE_SETTINGS_SENT_PM_ID = "0"
_telegram_mocks["config"].EMOJI_ROLE_SETTINGS_CANCEL_ID = "0"
_telegram_mocks["config"].EMOJI_ROLE_SETTINGS_BACK_PREMIUM_ID = "0"
_telegram_mocks["config"].EMOJI_UNPUNISH_ID = "0"
_telegram_mocks["persistence"].CHAT_SETTINGS = {}
_telegram_mocks["persistence"].save_chat_settings = _mock.MagicMock()
_telegram_mocks["persistence"]._is_duplicate_callback_query = _mock.MagicMock(return_value=False)
_telegram_mocks["moderation"]._mod_get_chat = _mock.MagicMock(return_value={"settings": {}})
_telegram_mocks["moderation"]._mod_save = _mock.MagicMock()
_telegram_mocks["moderation"]._mod_duration_text = _mock.MagicMock(return_value="1ч")
_telegram_mocks["moderation"]._parse_duration_prefix = _mock.MagicMock(return_value=(3600, 1, False))
_telegram_mocks["moderation"]._mod_new_action_id = _mock.MagicMock(return_value="test_id")
_telegram_mocks["moderation"]._mod_log_append = _mock.MagicMock()
_telegram_mocks["moderation"]._mod_warn_add = _mock.MagicMock(return_value=("id", 1, 0))
_telegram_mocks["moderation"]._auto_punish_for_warns = _mock.MagicMock()
_telegram_mocks["moderation"]._apply_mute = _mock.MagicMock(return_value=(True, None, 0))
_telegram_mocks["moderation"]._apply_ban = _mock.MagicMock(return_value=(True, None, 0))
_telegram_mocks["moderation"]._mark_farewell_suppressed = _mock.MagicMock()
_telegram_mocks["moderation"]._send_punish_message_with_button = _mock.MagicMock()
_telegram_mocks["helpers"].is_owner = _mock.MagicMock(return_value=False)
_telegram_mocks["helpers"].is_dev = _mock.MagicMock(return_value=False)
_telegram_mocks["helpers"].is_group_approved = _mock.MagicMock(return_value=True)
_telegram_mocks["helpers"].get_user_rank = _mock.MagicMock(return_value=0)
_telegram_mocks["helpers"].link_for_user = _mock.MagicMock(return_value="user")
_telegram_mocks["helpers"].premium_prefix = lambda t: t
_telegram_mocks["helpers"].match_command = _mock.MagicMock(return_value=False)
_telegram_mocks["settings_ui"]._pending_get = _mock.MagicMock(return_value={})
_telegram_mocks["settings_ui"]._pending_put = _mock.MagicMock()
_telegram_mocks["settings_ui"]._pending_pop = _mock.MagicMock(return_value=None)
_telegram_mocks["settings_ui"]._pending_msg_get = _mock.MagicMock(return_value=None)
_telegram_mocks["settings_ui"]._pending_msg_set = _mock.MagicMock()
_telegram_mocks["settings_ui"]._pending_msg_pop = _mock.MagicMock(return_value=None)
_telegram_mocks["settings_ui"]._delete_pending_ui = _mock.MagicMock()
_telegram_mocks["settings_ui"]._replace_pending_ui = _mock.MagicMock()
_telegram_mocks["settings_ui"]._try_delete_private_prompt = _mock.MagicMock()
_telegram_mocks["settings_ui"]._show_warn_settings_ui = _mock.MagicMock(return_value=True)
_telegram_mocks["settings_ui"]._user_can_open_settings = _mock.MagicMock(return_value=(True, None))
_telegram_mocks["settings_ui"]._bot_can_delete_messages = _mock.MagicMock(return_value=True)
_telegram_mocks["settings_ui"]._get_bot_id = _mock.MagicMock(return_value=1)
_telegram_mocks["settings_ui"].MIN_PUNISH_SECONDS = 60
_telegram_mocks["settings_ui"].MAX_PUNISH_SECONDS = 365 * 24 * 60 * 60
_telegram_mocks["settings_ui"].CLEANUP_ICON_ENABLE_ID = "0"
_telegram_mocks["settings_ui"].CLEANUP_ICON_DISABLE_ID = "0"

for name, mock in _telegram_mocks.items():
    sys.modules[name] = mock

# Теперь можно безопасно импортировать функции
_bw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bw)

# Импортируем нужные функции
normalize_for_mode = _bw.normalize_for_mode
_term_matches = _bw._term_matches
is_wildcard_rule = _bw.is_wildcard_rule
validate_wildcard_rule = _bw.validate_wildcard_rule
_wildcard_matches = _bw._wildcard_matches
_parse_term_lines = _bw._parse_term_lines
_base_normalize = _bw._base_normalize


# ─────────────────────────────────────────────
# Тесты нормализации
# ─────────────────────────────────────────────

class TestNormalize:

    def test_exact_casefold(self):
        assert normalize_for_mode("МАТ", "EXACT") == "мат"

    def test_exact_nfkc(self):
        # NFKC нормализует полноширинные символы в обычные ASCII
        full = "\uff4d\uff41\uff54"  # ｍａｔ полноширинные
        result = normalize_for_mode(full, "EXACT")
        # NFKC конвертирует ｍａｔ → mat (затем casefold)
        assert result == "mat", f"Got: {result!r}"

    def test_normalized_cleans_separators(self):
        result = normalize_for_mode("м-а-т", "NORMALIZED")
        assert " " in result or result == "м а т", f"Got: {result!r}"

    def test_split_proof_condenses(self):
        result = normalize_for_mode("м-а-т", "SPLIT_PROOF")
        assert result == "мат", f"Got: {result!r}"

    def test_split_proof_spaces(self):
        result = normalize_for_mode("м а т", "SPLIT_PROOF")
        assert result == "мат", f"Got: {result!r}"

    def test_confusables_maps_latin(self):
        # 'a' → 'а', 'e' → 'е' и т.д.
        result = normalize_for_mode("mat", "CONFUSABLES")
        assert result == "мат", f"Got: {result!r}"

    def test_confusables_mixed(self):
        # мат с латинскими 'a', 't'
        mixed = "м" + "a" + "т"  # 'a' — латинская
        result = normalize_for_mode(mixed, "CONFUSABLES")
        assert result == "мат", f"Got: {result!r}"

    def test_aggressive_collapses_repeats(self):
        result = normalize_for_mode("мааааат", "AGGRESSIVE")
        assert result == "мат", f"Got: {result!r}"

    def test_aggressive_collapses_after_confusables(self):
        # mat → маt → мaт (confusables) → мат, collapse repeats
        result = normalize_for_mode("maaaat", "AGGRESSIVE")
        assert result == "мат", f"Got: {result!r}"


# ─────────────────────────────────────────────
# Тесты матчинга по режимам
# ─────────────────────────────────────────────

class TestTermMatches:

    # EXACT: ловит только прямое совпадение
    def test_exact_catches_word(self):
        assert _term_matches("в тексте мат стоит", "мат", "word", "EXACT")

    def test_exact_no_split(self):
        assert not _term_matches("м а т", "мат", "word", "EXACT")

    def test_exact_no_dash(self):
        assert not _term_matches("м-а-т", "мат", "word", "EXACT")

    def test_exact_word_boundary(self):
        # "маты" не должно совпасть с "мат" (EXACT, word boundary)
        assert not _term_matches("маты в тексте", "мат", "word", "EXACT")

    def test_exact_case_insensitive(self):
        assert _term_matches("МАТ плохо", "мат", "word", "EXACT")

    # NORMALIZED: очищает пунктуацию/пробелы, но не склеивает
    def test_normalized_catches_word(self):
        assert _term_matches("мат!", "мат", "word", "NORMALIZED")

    def test_normalized_no_split(self):
        # NORMALIZED НЕ должен ловить м-а-т как мат (не склеивает)
        assert not _term_matches("м-а-т", "мат", "word", "NORMALIZED")

    # SPLIT_PROOF: ловит раздельное написание
    def test_split_proof_dash(self):
        assert _term_matches("м-а-т это плохо", "мат", "word", "SPLIT_PROOF")

    def test_split_proof_spaces(self):
        assert _term_matches("м а т сказал он", "мат", "word", "SPLIT_PROOF")

    def test_split_proof_dots(self):
        assert _term_matches("м.а.т", "мат", "word", "SPLIT_PROOF")

    def test_split_proof_catches_direct(self):
        assert _term_matches("просто мат", "мат", "word", "SPLIT_PROOF")

    # CONFUSABLES: ловит подмену символов
    def test_confusables_latin_a(self):
        # 'а' заменена на латинскую 'a'
        mixed = "м" + "a" + "т"  # латинская 'a'
        assert _term_matches(mixed, "мат", "word", "CONFUSABLES")

    def test_confusables_full_latin(self):
        # mat (полностью латинское) → мат
        assert _term_matches("mat", "мат", "word", "CONFUSABLES")

    def test_confusables_with_split(self):
        # m-a-t (латинское с разделителем)
        assert _term_matches("m-a-t", "мат", "word", "CONFUSABLES")

    # AGGRESSIVE: ловит повторы букв
    def test_aggressive_repeats(self):
        assert _term_matches("мааааат", "мат", "word", "AGGRESSIVE")

    def test_aggressive_mixed_repeats(self):
        assert _term_matches("мааааааат это", "мат", "word", "AGGRESSIVE")

    def test_aggressive_latin_repeats(self):
        # maaat (латинское с повторами)
        assert _term_matches("maaaat", "мат", "word", "AGGRESSIVE")

    # Phrases
    def test_phrase_exact(self):
        assert _term_matches("это плохая фраза совсем", "плохая фраза", "phrase", "EXACT")

    def test_phrase_normalized(self):
        assert _term_matches("это плохая-фраза совсем", "плохая фраза", "phrase", "NORMALIZED")

    def test_phrase_split_proof(self):
        # Фраза склеивается при SPLIT_PROOF
        assert _term_matches("плохаяфраза", "плохая фраза", "phrase", "SPLIT_PROOF")


# ─────────────────────────────────────────────
# Тесты исключений (allowlist)
# ─────────────────────────────────────────────

class TestAllowlist:
    """
    Если слово совпадает с исключением — нет нарушения.
    Проверяем логику через _term_matches напрямую.
    """

    def test_allow_term_matches(self):
        # "разрешённое" совпадает с allowlist
        assert _term_matches("разрешённое слово", "разрешённое", "word", "NORMALIZED")

    def test_allow_term_no_match(self):
        # "запрещённое" не в allowlist
        assert not _term_matches("другое слово", "запрещённое", "word", "NORMALIZED")

    def test_allow_takes_precedence(self):
        """
        Моделируем логику: если текст совпадает с allowlist-терминoм,
        проверка запрещённых слов не должна срабатывать.
        """
        text = "матрас"
        allowed_term = "матрас"
        banned_term = "мат"

        # allowlist совпадает
        allow_match = _term_matches(text, allowed_term, "word", "NORMALIZED")
        # Если совпало с allowlist — не проверяем banned list
        if allow_match:
            result = "no_violation"
        else:
            result = "violation" if _term_matches(text, banned_term, "word", "NORMALIZED") else "no_violation"

        assert result == "no_violation"

    def test_banned_without_allowlist(self):
        """Без allowlist запрещённое слово ловится."""
        text = "это мат в тексте"
        banned_term = "мат"
        # Нет allowlist совпадений
        allow_match = _term_matches(text, "матрас", "word", "NORMALIZED")
        assert not allow_match

        result = "violation" if _term_matches(text, banned_term, "word", "NORMALIZED") else "no_violation"
        assert result == "violation"


# ─────────────────────────────────────────────
# Тесты wildcard-правил
# ─────────────────────────────────────────────

class TestWildcardRules:

    def test_is_wildcard_star(self):
        assert is_wildcard_rule("сло(*)во")

    def test_is_wildcard_plus(self):
        assert is_wildcard_rule("сло(+)во")

    def test_not_wildcard_bare_star(self):
        assert not is_wildcard_rule("мат*")

    def test_not_wildcard_bare_plus(self):
        assert not is_wildcard_rule("мат+")

    def test_validate_valid_star(self):
        ok, msg = validate_wildcard_rule("сло(*)во")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_valid_plus(self):
        ok, msg = validate_wildcard_rule("сло(+)во")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_invalid_no_left(self):
        # Маркер в начале строки теперь ДОПУСТИМ (маркер-префикс)
        ok, msg = validate_wildcard_rule("(*)слово")
        assert ok, f"Expected valid (marker at start is now allowed), got: {msg}"

    def test_validate_invalid_no_right(self):
        # Маркер в конце строки теперь ДОПУСТИМ (маркер-суффикс)
        ok, msg = validate_wildcard_rule("слово(*)")
        assert ok, f"Expected valid (marker at end is now allowed), got: {msg}"

    def test_validate_invalid_only_marker(self):
        ok, msg = validate_wildcard_rule("(*)")
        assert not ok

    def test_validate_invalid_space_near_marker(self):
        ok, msg = validate_wildcard_rule("слово (*)два")
        assert not ok, "Space before marker should be invalid"

    def test_validate_multiple_markers_valid(self):
        ok, msg = validate_wildcard_rule("а(*)б(+)в")
        assert ok, f"Expected valid: {msg}"

    def test_wildcard_star_matches_anything(self):
        assert _wildcard_matches("слово", "сло(*)во", "EXACT")
        assert _wildcard_matches("слоXXXво", "сло(*)во", "EXACT")
        assert _wildcard_matches("словово", "сло(*)во", "EXACT")  # empty match for (*)

    def test_wildcard_star_matches_empty(self):
        # (*) может быть пустым
        assert _wildcard_matches("слово", "сло(*)во", "NORMALIZED")

    def test_wildcard_plus_matches_nonempty(self):
        assert _wildcard_matches("слоXво", "сло(+)во", "EXACT")

    def test_wildcard_plus_no_space(self):
        # (+) не должен пересекать пробел
        assert not _wildcard_matches("сло во", "сло(+)во", "EXACT")

    def test_wildcard_with_mode_normalized(self):
        # В NORMALIZED режиме нормализуется и паттерн и текст
        assert _wildcard_matches("сло 123 во", "сло(*)во", "SPLIT_PROOF")

    def test_wildcard_with_confusables(self):
        # Латинские символы в шаблоне нормализуются
        assert _wildcard_matches("слоXво", "сло(+)во", "CONFUSABLES")


# ─────────────────────────────────────────────
# Тесты wildcard-маркеров в начале/конце строки
# ─────────────────────────────────────────────

class TestWildcardEdgeMarkers:
    """
    Проверяем новое поведение: маркеры (*) и (+) разрешены в начале и конце правила.
    """

    # ── validate: маркер в конце / начале теперь допустим ──

    def test_validate_suffix_star_valid(self):
        ok, msg = validate_wildcard_rule("бля(*)")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_suffix_plus_valid(self):
        ok, msg = validate_wildcard_rule("бля(+)")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_prefix_plus_valid(self):
        ok, msg = validate_wildcard_rule("(+)бля")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_prefix_star_valid(self):
        ok, msg = validate_wildcard_rule("(*)бля")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_both_sides_star_valid(self):
        ok, msg = validate_wildcard_rule("(*)бля(*)")
        assert ok, f"Expected valid, got: {msg}"

    def test_validate_only_marker_invalid(self):
        ok, msg = validate_wildcard_rule("(*)")
        assert not ok, "Only marker with no text should be invalid"

    def test_validate_two_markers_no_text_invalid(self):
        ok, msg = validate_wildcard_rule("(*)(+)")
        assert not ok, "Two markers with no text should be invalid"

    # ── бля(*) матчит различные суффиксы ──

    def test_suffix_star_matches_suffix(self):
        assert _wildcard_matches("бляха", "бля(*)", "EXACT")

    def test_suffix_star_matches_digits(self):
        assert _wildcard_matches("бля1", "бля(*)", "EXACT")

    def test_suffix_star_matches_long_suffix(self):
        assert _wildcard_matches("бля00000", "бля(*)", "EXACT")

    def test_suffix_star_matches_empty_suffix(self):
        # (*) может быть пустым — «бля» само по себе тоже матчится
        assert _wildcard_matches("бля", "бля(*)", "EXACT")

    def test_suffix_star_matches_in_context(self):
        # Матч в середине текста тоже должен работать (search)
        assert _wildcard_matches("это бляха тут", "бля(*)", "EXACT")

    # ── бля(+) требует хотя бы 1 символ, не пробел ──

    def test_suffix_plus_matches_nonempty(self):
        assert _wildcard_matches("бляха", "бля(+)", "EXACT")

    def test_suffix_plus_matches_digit(self):
        assert _wildcard_matches("бля1", "бля(+)", "EXACT")

    def test_suffix_plus_no_match_bare_word(self):
        # «бля» без продолжения — НЕ матчится с бля(+)
        assert not _wildcard_matches("бля", "бля(+)", "EXACT")

    def test_suffix_plus_no_match_with_space(self):
        # «бля слово» — (+) не пересекает пробел
        assert not _wildcard_matches("бля слово", "бля(+)", "EXACT")

    # ── (+)бля требует хотя бы 1 символ перед «бля» ──

    def test_prefix_plus_matches_prefix(self):
        assert _wildcard_matches("абля", "(+)бля", "EXACT")

    def test_prefix_plus_matches_digit_prefix(self):
        assert _wildcard_matches("1бля", "(+)бля", "EXACT")

    def test_prefix_plus_no_match_space_before(self):
        # Пробел перед «бля» — (+) не матчит пробел
        # В тексте " бля" на позиции 0 пробел не матчит \S+
        # Но «бля» без символов слева тоже не матчит
        result = _wildcard_matches("бля", "(+)бля", "EXACT")
        assert not result, "(+)бля should NOT match standalone 'бля'"

    # ── (*)бля(*) матчит как подстрока ──

    def test_both_star_matches_middle(self):
        assert _wildcard_matches("это бляха тут", "(*)бля(*)", "EXACT")

    def test_both_star_matches_prefix_only(self):
        assert _wildcard_matches("нубля123", "(*)бля(*)", "EXACT")

    def test_both_star_matches_exact(self):
        assert _wildcard_matches("бля", "(*)бля(*)", "EXACT")

    # ── Allowlist перебивает запрет с wildcard ──

    def test_allowlist_overrides_suffix_star(self):
        """бля(*) запрещено, но бляха в allowlist → не наказывать."""
        text = "бляха"
        banned_rule = "бля(*)"
        allow_rule = "бляха"
        # allowlist совпадает по точному термину
        allow_match = _term_matches(text, allow_rule, "word", "EXACT")
        if allow_match:
            result = "no_violation"
        else:
            result = "violation" if _wildcard_matches(text, banned_rule, "EXACT") else "no_violation"
        assert result == "no_violation"

    def test_allowlist_prefix_overrides(self):
        """(*)бля(*) запрещено, нубля(*) в allowlist — не наказывать нубля...."""
        text = "нубляха"
        banned_rule = "(*)бля(*)"
        allow_rule = "нубля(*)"
        allow_match = _wildcard_matches(text, allow_rule, "EXACT")
        if allow_match:
            result = "no_violation"
        else:
            result = "violation" if _wildcard_matches(text, banned_rule, "EXACT") else "no_violation"
        assert result == "no_violation"


# ─────────────────────────────────────────────
# Тесты парсинга терминов
# ─────────────────────────────────────────────

class TestParseTermLines:

    def test_single_word(self):
        terms = _parse_term_lines("плохоеслово")
        assert len(terms) == 1
        assert terms[0]["kind"] == "word"
        assert terms[0]["text"] == "плохоеслово"

    def test_single_phrase(self):
        terms = _parse_term_lines("плохая фраза")
        assert len(terms) == 1
        assert terms[0]["kind"] == "phrase"

    def test_multiple_lines(self):
        terms = _parse_term_lines("слово1\nслово2\nплохая фраза\nещё одна фраза")
        assert len(terms) == 4
        assert terms[0]["kind"] == "word"
        assert terms[1]["kind"] == "word"
        assert terms[2]["kind"] == "phrase"
        assert terms[3]["kind"] == "phrase"

    def test_empty_lines_ignored(self):
        terms = _parse_term_lines("слово1\n\n\nслово2\n")
        assert len(terms) == 2

    def test_normalized_stored(self):
        terms = _parse_term_lines("СЛОВО")
        assert terms[0]["normalized"] == _base_normalize("СЛОВО")

    def test_wildcard_preserved(self):
        terms = _parse_term_lines("сло(*)во")
        assert terms[0]["text"] == "сло(*)во"
        assert terms[0]["kind"] == "word"  # нет пробелов
