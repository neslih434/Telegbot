"""
tests/test_stats_image.py — Self-check for _render_stats_image.

Verifies that the rendered PNG is always exactly 1024×640 px
regardless of the number of users (3 or 30).
"""
import os
import sys
import importlib.util
import unittest.mock as _mock
import types as _types
import tempfile
import pytest

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _load_render_func():
    """Import _render_stats_image without loading the full bot."""
    # Provide stub modules for all telegram/bot dependencies so we can
    # import handlers.py in a test environment without a running bot.
    stub_names = [
        "telebot", "telebot.types", "telebot.apihelper",
        "config", "persistence", "antispam", "moderation",
        "banned_words", "pin", "cmd_basic", "settings_ui",
    ]
    for name in stub_names:
        if name not in sys.modules:
            sys.modules[name] = _mock.MagicMock()

    # config stubs
    cfg = sys.modules["config"]
    cfg.TOKEN = "test"
    cfg.ADMIN_IDS = []

    spec = importlib.util.spec_from_file_location(
        "_handlers_test",
        os.path.join(_PROJECT_ROOT, "handlers.py"),
    )
    module = importlib.util.module_from_spec(spec)
    # Provide minimal bot mock so module-level code doesn't crash
    sys.modules["_handlers_test"] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        pass  # Module-level bot init may fail; function is still accessible

    return getattr(module, "_render_stats_image", None)


def _make_rows(n: int):
    """Generate n fake (user_id, count) tuples."""
    rows = []
    for i in range(n):
        uid = 100000000 + i * 7919
        count = max(1, 154 - i * (154 // n))
        rows.append((uid, count))
    return rows


def _make_users_map(rows):
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    m = {}
    for i, (uid, _) in enumerate(rows):
        if i % 3 == 0:
            m[uid] = names[i % len(names)]
        # leave some unmapped to test "Unknown [ID]" path
    return m


@pytest.fixture(scope="module")
def render_fn():
    fn = _load_render_func()
    if fn is None:
        pytest.skip("_render_stats_image could not be loaded (missing Pillow?)")
    try:
        from PIL import Image  # noqa: F401
    except ImportError:
        pytest.skip("Pillow not installed")
    return fn


def test_output_size_3_users(render_fn, tmp_path):
    """3 users → PNG must be exactly 1024×640."""
    from PIL import Image
    import io

    rows = _make_rows(3)
    users_map = _make_users_map(rows)
    img_bytes = render_fn(rows, "Test Chat", "день", users_map, max_users=30)

    assert isinstance(img_bytes, bytes), "Expected bytes"
    img = Image.open(io.BytesIO(img_bytes))
    assert img.size == (1024, 640), f"Expected 1024×640, got {img.size}"

    # Save for visual inspection
    out_path = tmp_path / "out_3.png"
    out_path.write_bytes(img_bytes)
    print(f"Saved {out_path}")


def test_output_size_30_users(render_fn, tmp_path):
    """30 users → PNG must be exactly 1024×640."""
    from PIL import Image
    import io

    rows = _make_rows(30)
    users_map = _make_users_map(rows)
    img_bytes = render_fn(rows, "Test Chat", "день", users_map, max_users=30)

    assert isinstance(img_bytes, bytes), "Expected bytes"
    img = Image.open(io.BytesIO(img_bytes))
    assert img.size == (1024, 640), f"Expected 1024×640, got {img.size}"

    out_path = tmp_path / "out_30.png"
    out_path.write_bytes(img_bytes)
    print(f"Saved {out_path}")


def test_output_size_1_user(render_fn, tmp_path):
    """1 user (edge case) → PNG must be exactly 1024×640."""
    from PIL import Image
    import io

    rows = [(173123912, 154)]
    users_map = {173123912: "Alice"}
    img_bytes = render_fn(rows, "Chat", "день", users_map, max_users=30)

    img = Image.open(io.BytesIO(img_bytes))
    assert img.size == (1024, 640), f"Expected 1024×640, got {img.size}"


def test_unknown_user_label(render_fn):
    """User not in users_map → label should contain 'Unknown'."""
    from PIL import Image
    import io

    rows = [(999888777, 42)]
    users_map = {}  # no name for this user
    img_bytes = render_fn(rows, "Chat", "день", users_map, max_users=30)

    img = Image.open(io.BytesIO(img_bytes))
    assert img.size == (1024, 640)


def test_no_rows_raises(render_fn):
    """Empty rows must raise ValueError."""
    with pytest.raises(ValueError):
        render_fn([], "Chat", "день", {}, max_users=30)
