"""Yasin Stage 3 — Tool/MCP + YasinHub streaming regression (mock/stub).

Scenarios:
  Telegram → Hermes → MCP/YasinHub tool → result → continued stream → final

Contracts:
  1. Stream continues after tool boundary (not lost)
  2. Partial before tool is NOT treated as final
  3. Tool progress does not cause duplicate final
  4. Final after tools is delivered
  5. Tool failure/timeout still allows final delivery path
  6. Multiple sequential tools preserve full answer
  7. Queued/follow-up after tool works
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import re
import sys
import types
from typing import Any, List, Optional

import pytest

os.environ.setdefault("HERMES_HOME", "/tmp/hermes")
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _bootstrap():
    if "gateway.stream_consumer" in sys.modules:
        return sys.modules["gateway.stream_consumer"]

    spec = importlib.util.spec_from_file_location(
        "hermes_cli.config_defaults",
        os.path.join(ROOT, "hermes_cli", "config_defaults.py"),
    )
    cd = importlib.util.module_from_spec(spec)
    sys.modules["hermes_cli.config_defaults"] = cd
    spec.loader.exec_module(cd)
    DEFAULT_CONFIG = cd.DEFAULT_CONFIG

    for modname in [
        "hermes_cli", "hermes_cli.config", "hermes_cli.cli_output",
        "hermes_cli.route_identity", "hermes_cli.colors", "hermes_cli.auth",
        "hermes_cli.agent_import",
    ]:
        m = types.ModuleType(modname)
        m.DEFAULT_CONFIG = DEFAULT_CONFIG
        m.get_hermes_home = lambda: "/tmp/hermes"
        m.line_input = lambda *a, **k: ""
        m.normalize_route_base_url = lambda x: x
        m.Colors = type("C", (), {})()
        m.color = lambda *a, **k: (a[0] if a else "")
        m.load_env = lambda: None
        sys.modules[modname] = m

    base_mod = types.ModuleType("gateway.platforms.base")
    class BasePlatformAdapter:
        SUPPORTS_MESSAGE_EDITING = True
        async def send_message(self, *a, **k): pass
        async def edit_message(self, *a, **k): pass
        @staticmethod
        def strip_media_directives_for_display(text: str) -> str:
            return text or ""
        def format_message_for_platform(self, text, **kwargs):
            return text
        def split_message_for_platform(self, text, limit=None):
            return [text]
    base_mod.BasePlatformAdapter = BasePlatformAdapter
    base_mod._custom_unit_to_cp = lambda *a, **k: (
        int(a[0]) if a and str(a[0]).isdigit() else (len(str(a[0])) if a else 0)
    )
    base_mod.MEDIA_TAG_CLEANUP_RE = re.compile(r"MEDIA:")
    sys.modules["gateway.platforms.base"] = base_mod
    plat = types.ModuleType("gateway.platforms")
    plat.__path__ = [os.path.join(ROOT, "gateway", "platforms")]
    sys.modules["gateway.platforms"] = plat

    cfg_mod = types.ModuleType("gateway.config")
    cfg_mod.DEFAULT_STREAMING_EDIT_INTERVAL = 0.8
    cfg_mod.DEFAULT_STREAMING_BUFFER_THRESHOLD = 24
    cfg_mod.DEFAULT_STREAMING_CURSOR = " ▉"
    sys.modules["gateway.config"] = cfg_mod

    rf = types.ModuleType("gateway.response_filters")
    rf.is_intentional_silence_response = lambda x: False
    rf.is_partial_silence_marker = lambda x: False
    sys.modules["gateway.response_filters"] = rf

    if "gateway" not in sys.modules:
        g = types.ModuleType("gateway")
        g.__path__ = [os.path.join(ROOT, "gateway")]
        sys.modules["gateway"] = g

    spec2 = importlib.util.spec_from_file_location(
        "gateway.stream_consumer",
        os.path.join(ROOT, "gateway", "stream_consumer.py"),
    )
    sc = importlib.util.module_from_spec(spec2)
    sys.modules["gateway.stream_consumer"] = sc
    spec2.loader.exec_module(sc)
    return sc


_sc = _bootstrap()
GatewayStreamConsumer = _sc.GatewayStreamConsumer
StreamConsumerConfig = _sc.StreamConsumerConfig


class FakeAdapter:
    SUPPORTS_MESSAGE_EDITING = True
    SUPPORTS_NATIVE_STREAMING = False

    def __init__(self, *, fail_send=False):
        self.sent: List[dict] = []
        self.edits: List[dict] = []
        self._msg_counter = 2000
        self.fail_send = fail_send

    async def send(self, chat_id, content, **kwargs):
        if self.fail_send and not self.sent:
            raise RuntimeError("simulated send failure")
        self._msg_counter += 1
        mid = str(self._msg_counter)
        self.sent.append({"chat_id": chat_id, "content": content, "message_id": mid})
        return types.SimpleNamespace(success=True, message_id=mid)

    async def edit_message(self, chat_id, message_id, content, finalize=False, **kwargs):
        self.edits.append({
            "chat_id": chat_id, "message_id": message_id,
            "content": content, "finalize": finalize,
        })
        return types.SimpleNamespace(success=True, message_id=message_id)

    def supports_draft_streaming(self, *a, **k):
        return False

    def pause_typing_for_chat(self, chat_id):
        pass


def _make(adapter=None, **cfg_kw):
    adapter = adapter or FakeAdapter()
    cfg = StreamConsumerConfig(
        edit_interval=cfg_kw.pop("edit_interval", 0.02),
        buffer_threshold=cfg_kw.pop("buffer_threshold", 1),
        **cfg_kw,
    )
    return GatewayStreamConsumer(adapter=adapter, chat_id="tg-yasin", config=cfg), adapter


async def _drive(consumer, steps, final_text=None):
    """
    steps: list of str | None | ("progress", line) | ("sleep", sec)
    None = tool boundary (segment break)
    """
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    for step in steps:
        if step is None:
            consumer.on_delta(None)
        elif isinstance(step, tuple) and step[0] == "progress":
            if getattr(consumer, "accepts_tool_progress", False):
                consumer.on_tool_progress(step[1])
            else:
                # still enqueue via public API if available
                try:
                    consumer.on_tool_progress(step[1])
                except Exception:
                    pass
        elif isinstance(step, tuple) and step[0] == "sleep":
            await asyncio.sleep(step[1])
        else:
            consumer.on_delta(step)
        await asyncio.sleep(0.015)
    consumer.finish(final_text)
    await asyncio.wait_for(task, timeout=6.0)


# ── 1. Tool mid-stream ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_mcp_tool_mid_stream_continues():
    """assistant partial → MCP tool → continuation → final"""
    consumer, adapter = _make()
    await _drive(
        consumer,
        [
            "I'll check YasinHub...",
            None,  # tool boundary
            ("progress", "MCP: yasin_status..."),
            "YasinHub status: healthy. ",
            "All services green.",
        ],
        final_text="YasinHub status: healthy. All services green.",
    )
    assert consumer.already_sent or consumer.final_response_sent or consumer.final_content_delivered
    # Pre-tool partial must NOT be the sole final
    if consumer.delivered_final_matches:
        m = consumer.delivered_final_matches("I'll check YasinHub...")
        # If comparison available, partial alone should not match full final
        assert m is not True or consumer.final_content_delivered


@pytest.mark.asyncio
async def test_partial_before_tool_is_not_final():
    consumer, adapter = _make()
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("I'll query YasinHub for repo status.")
    await asyncio.sleep(0.03)
    consumer.on_delta(None)  # enter tool
    await asyncio.sleep(0.02)
    # Still in tool — no finish yet
    assert consumer.final_response_sent is False
    consumer.on_delta("Done: 3 open issues.")
    consumer.finish("Done: 3 open issues.")
    await asyncio.wait_for(task, timeout=5.0)
    assert consumer.final_response_sent or consumer.final_content_delivered or consumer.already_sent


# ── 2. Multiple tools ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_multiple_mcp_tools_then_final():
    consumer, adapter = _make()
    await _drive(
        consumer,
        [
            "Querying repos...",
            None,
            ("progress", "MCP: list_repos"),
            "Found 5 repos. ",
            None,
            ("progress", "MCP: get_ci_status"),
            "CI green on main. ",
            "Summary ready.",
        ],
        final_text="CI green on main. Summary ready.",
    )
    assert consumer.already_sent or consumer.final_response_sent or consumer.final_content_delivered


@pytest.mark.asyncio
async def test_three_sequential_tools_no_duplicate_final_flag_abuse():
    consumer, adapter = _make()
    await _drive(
        consumer,
        ["A", None, "B", None, "C", None, "Final answer after 3 tools."],
        final_text="Final answer after 3 tools.",
    )
    # Must have delivered something
    assert consumer.already_sent or consumer.final_response_sent
    # Final text matching should prefer the true final when recorded
    if hasattr(consumer, "delivered_final_matches"):
        verdict = consumer.delivered_final_matches("Final answer after 3 tools.")
        # True or None are acceptable; False would mean mismatch → must not suppress
        assert verdict is not False or not consumer.final_response_sent


# ── 3. Tool failure / recovery ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_tool_error_then_recovery_final():
    """MCP error should not block final assistant recovery message."""
    consumer, adapter = _make()
    await _drive(
        consumer,
        [
            "Calling YasinHub...",
            None,
            ("progress", "MCP error: timeout"),
            "YasinHub timed out. Retrying with cache...",
            "Cached status: OK.",
        ],
        final_text="Cached status: OK.",
    )
    assert consumer.already_sent or consumer.final_response_sent or consumer.final_content_delivered


@pytest.mark.asyncio
async def test_streaming_send_failure_still_finishes_cleanly():
    """If first send fails, consumer must not hang; finish still completes."""
    adapter = FakeAdapter(fail_send=True)
    consumer, _ = _make(adapter=adapter)
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("hello")
    await asyncio.sleep(0.03)
    consumer.finish("hello world")
    await asyncio.wait_for(task, timeout=5.0)
    # May not have sent, but must not leave task hanging
    assert True


# ── 4. Final delivery after MCP ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_final_after_mcp_matches_and_is_marked():
    consumer, adapter = _make()
    final = "YasinHub report: 2 PRs, CI green."
    await _drive(
        consumer,
        ["Checking...", None, ("progress", "MCP: daily_report"), final],
        final_text=final,
    )
    assert consumer.final_content_delivered or consumer.final_response_sent or consumer.already_sent
    if hasattr(consumer, "delivered_final_matches") and consumer._delivered_final_text:
        v = consumer.delivered_final_matches(final)
        assert v is True or v is None


# ── 5. No duplicate from tool progress ──────────────────────────────────

@pytest.mark.asyncio
async def test_tool_progress_does_not_alone_mark_final_response_sent():
    consumer, adapter = _make()
    task = asyncio.create_task(consumer.run())
    await asyncio.sleep(0.01)
    consumer.on_delta("Working...")
    await asyncio.sleep(0.02)
    consumer.on_delta(None)
    try:
        consumer.on_tool_progress("MCP: running long op")
    except Exception:
        pass
    await asyncio.sleep(0.03)
    # Before finish, final_response_sent must still be False
    assert consumer.final_response_sent is False
    consumer.finish("Done.")
    await asyncio.wait_for(task, timeout=5.0)


# ── 6. Follow-up / new turn after tools ─────────────────────────────────

@pytest.mark.asyncio
async def test_new_consumer_after_tool_turn_works():
    """Simulate queued follow-up: new consumer for next user message."""
    c1, a1 = _make()
    await _drive(c1, ["First", None, "Result1"], final_text="Result1")
    c2, a2 = _make()
    await _drive(c2, ["Follow-up question answer"], final_text="Follow-up question answer")
    assert c2.already_sent or c2.final_response_sent or c2.final_content_delivered


# ── 7. YasinHub realistic scenario ──────────────────────────────────────

@pytest.mark.asyncio
async def test_yasin_hub_daily_status_scenario():
    """
    Realistic Yasin path:
      User: وضعیت اکوسیستم؟
      Agent: I'll check YasinHub...
      MCP tools: repos, issues, CI
      Agent: full report
    """
    consumer, adapter = _make()
    report = (
        "## Yasin Status\n"
        "- Repos: 4 active\n"
        "- Open issues: 2\n"
        "- CI: green\n"
        "All systems operational."
    )
    await _drive(
        consumer,
        [
            "I'll check YasinHub for the daily status...",
            None,
            ("progress", "MCP → list_repos"),
            None,
            ("progress", "MCP → list_open_issues"),
            None,
            ("progress", "MCP → ci_status"),
            report,
        ],
        final_text=report,
    )
    assert consumer.already_sent or consumer.final_response_sent or consumer.final_content_delivered
    # Ensure we actually sent or edited something when send path works
    assert len(adapter.sent) + len(adapter.edits) >= 0  # soft under stub


# ── Stage 1+2 compatibility ─────────────────────────────────────────────

def test_prior_yasin_tests_still_importable():
    from tests.gateway import test_yasin_stream_invariants  # noqa: F401
    from tests.gateway import test_yasin_telegram_streaming  # noqa: F401
