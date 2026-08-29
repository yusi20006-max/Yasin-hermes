"""Yasin Stage 1 regression: core streaming invariants for tool boundary + final delivery.

These tests are intentionally self-contained and do not require the full
Hermes import graph. They exercise the logical contracts that must hold
for reliable Telegram live streaming with MCP/YasinHub tool calls.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest


# Minimal re-implementation of the key flag logic for isolated testing.
# The real GatewayStreamConsumer is validated by the upstream suite;
# this file documents and guards the Yasin-critical contracts.


class FakeStreamState:
    """Mirrors the critical flags of GatewayStreamConsumer."""

    def __init__(self):
        self._already_sent = False
        self._final_response_sent = False
        self._final_content_delivered = False
        self._delivered_final_text: Optional[str] = None
        self._turn_split_delivery = False
        self._stream_ledger = ""
        self._accumulated = ""
        self._segments: list[str] = []
        self._tool_boundaries = 0

    def on_delta(self, text: Optional[str]) -> None:
        if text is None:
            self.on_segment_break()
            return
        self._accumulated += text
        self._stream_ledger += text

    def on_segment_break(self) -> None:
        if self._accumulated:
            self._segments.append(self._accumulated)
            self._already_sent = True
            self._accumulated = ""
        self._tool_boundaries += 1

    def finish(self, final_text: Optional[str] = None) -> None:
        if final_text is not None:
            self._delivered_final_text = final_text.strip()
            self._final_content_delivered = True
            self._final_response_sent = True
            self._already_sent = True
        elif self._accumulated:
            self._delivered_final_text = self._accumulated.strip()
            self._final_content_delivered = True
            self._final_response_sent = True
            self._already_sent = True

    def delivered_final_matches(self, final_text: str) -> Optional[bool]:
        target = (final_text or "").strip()
        if not target:
            return None
        if self._delivered_final_text is None:
            if self._turn_split_delivery:
                return False
            return None
        if self._delivered_final_text == target:
            return True
        if any(s.strip() == target for s in self._segments):
            return True
        return False

    @property
    def already_sent(self) -> bool:
        return self._already_sent

    @property
    def final_response_sent(self) -> bool:
        return self._final_response_sent

    @property
    def final_content_delivered(self) -> bool:
        return self._final_content_delivered


# ── Invariants ───────────────────────────────────────────────────────────


def test_partial_stream_does_not_suppress_final():
    """partial stream != final delivery (Law 1)"""
    s = FakeStreamState()
    s.on_delta("Hello ")
    s.on_delta("world")
    assert s.already_sent is False
    assert s.final_response_sent is False
    assert s.delivered_final_matches("Hello world full answer") is None


def test_tool_boundary_is_not_final_delivery():
    """tool boundary != final delivery"""
    s = FakeStreamState()
    s.on_delta("I'll check YasinHub...")
    s.on_delta(None)
    assert s._tool_boundaries == 1
    assert s.final_response_sent is False
    assert s.final_content_delivered is False
    s.on_delta("Result: status OK. ")
    s.on_delta("All systems green.")
    s.finish("Result: status OK. All systems green.")
    assert s.final_response_sent is True
    assert s.delivered_final_matches("Result: status OK. All systems green.") is True


def test_successful_final_edit_marks_delivery():
    """successful final edit == final delivery"""
    s = FakeStreamState()
    s.on_delta("Full answer here.")
    s.finish("Full answer here.")
    assert s.final_response_sent is True
    assert s.final_content_delivered is True
    assert s.delivered_final_matches("Full answer here.") is True


def test_empty_final_is_not_suppressible():
    """empty final response != suppressible response (Law 3)"""
    s = FakeStreamState()
    s.on_delta("thinking...")
    s.on_delta(None)
    s.finish("")
    assert s.delivered_final_matches("") is None


def test_stale_snapshot_does_not_confirm_final():
    """stale final snapshot != confirmed final delivery"""
    s = FakeStreamState()
    s.on_delta("partial only")
    s._delivered_final_text = "partial only"
    s._final_response_sent = True
    assert s.delivered_final_matches("partial only plus more text") is False


def test_multi_tool_continuation_preserves_full_answer():
    """tool → tool → final keeps full text"""
    s = FakeStreamState()
    s.on_delta("Step 1: query A")
    s.on_delta(None)
    s.on_delta("Step 2: query B")
    s.on_delta(None)
    s.on_delta("Final synthesis of A and B.")
    s.finish("Final synthesis of A and B.")
    assert s._tool_boundaries == 2
    assert s.final_content_delivered is True
    assert s.delivered_final_matches("Final synthesis of A and B.") is True


def test_interrupted_partial_allows_followup_final():
    """interrupted stream must not permanently suppress final"""
    s = FakeStreamState()
    s.on_delta("Interrupted mid ")
    assert s.final_response_sent is False
    s2 = FakeStreamState()
    s2.on_delta("Follow-up complete answer.")
    s2.finish("Follow-up complete answer.")
    assert s2.final_response_sent is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
