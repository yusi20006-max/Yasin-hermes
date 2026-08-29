# Yasin Hermes — Telegram Streaming Execution Plan

## Objective
Make Hermes Telegram streaming reliable enough to operate YasinHub through a single conversational request, without requiring manual re-checks after every tool call.

## Baseline
- Upstream-derived fork: `yusi20006-max/Yasin-hermes`
- Baseline branch: `main`
- Working branch: `feat/yasin-telegram-streaming`
- Baseline HEAD: `ac6c8028e00d01ee2f299ba7fd03329c7f10382d`

## Stages

### Stage 1 — Baseline and regression contract
- Verify current Telegram transport (`auto` / `draft` / `edit`).
- Add focused tests for progressive DM streaming.
- Verify final delivery flags and fallback behavior.
- Verify interruption and queued-follow-up behavior.
- Acceptance: partial output is visible before generation completes; final answer is delivered exactly once.

### Stage 2 — Telegram transport correctness
- Audit native draft path and edit fallback.
- Verify DM-only draft gating and group/topic behavior.
- Verify rate limiting/flood-control recovery.
- Verify message/thread/reply metadata survives streaming.
- Acceptance: Telegram stays live without freezes, duplicates, or stale previews.

### Stage 3 — Tool/MCP boundary continuity
- Verify stream → tool call → tool result → stream continuation.
- Verify YasinHub/MCP tool calls do not terminate or suppress the final answer.
- Verify long tool calls keep the user-facing activity state alive.
- Acceptance: a YasinHub inspection can run through multiple tools while Telegram remains responsive.

### Stage 4 — Finalization and delivery integrity
- Reconcile streamed payload with `final_response`.
- Ensure transformed/post-processed responses are not lost.
- Cover split-message finalization and Telegram's message-size limit.
- Prevent both duplicate final sends and missing final sends.
- Acceptance: the final Telegram message contains the complete authoritative answer.

### Stage 5 — Production hardening
- Add regression coverage for cancellation, interruption, provider failure, empty final responses, and fallback.
- Run the relevant gateway/streaming test matrix in CI.
- Document the supported Telegram configuration for YasinHub.
- Acceptance: CI green and end-to-end YasinHub workflow is stable.

## Upstream findings to track
- Hermes upstream has active/recent streaming delivery issues, including Telegram visual ghosting and split-message delivery risks.
- Current fork already contains a substantial `GatewayStreamConsumer` implementation with `auto`/`draft`/`edit` transport selection; do not replace it wholesale. Fix and test the existing architecture first.

## Issue tracker
GitHub Issues are currently disabled on this fork, so issue creation is blocked by repository settings. This plan is intentionally kept in-repo until Issues are enabled; once enabled, each Stage above becomes one issue and implementation proceeds in order.
