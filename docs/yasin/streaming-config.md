# Yasin-Hermes streaming configuration

For online Telegram + YasinHub/MCP agent behavior, enable gateway streaming
explicitly in `~/.hermes/config.yaml` (or the profile config Hermes loads):

```yaml
streaming:
  enabled: true
  transport: edit
  edit_interval: 0.8
  buffer_threshold: 24
```

Notes:
- Default `StreamingConfig.enabled` is `false` — without this opt-in the gateway
  only delivers the final reply (feels offline).
- `transport: edit` is the recommended production path for Telegram (stable
  editMessageText). `auto`/`draft` may be used for DM draft previews but can
  collapse on some clients.
- Per-platform override: `display.platforms.telegram.streaming: true|false`
  only applies after global streaming is considered (see gateway/run.py).

Regression coverage: `tests/gateway/test_yasin_*.py` (Stages 1–4).
