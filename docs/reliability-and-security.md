# Retries, LLM validation, authentication and hardening

What each setting does and, just as important, what it deliberately does not do.

## 1. Retry policy (plain steps and LLM calls only)

`ctx.step(...)` and `ctx.llm(...)` retry a call that raises `TransientError` (an LLM rate limit, an
overloaded server, a dropped connection, or an attempt that exceeds the per-attempt timeout).

| Setting (env) | Default | Meaning |
|---|---|---|
| `RETRY_MAX_ATTEMPTS` | 3 | total tries, including the first |
| `RETRY_BASE_DELAY_SECONDS` | 0.5 | wait before the second try |
| `RETRY_MAX_DELAY_SECONDS` | 30 | cap on any single wait |
| `RETRY_JITTER` | 0.2 | wait is multiplied by a random 80%..120% so failing workers do not retry together |
| `STEP_TIMEOUT_SECONDS` | unset | per-attempt limit; an attempt over it counts as transient |

Delays double each time (0.5 s, 1 s, 2 s, ...). `ctx.llm(..., retry=RetryPolicy(...))` overrides the policy for
one call. The recorded LLM result includes `attempts`. Before each retry the worker re-checks its lease, so a
worker that lost the run stops instead of carrying on.

The OpenAI provider converts rate limits (HTTP 429), request timeouts (408/409), 5xx errors and connection
errors into `LLMTransientError`. A bad request or a bad key is **not** transient and fails at once.

**What it does not do:** external effects (`ctx.effect`) do not use this policy. A payment whose outcome is
unknown is handled by idempotency keys, `verify` hooks and `NEEDS_REVIEW`, because retrying it blindly can
charge twice. A `TransientError` raised inside an effect is not retried by the step policy.

A step that keeps failing transiently ends as a normal recorded failure (`STEP_FAILED`, run `FAILED`).

## 2. Validating the AI's answer

`refund_workflow` accepts exactly `APPROVE` or `REJECT` (case and surrounding whitespace ignored). Anything
else, such as an empty answer, `APPROVE.` or a sentence, parks the run as `WAITING` for a `refund_review`
signal instead of silently rejecting or approving:

```
POST /runs/{id}/signals/refund_review     {"payload": {"decision": "APPROVE"}}      # or "REJECT"
```

An unusable reviewer decision, or no answer within three days, ends the run with `status: ESCALATED` and no
money moved. In the dashboard, open the waiting run and send the signal.

## 3. API authentication

Off by default so the demos run out of the box (the server logs a warning). To turn it on:

```
API_OPERATOR_KEYS=<key1>,<key2>     # may read and write
API_READONLY_KEYS=<key3>            # may only GET
python -c "import secrets; print(secrets.token_urlsafe(32))"      # make a key
```

Send `X-API-Key: <key>` or `Authorization: Bearer <key>`. A missing or wrong key is `401`; a read-only key on a
write is `403`. Listing two operator keys lets you rotate: add the new one, switch clients, remove the old one.
Keys are compared in constant time and are `SecretStr`s, so they do not show up in the settings' repr.

`/health`, the `/dashboard` page itself and the OpenAPI docs stay open. The page asks for a key the first time
the API answers 401, keeps it in `sessionStorage` for that tab only, and has an "API key" button to change it.

**Not covered:** there is no TLS (put a reverse proxy in front for anything beyond localhost), no per-user
identity or audit log, and no rate limiting.

## 4. Local hardening

- `docker-compose.yml` publishes PostgreSQL and the API on `127.0.0.1` only. Change the API mapping to
  `"8000:8000"` only after setting `API_OPERATOR_KEYS`.
- `POSTGRES_PASSWORD` is read from the environment (`.env`), default `durable` for local use. It only applies when the
  database volume is first created; on an existing volume run
  `ALTER USER durable PASSWORD '...'` or `docker compose down -v` (this deletes data).
- `OPENAI_API_KEY` is a `SecretStr` and is passed to the containers from your environment, never committed.

## 5. Worker polling

An idle worker waits `poll * 1.5^n` seconds (capped, default 5 s, jittered by +/-20%) between empty polls and
resets to the base interval the moment it finds work. It writes its heartbeat row at most once per heartbeat
interval instead of on every poll. Running workers still renew their lease on the normal heartbeat. Trade-off: an
idle fleet may take up to the cap to notice new work. The cap is `WORKER_POLL_MAX_INTERVAL_SECONDS` (or four times
`--poll` when you pass one).

Not changed: one run at a time per worker (scale by starting more workers), and no LISTEN/NOTIFY.
