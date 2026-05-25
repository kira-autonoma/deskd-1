# deskd

[![CI](https://github.com/kgatilin/deskd/actions/workflows/ci.yml/badge.svg)](https://github.com/kgatilin/deskd/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kgatilin/deskd/graph/badge.svg)](https://codecov.io/gh/kgatilin/deskd)

**Agent orchestration runtime — fractal message bus for AI agents**

Spawn, route, and manage AI agents. Each agent gets its own isolated message bus (Unix socket), persistent session, and worker loop. Agents communicate via pub/sub routing; adapters bridge external platforms (Telegram, Discord). Skill graphs encode multi-step workflows as executable DAGs.

---

## Quick Start

**Install** (prebuilt binary):
```bash
curl -fsSL https://raw.githubusercontent.com/kgatilin/deskd/main/install.sh | bash
```

**Configure** a workspace:
```yaml
# workspace.yaml
agents:
  - name: dev
    work_dir: /home/dev
    command: [claude, --output-format, stream-json, ...]
    telegram:
      token: ${BOT_TOKEN}   # optional
```

**Run**:
```bash
deskd serve --config workspace.yaml
```

---

## Architecture

```
workspace.yaml
    ↓
deskd serve
    ↓
┌──────────────────────┐     ┌──────────────────────┐
│ agent: kira           │     │ agent: dev            │
│ bus.sock              │     │ bus.sock              │
│ worker (Claude)       │     │ worker (Claude)       │
│ [telegram adapter]    │     │                       │
└──────────────────────┘     └──────────────────────┘
```

Each agent is fully isolated — its own socket, inbox, logs, and session state. Agents discover each other via the bus routing protocol.

**Source modules:**

| Module | Role |
|--------|------|
| `bus.rs` | Per-agent Unix socket pub/sub with glob routing |
| `worker.rs` | Worker loop: read bus → execute via Claude → post result |
| `agent.rs` | Agent state, create/recover, streaming Claude calls |
| `inbox.rs` | File-based inbox for async task results |
| `mcp.rs` | MCP server (stdio JSON-RPC) for Claude tool integration |
| `graph.rs` | Skill graph engine: DAG of tool groups + LLM decision nodes |
| `schedule.rs` | Cron-based scheduled actions on the bus |
| `adapters/telegram.rs` | Telegram bot adapter (teloxide) |
| `unified_inbox` | Unified view across all agent inboxes |

---

## Key Features

- **Persistent sessions** — agents survive restarts; session ID and cost are tracked in `~/.deskd/agents/<name>.yaml`
- **Sub-agents** — spawn child agents dynamically via MCP `add_persistent_agent`
- **Skill graphs** — YAML-defined DAGs mixing tool execution and LLM decision nodes; run with `deskd graph run`
- **MCP tools** — `deskd mcp` exposes `send_message`, `add_persistent_agent`, `run_graph` as Claude tools
- **Telegram & Discord adapters** — route chat messages to/from the agent bus
- **Schedule system** — cron-based triggers defined in `deskd.yaml`
- **Unified inbox** — async task results readable by sender name

---

## CLI

```bash
# Start all agents
deskd serve --config workspace.yaml

# Send a task to an agent
deskd agent send <name> "task" --socket <bus.sock>

# Read inbox (async replies)
deskd agent read <sender>
deskd agent read <sender> --clear   # read and delete

# Agent management
deskd agent list --socket <bus.sock>
deskd agent stats <name>
deskd agent create <name> [--prompt ...] [--model ...]
deskd agent rm <name>

# Skill graphs
deskd graph run <file.yaml>
deskd graph run <file.yaml> --work-dir .
deskd graph validate <file.yaml>

# MCP server (invoked by Claude via --mcp-config)
deskd mcp --agent <name>
```

---

## Configuration

### `workspace.yaml` — defines agents for `deskd serve`

```yaml
agents:
  - name: dev
    work_dir: /home/dev
    command: [claude, --output-format, stream-json, ...]
    telegram:
      token: ${BOT_TOKEN}
```

### `deskd.yaml` — per-agent config (at `{work_dir}/deskd.yaml`)

```yaml
model: claude-sonnet-4-6
system_prompt: "You are..."
max_turns: 100
mcp_config: '{"mcpServers":{...}}'
telegram:
  routes:
    - chat_id: -1234567890
agents: []      # sub-agents
schedules: []   # cron jobs
channels: []    # named bus targets
```

### `web:` — optional web control panel (#443)

When the workspace defines a `web:` block with `enabled: true`, `deskd serve`
also starts a small axum HTTP server with Telegram magic-link login.

```yaml
web:
  enabled: true
  bind: 127.0.0.1:8127             # local-only; reverse proxy handles TLS
  external_url: https://deskd.example.com
  session_ttl_days: 30
  magic_link_ttl_seconds: 300
  allowed_telegram_ids: [123456]
  audit_log: ~/.deskd/logs/web-audit.jsonl
  rate_limit:
    auth_requests_per_hour: 20
```

Bind is intentionally local. Front it with a reverse proxy (caddy, nginx) that
terminates TLS, forwards `X-Forwarded-For`, and proxies to `127.0.0.1:8127`.

### `github_webhooks:` — optional GitHub webhook adapter (#457)

When the workspace defines a `github_webhooks:` block, the web adapter
(requires `web.enabled: true`) mounts `POST /webhooks/github`. Signed payloads
are HMAC-SHA256-verified against `secret` (constant-time compare against the
`X-Hub-Signature-256` header), then matched against `subscriptions` and
delivered to the configured bus targets.

```yaml
web:
  enabled: true                       # required — endpoint is mounted on the web server
  bind: 127.0.0.1:8127

github_webhooks:
  secret: "${GITHUB_WEBHOOK_SECRET}"  # shared HMAC-SHA256 secret; env-interpolated
  subscriptions:
    - repo: kgatilin/archai           # owner/name from payload.repository.full_name
      events:
        - pull_request.closed         # X-GitHub-Event + ".action" (both must match)
        - pull_request_review.submitted
        - issues.labeled
      deliver_to: agent:kira          # any bus target — agent:<name>, queue:<name>, etc.
    - repo: kgatilin/deskd
      events:
        - ping                        # bare event matches any action (also matches no action)
        - pull_request
      deliver_to: agent:kira
```

Configure the matching webhook on GitHub:

- **Payload URL**: `https://deskd.example.com/webhooks/github` (the public host
  of your reverse proxy).
- **Content type**: `application/json` — form-encoded payloads cannot be HMAC-verified.
- **Secret**: the same value as `secret:` above. Stored in the env so the
  workspace YAML can be committed.
- **SSL verification**: enabled.
- **Events**: whichever ones you listed under `events` (or "Send me everything"
  if you prefer to filter server-side).

**Event matching.** Each `events:` entry is the `X-GitHub-Event` header value,
optionally suffixed with `.action`. A bare event (e.g. `ping`, `pull_request`)
matches any action — useful when you want every PR event regardless of state
transition. A qualified event (e.g. `pull_request.closed`) matches only that
action. Multiple subscriptions for the same repo are permitted and fan-out
independently.

Bad signatures return `401`; events not matching any subscription are ack'd
with `200` and dropped. Each successful delivery is logged at `info`.

### Key paths

| Path | Purpose |
|------|---------|
| `{work_dir}/.deskd/bus.sock` | Agent's message bus socket |
| `~/.deskd/agents/{name}.yaml` | Agent state (cost, turns, session_id) |
| `~/.deskd/inbox/{sender}/` | Task results for async reading |
| `~/.deskd/logs/{name}.log` | Agent logs |

---

## Bus Protocol

Unix socket, newline-delimited JSON.

```json
// Register
{"type":"register","name":"cli","subscriptions":["agent:cli"]}

// Send message
{"type":"message","id":"uuid","source":"cli","target":"agent:dev","payload":{"task":"..."}}

// List clients
{"type":"list"}
```

Routing targets: `agent:<name>`, `queue:<name>`, `telegram.out:<chat_id>`, `broadcast`, glob patterns (`agent:*`).

---

## Build

```bash
cargo build --release

# Linux static (containers/VPS)
cargo build --release --target x86_64-unknown-linux-musl

# macOS — re-sign after build
codesign --force --sign - target/release/deskd
```

Quality gate (CI requires all three):
```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
```

---

## License

MIT
