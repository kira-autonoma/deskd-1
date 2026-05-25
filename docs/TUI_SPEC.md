# deskd TUI — Terminal UI Specification

## Overview

Terminal UI for deskd — the primary interface for observing and controlling the agent orchestration runtime. Replaces Telegram as the main interaction channel.

**Framework:** Ink (React for terminal) + TypeScript. Same paradigm as Claude Code.
**Architecture:** Separate process, communicates with deskd via Unix socket bus.
**Launch:** `deskd tui` spawns the Ink process and connects to the running `deskd serve`.

## Why Ink / React

1. Claude Code uses Ink — proven in production for exactly this kind of UI
2. React is the UI paradigm LLMs write best — agents can modify/extend the TUI
3. Declarative components map naturally to "widget = observable" concept
4. Flexbox layout, hooks, state management — all built in
5. Rich ecosystem: syntax highlighting, markdown rendering, spinners, tables

## Core Concept: Observability Graph

The TUI is built on top of an **observability graph** — a declarative data structure that defines what the system observes and how observations relate to each other.

**Every widget on screen is a node in the graph. Every data subscription is an edge.** The graph IS the behavioral specification of the system. Adding a widget = extending the spec. Removing a widget = narrowing what you observe, which means the underlying code graph could potentially be simplified.

### Graph Structure

```typescript
interface ObservabilityGraph {
  nodes: ObservableNode[]
  edges: ObservableEdge[]
}

interface ObservableNode {
  id: string                          // unique identifier
  source: string                      // data source (see Node Source Types below)
  type: 'poll' | 'stream' | 'derived' // how data arrives
  interval?: number                   // poll interval in ms (for type: poll)
  trigger?: string                    // bus event pattern that forces refresh (for type: poll)
  transform?: (raw: any) => any       // optional data transformation
  render?: 'table' | 'text' | 'json' | 'list' | 'chart' // how to display (for shell/http sources)
  columns?: string[]                  // which fields to show (for render: table)
  params?: Record<string, string>     // parameters, supports $selected interpolation
}

interface ObservableEdge {
  from: string       // source node id
  to: string         // target node id
  on: string         // what triggers navigation/filtering: "select", field name, event
  type: 'navigate' | 'filter' | 'derive'
}
```

### Node Source Types

Nodes can pull data from different source types:

```typescript
// Built-in deskd sources
{ source: "deskd:query/agent_list" }          // bus API query
{ source: "bus:*" }                            // raw bus stream
{ source: "bus:agent:dev" }                    // filtered bus stream

// Shell commands — execute any command, parse output
{ source: "shell:gh issue list --repo kgatilin/deskd --json number,title,state --jq '.[]'" }
{ source: "shell:gh pr list --repo kgatilin/deskd --state open --json number,title,headRefName" }
{ source: "shell:git -C /path/to/deskd log --oneline -10" }
{ source: "shell:git -C /path/to/deskd branch --list" }
{ source: "shell:cat /home/dev/.deskd/reminders/*.json" }

// HTTP endpoints
{ source: "http:https://api.github.com/repos/kgatilin/deskd/actions/runs?per_page=5" }
{ source: "http:https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFF" }

// File watch — re-read on change
{ source: "file:/path/to/deskd/.archlint.yaml" }
{ source: "file:/home/dev/deskd.yaml" }
```

Shell sources are the most powerful — anything you can get via CLI, you can observe. The TUI just runs the command, captures stdout, parses as JSON (or raw text), and renders it.

### External Data in tui.yaml

```yaml
views:
  # Custom view: My Work Context
  my_context:
    title: "My Work"
    layout: vertical
    nodes:
      - id: open_issues
        source: "shell:gh issue list --repo kgatilin/deskd --state open --label agent-ready --json number,title,labels"
        poll: 60s
        render: table
        columns: [number, title, labels]

      - id: open_prs
        source: "shell:gh pr list --repo kgatilin/deskd --state open --json number,title,headRefName,reviewDecision"
        poll: 30s
        render: table
        columns: [number, title, headRefName, reviewDecision]

      - id: ci_status
        source: "shell:gh run list --repo kgatilin/deskd --limit 5 --json databaseId,conclusion,displayTitle,event"
        poll: 60s
        render: table
        columns: [displayTitle, conclusion, event]

      - id: dirty_worktrees
        source: "shell:git -C /path/to/deskd worktree list --porcelain"
        poll: 30s
        render: text

      - id: archlint_health
        source: "shell:cd /path/to/archlint && go run ./cmd/archlint check /path/to/deskd --format json"
        poll: 300s
        render: json
        highlight: [health, violations]

    edges:
      - from: open_issues -> open_prs (filter by linked PR)
      - from: open_prs -> ci_status (filter by branch)

  # Custom view: Trading
  trading:
    title: "Trading"
    nodes:
      - id: portfolio
        source: "shell:sc broker holdings --json"
        poll: 60s
        render: table

      - id: watchlist
        source: "shell:sc broker watchlist --json"
        poll: 300s
        render: table

      - id: fed_rate
        source: "http:https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFF&obs_start=2026-01-01"
        poll: 3600s
        render: text
```

### How It Works

```typescript
// The dashboard is defined as a graph, not hardcoded components:
const dashboardGraph: ObservabilityGraph = {
  nodes: [
    { id: "agents", source: "deskd:query/agent_list", type: "poll", interval: 5000,
      trigger: "event:agent_*" },
    { id: "tasks", source: "deskd:query/task_list", type: "poll", interval: 10000,
      trigger: "event:task_*" },
    { id: "workflows", source: "deskd:query/sm_list", type: "poll", interval: 10000,
      trigger: "event:transition_*" },
    { id: "bus", source: "bus:*", type: "stream" },
    { id: "cost", source: "deskd:query/usage_stats", type: "poll", interval: 30000 },
  ],
  edges: [
    { from: "agents", to: "tasks", on: "select", type: "filter" },
    { from: "tasks", to: "workflows", on: "smInstanceId", type: "navigate" },
    { from: "workflows", to: "tasks", on: "taskIds", type: "navigate" },
    { from: "agents", to: "bus", on: "select", type: "filter" },
  ]
}

// One hook drives all data:
function Dashboard() {
  const graph = useGraph(dashboardGraph)
  // graph.nodes["agents"].data -> AgentInfo[]
  // graph.nodes["tasks"].data -> TaskInfo[]
  // graph.select("agents", agentName) -> filters tasks and bus by agent
  // graph.navigate("tasks", taskId) -> switches to TaskDetail view
}
```

### useGraph() — The Single Hook

`useGraph(graph: ObservabilityGraph)` is the only data hook. It:

1. **Subscribes** to all node sources (bus subscriptions, poll intervals)
2. **Updates** node data when events arrive or polls complete
3. **Propagates** through edges — selecting an agent filters tasks, bus messages
4. **Navigates** — following an edge of type "navigate" switches the view
5. **Serializes** — the current graph state can be saved/loaded as `tui.yaml`

### Graph as Behavioral Spec

The graph is serializable to YAML:

```yaml
# tui.yaml — this file IS the behavioral specification
views:
  dashboard:
    nodes:
      - id: agents
        source: deskd:query/agent_list
        poll: 5s
        trigger: event:agent_*
      - id: tasks
        source: deskd:query/task_list
        poll: 10s
        trigger: event:task_*
      - id: bus
        source: bus:*
        type: stream
        buffer: 2000
    edges:
      - from: agents -> tasks (filter by assignee)
      - from: tasks -> workflows (navigate by smInstanceId)

  agent_detail:
    nodes:
      - id: agent
        source: deskd:query/agent_detail
        params: { name: "$selected" }
      - id: agent_tasks
        source: deskd:query/task_list
        params: { assignee: "$selected" }
      - id: agent_bus
        source: bus:agent:$selected
        type: stream
    # ...
```

This means:
- **Customizable**: user edits `tui.yaml` to change what they observe
- **Comparable**: the observability graph can be compared to the code dependency graph
- **Optimizable**: same algorithms (bisimulation, MDL) can compress both graphs
- **Versionable**: behavioral spec lives in git alongside the code

### Connection to Architecture Optimization

The observability graph and the code dependency graph form a **Galois connection**:

```
Observability Graph (what you watch)
        ↕ co-optimization
Code Dependency Graph (how it's built)
```

If an observable node has no corresponding code path → dead observation (remove widget).
If a code module has no observable output → unobserved code (maybe unnecessary, or missing observability).

The archlint graph optimizer (#132) can take both graphs as input and suggest:
- Code modules that are never observed → candidates for removal
- Observations that require unnecessarily complex code paths → candidates for abstraction
- The **minimum code graph** that satisfies the observability graph

## Process Architecture

```
deskd serve (Rust)          deskd-tui (TypeScript/Ink)
┌──────────────┐            ┌──────────────────────────┐
│ agents       │            │ ObservabilityGraph       │
│ workflow     │◄──bus.sock──│   ↓                     │
│ tasks        │            │ useGraph(graph)          │
│ schedules    │            │   ↓                     │
│ bus server   │            │ React views (rendering)  │
│ bus_api      │            │                          │
└──────────────┘            └──────────────────────────┘
```

### Bus Protocol

The TUI connects to deskd's Unix socket as a regular bus client:

```json
{"type":"register","name":"tui","subscriptions":["*"]}
```

For queries and mutations, TUI sends messages to specific targets and reads responses. The bus is the only communication channel — no HTTP, no gRPC, no shared filesystem reads.

### Process Lifecycle

1. `deskd tui` checks if `deskd serve` is running (bus socket exists)
2. Spawns `bun run` / `npx` the Ink app
3. Ink app connects to bus, registers as `tui`
4. On quit (`q`): Ink app disconnects, process exits. deskd keeps running.
5. On force quit (`Q`): sends shutdown signal to deskd, then exits.

### Project Structure

```
tui/
  package.json
  tsconfig.json
  src/
    index.tsx              -- entry point, arg parsing, bus connection
    app.tsx                -- root App component, view routing, global state
    bus.tsx                -- bus client: connect, send, receive, reconnect
    theme.ts               -- colors, styles, box characters

    graph/
      types.ts             -- ObservabilityGraph, ObservableNode, ObservableEdge
      useGraph.ts          -- the single hook: subscribe, update, propagate, navigate
      graphs.ts            -- predefined graphs for each view (dashboard, agent, task, etc.)
      serialize.ts         -- load/save graph as tui.yaml

    views/
      Dashboard.tsx        -- main overview (default view)
      AgentDetail.tsx      -- single agent deep dive
      TaskDetail.tsx       -- single task inspection
      WorkflowDetail.tsx   -- SM instance + ASCII diagram
      BusStream.tsx        -- live message tail with filters
      CostTracker.tsx      -- usage and budget dashboard
      TaskQueue.tsx        -- full task queue management
      Schedules.tsx        -- schedule list and management

    components/
      AgentList.tsx        -- agent table with status badges
      TaskList.tsx         -- task table with status colors
      WorkflowList.tsx     -- active workflow instances
      BusTail.tsx          -- last N bus messages
      SmDiagram.tsx        -- ASCII state machine renderer
      StatusBar.tsx        -- bottom bar: daily stats, key hints
      FilterInput.tsx      -- search/filter prompt
      ConfirmDialog.tsx    -- confirmation for destructive actions
      MessageComposer.tsx  -- compose and send message to agent
```

## Data Layer

All data flows through `useGraph()`. The individual type definitions below describe the shape of data at each node. Views don't call these hooks directly — they access `graph.nodes["agents"].data` etc.

### useAgents()

```typescript
interface AgentInfo {
  name: string
  status: 'ready' | 'busy' | 'unhealthy' | 'stopped'
  model: string
  currentTask?: string
  pid?: number
  turns: number
  costUsd: number
  budgetUsd: number
  uptime: string
  unixUser: string
  workDir: string
  sessionMode: string
}

// Returns:
{
  agents: AgentInfo[]
  refresh: () => void
}
```

**Data source:** Send `{"type":"message","target":"deskd:agent_list"}` to bus, parse response. Poll every 5s. Also update on bus events matching `agent:*`.

### useTasks()

```typescript
interface TaskInfo {
  id: string
  description: string
  status: 'pending' | 'active' | 'done' | 'failed' | 'cancelled' | 'dead_letter'
  assignee?: string
  model?: string
  labels: string[]
  createdAt: string
  updatedAt: string
  attempt: number
  maxAttempts: number
  costUsd: number
  turns: number
  smInstanceId?: string
}

// Returns:
{
  tasks: TaskInfo[]
  create: (desc: string, opts?: TaskCreateOpts) => void
  cancel: (id: string) => void
  refresh: () => void
}
```

**Data source:** `task_list` via bus. Update on `event:task_*` bus events.

### useWorkflows()

```typescript
interface WorkflowInfo {
  instanceId: string
  model: string
  title: string
  currentState: string
  assignee?: string
  costUsd: number
  transitions: TransitionInfo[]
  taskIds: string[]
  createdAt: string
}

// Returns:
{
  instances: WorkflowInfo[]
  models: ModelDef[]
  create: (model: string, title: string) => void
  move: (instanceId: string, toState: string, note?: string) => void
  cancel: (instanceId: string) => void
  refresh: () => void
}
```

**Data source:** `sm_query` via bus.

### useBus()

```typescript
interface BusMessage {
  timestamp: string
  source: string
  target: string
  messageId: string
  payload: string
  type: string
}

// Returns:
{
  messages: BusMessage[]     // ring buffer, last 2000
  send: (target: string, payload: string) => void
  filter: string
  setFilter: (f: string) => void
  filteredMessages: BusMessage[]
}
```

**Data source:** All bus messages received by TUI's `*` subscription. No polling — purely event-driven.

### useCost()

```typescript
interface CostStats {
  period: string
  totalCost: number
  totalTasks: number
  totalInputTokens: number
  totalOutputTokens: number
  byAgent: { name: string, cost: number, tasks: number, input: number, output: number }[]
  budgetUsed: number
  budgetTotal: number
}

// Returns:
{
  stats: CostStats
  period: 'today' | '7d' | '30d' | 'all'
  setPeriod: (p: string) => void
}
```

**Data source:** `usage_stats` via bus.

### useSchedules()

```typescript
interface ScheduleInfo {
  index: number
  cron: string
  action: string
  target: string
  nextFire: string
  description: string
}

// Returns:
{
  schedules: ScheduleInfo[]
  add: (cron: string, action: string, target: string) => void
  remove: (index: number) => void
}
```

**Data source:** `schedule_list` via bus.

### useInbox()

```typescript
interface InboxMessage {
  inbox: string
  from: string
  text: string
  timestamp: string
  source: string
}

// Returns:
{
  inboxes: { name: string, count: number }[]
  messages: InboxMessage[]
  selectedInbox: string
  selectInbox: (name: string) => void
  search: (query: string) => InboxMessage[]
}
```

**Data source:** `list_inboxes`, `read_inbox`, `search_inbox` via bus.

## Views

### View 1: Dashboard (default, key: `1`)

```
┌─ deskd ──────────────────────────────── 11 Apr 2026 14:32 ── $12.47 ─┐
│                                                                       │
│ ┌─ Agents ────────────────────┐ ┌─ Tasks ────────────────────────────┐│
│ │ ● dev      BUSY  opus-4-6  │ │ #142 ● review PR #89    → dev     ││
│ │   └─ task-142  12m  $2.30  │ │ #143 ○ fix CI            → sonnet  ││
│ │ ● reviewer READY sonnet-4  │ │ #138 ✓ update docs       → dev     ││
│ │ ● opus     READY opus-4-6  │ │ #140 ✗ refactor bus      → opus    ││
│ │ ● sonnet   READY sonnet-4  │ │ #141 ☠ parse cfg         → haiku   ││
│ │ ○ haiku    READY haiku-4.5 │ │                                     ││
│ └─────────────────────────────┘ └─────────────────────────────────────┘│
│ ┌─ Workflows ─────────────────┐ ┌─ Bus ──────────────────────────────┐│
│ │ pr-review #a3f              │ │ 14:32:05 dev → reviewer            ││
│ │   review → TESTING  $1.20  │ │   "review PR #89"                   ││
│ │ deploy #b71                 │ │ 14:31:58 reviewer → telegram.out   ││
│ │   build → DONE      $0.80  │ │   "merged #88, tagged v0.9.2"      ││
│ └─────────────────────────────┘ └─────────────────────────────────────┘│
│                                                                       │
│ 23 tasks │ $12.47 today │ 142K in 38K out │  ? help  1-8 views  / search│
└───────────────────────────────────────────────────────────────────────┘
```

**Panels:** 2×2 grid. Agents (top-left), Tasks (top-right), Workflows (bottom-left), Bus tail (bottom-right). Footer with daily stats and key hints.

**Interactions:**
- `Tab` — cycle focus between panels
- `Enter` on agent — go to AgentDetail
- `Enter` on task — go to TaskDetail
- `Enter` on workflow — go to WorkflowDetail
- `a` — quick-add task (opens TaskQueue with create prompt)
- `s` — send message to agent (opens MessageComposer)

### View 2: Agent Detail (key: `2`, or `Enter` on agent)

```
┌─ Agent: dev ─────────────────────────────────────── Esc back ────────┐
│ Status: BUSY (task-142)     Model: claude-opus-4-6                   │
│ Session: persistent          PID: 48291          Uptime: 4h 23m      │
│ Work dir: /home/dev          User: dev           Bus: bus.sock       │
│ Used: $12.47                                                         │
│ Capabilities: coding, review, delegation                             │
├──────────────────────────────────────────────────────────────────────┤
│ Current Task                                                         │
│ #142 "Review PR #89 in kgatilin/deskd"                               │
│ Status: active  Attempt: 1/3  Elapsed: 12m  Cost: $2.30  Turns: 8   │
├──────────────────────────────────────────────────────────────────────┤
│ Recent Tasks                                                j/k ↕    │
│ #138 ✓ update docs for v0.9          $0.45    3m   5 turns           │
│ #135 ✓ fix telegram adapter          $1.80   18m  14 turns           │
│ #131 ✗ refactor config parser        $0.90    8m   6 turns           │
│ #128 ✓ create archlint issues        $0.20    2m   3 turns           │
├──────────────────────────────────────────────────────────────────────┤
│ Agent Bus Messages                                          f follow │
│ 14:32:05 → reviewer  "review PR #89"                                │
│ 14:31:30 → opus      "analyze arch for TUI"                         │
│ 14:28:12 ← schedule  github_poll trigger                            │
│ 14:25:00 ← telegram  [Telegram: design TUI spec]                    │
├──────────────────────────────────────────────────────────────────────┤
│ s send message  k kill  p pause  l logs  e stderr         Esc back  │
└──────────────────────────────────────────────────────────────────────┘
```

**Interactions:**
- `s` — send message to this agent (opens MessageComposer)
- `k` — kill agent process (ConfirmDialog)
- `p` — pause/resume agent (stop dispatching tasks)
- `l` — show full task log (scrollable)
- `e` — show stderr tail (scrollable)
- `Enter` on task — go to TaskDetail
- `Esc` — back to Dashboard

### View 3: Task Detail (key: `3`, or `Enter` on task)

```
┌─ Task #142 ──────────────────────────────────────── Esc back ────────┐
│ Description: Review PR #89 in kgatilin/deskd                         │
│ Status: active         Assignee: dev                                 │
│ Created: 14:20:01      Updated: 14:32:05      Elapsed: 12m 04s      │
│ Created by: schedule   SM Instance: pr-review #a3f                   │
│ Attempt: 1/3           Retry after: —                                │
│ Cost: $2.30            Turns: 8                                      │
│ Model: claude-opus-4-6 Labels: [review, github]                      │
├──────────────────────────────────────────────────────────────────────┤
│ Metadata                                                             │
│ {"pr_number": 89, "repo": "kgatilin/deskd", "branch": "fix-bus"}    │
├──────────────────────────────────────────────────────────────────────┤
│ Result                                                               │
│ (task still active — no result yet)                                  │
├──────────────────────────────────────────────────────────────────────┤
│ Timeline                                                             │
│ 14:20:01  created (by schedule, via sm-a3f review→testing)           │
│ 14:20:02  dispatched to agent:dev                                    │
│ 14:20:03  agent:dev accepted                                         │
│ 14:32:05  ... (still running)                                        │
├──────────────────────────────────────────────────────────────────────┤
│ c cancel  w go to workflow                                 Esc back  │
└──────────────────────────────────────────────────────────────────────┘
```

**Interactions:**
- `c` — cancel task (ConfirmDialog)
- `w` — jump to linked workflow (View 4)
- `y` — copy result to clipboard
- `Esc` — back

### View 4: Workflow Detail (key: `4`, or `Enter` on workflow)

```
┌─ Workflow: pr-review  Instance: #a3f ────────────── Esc back ────────┐
│ Title: Review PR #89                                                 │
│ State: TESTING          Assignee: reviewer                           │
│ Created: 14:18:00       Cost: $3.50     Transitions: 4               │
├──────────────────────────────────────────────────────────────────────┤
│ State Machine                                                        │
│                                                                      │
│  [new] ──auto──▶ [review] ──pass──▶ [TESTING] ──pass──▶ [merge]     │
│                     │                   ▲                   │        │
│                     └──fail──▶ [fix] ───┘               [[done]]    │
│                                                                      │
│  Current: TESTING (step: check, cmd: "cargo test")                   │
│  Timeout: 10m → fix                                                  │
├──────────────────────────────────────────────────────────────────────┤
│ Transition History                                                   │
│ 14:18:00  new → review       auto          $0.00  task-140           │
│ 14:18:02  review → testing   pass (dev)    $1.80  task-141           │
│ 14:28:15  testing → fix      fail (check)  $0.00  —                  │
│ 14:29:00  fix → testing      retry (dev)   $1.70  task-142           │
├──────────────────────────────────────────────────────────────────────┤
│ Owned Tasks                                                          │
│ #140 ✓  #141 ✓  #142 ●                                              │
├──────────────────────────────────────────────────────────────────────┤
│ m manual move  x cancel workflow  Enter on task → detail   Esc back │
└──────────────────────────────────────────────────────────────────────┘
```

**Interactions:**
- `m` — manual move: select target state from allowed transitions
- `x` — cancel workflow instance (ConfirmDialog)
- `Enter` on task — go to TaskDetail
- `Esc` — back

### View 5: Bus Stream (key: `5`)

```
┌─ Bus Stream ──────────────────── filter: source:dev ── 847 msgs ─────┐
│                                                                       │
│ 14:32:05.123  agent:dev → agent:reviewer                             │
│   type: message  id: msg-8a3f                                        │
│   payload: {"task": "review PR #89 in kgatilin/deskd"}               │
│                                                                       │
│ 14:31:58.456  agent:reviewer → telegram.out:-1001234567890           │
│   type: message  id: msg-7b2e                                        │
│   payload: {"text": "merged #88, tagged v0.9.2"}                     │
│                                                                       │
│ 14:31:42.789  schedule → agent:reviewer                              │
│   type: message  id: msg-6c1d                                        │
│   payload: {"action": "github_poll"}                                 │
│                                                                       │
│ 14:31:30.012  agent:dev → agent:opus                                 │
│   type: message  id: msg-5d0c                                        │
│   payload: {"task": "analyze architecture for TUI"}                  │
│                                                                       │
├───────────────────────────────────────────────────────────────────────┤
│ / filter  f follow  c compact  y copy  Enter expand/collapse  Esc    │
└───────────────────────────────────────────────────────────────────────┘
```

**Filter syntax** (type `/` then enter filter):
- `source:dev` — messages from agent dev
- `target:telegram*` — messages to any telegram target
- `type:event` — only domain events
- `pr` — freetext search in payload
- Combine: `source:dev target:reviewer`

**Interactions:**
- `/` — open filter prompt
- `f` — toggle follow (auto-scroll)
- `c` — toggle compact mode (1 line vs expanded)
- `Enter` — expand/collapse selected message
- `y` — copy message JSON to clipboard
- `Esc` — back to Dashboard

### View 6: Cost Tracker (key: `6`)

```
┌─ Cost Tracker ─────────────────────── Period: [today] 7d  30d  all ──┐
│                                                                       │
│ Agent       Tasks    Cost    Input     Output    Duration              │
│ ─────────────────────────────────────────────────────────────────      │
│ dev            8   $4.30    42.1K     12.3K       48m                 │
│ reviewer       6   $3.20    28.5K      8.1K       35m                 │
│ opus           3   $3.80    51.2K     15.4K       22m                 │
│ sonnet         4   $0.95    18.3K      5.2K       15m                 │
│ haiku          2   $0.22     2.1K      0.8K        3m                 │
│ ─────────────────────────────────────────────────────────────────      │
│ TOTAL         23  $12.47   142.2K     41.8K      123m                 │
│                                                                       │
│ Avg per task: 6.2K input, 1.8K output, $0.54, 5.3m                   │
├───────────────────────────────────────────────────────────────────────┤
│ Cost Timeline (hourly)                                                │
│  $4 │          ██                                                     │
│  $3 │       ██ ██                                                     │
│  $2 │    ██ ██ ██ ██                                                  │
│  $1 │ ██ ██ ██ ██ ██                                                  │
│  $0 ┼──┼──┼──┼──┼──┼──┼──┼──                                         │
│     08 09 10 11 12 13 14                                              │
├───────────────────────────────────────────────────────────────────────┤
│ t cycle period  Enter agent detail                         Esc back  │
└───────────────────────────────────────────────────────────────────────┘
```

**Interactions:**
- `t` — cycle period: today → 7d → 30d → all
- `Enter` on agent row — go to AgentDetail
- `Esc` — back

### View 7: Task Queue (key: `7`)

Full task queue management — create, filter, cancel.

```
┌─ Task Queue ──────────── filter: [all] pending active done failed ───┐
│                                                                       │
│ ID    Status       Description                  Assignee  Cost  Age   │
│ ─────────────────────────────────────────────────────────────────      │
│ #143  ○ pending    fix CI pipeline              —         —     2m    │
│ #142  ● active     review PR #89               dev       $2.30 12m   │
│ #141  ☠ dead_letter parse config edge cases     haiku     $0.10 45m   │
│ #140  ✗ failed     refactor bus error handling  opus      $1.20 1h    │
│ #139  ✗ failed     update archlint config       sonnet    $0.30 1h    │
│ #138  ✓ done       update docs for v0.9         dev       $0.45 2h    │
│ #137  ✓ done       fix telegram adapter         dev       $1.80 3h    │
│                                                                       │
├───────────────────────────────────────────────────────────────────────┤
│ a add task  c cancel  Enter detail  1-5 filter status      Esc back  │
└───────────────────────────────────────────────────────────────────────┘
```

**Interactions:**
- `a` — add task: opens input prompt for description, optional model/labels
- `c` — cancel selected task (ConfirmDialog)
- `1`-`5` in this view — filter by status (all/pending/active/done/failed)
- `Enter` — go to TaskDetail
- `Esc` — back

### View 8: Schedules (key: `8`)

```
┌─ Schedules ──────────────────────────────────────────────────────────┐
│                                                                       │
│ #  Cron              Action         Target        Next Fire           │
│ ──────────────────────────────────────────────────────────────────     │
│ 0  */30 * * * *      github_poll    reviewer      14:30:00            │
│ 1  0 9 * * 1-5       raw            dev           Mon 09:00           │
│ 2  0 0 * * *         raw            reviewer      00:00:00            │
│                                                                       │
├───────────────────────────────────────────────────────────────────────┤
│ Reminders                                                             │
│ (none active)                                                         │
├───────────────────────────────────────────────────────────────────────┤
│ a add schedule  d delete  r add reminder                   Esc back  │
└───────────────────────────────────────────────────────────────────────┘
```

**Interactions:**
- `a` — add schedule (prompts for cron, action, target)
- `d` — delete selected schedule (ConfirmDialog)
- `r` — add reminder (prompts for delay/time, target, message)
- `Esc` — back

## Custom Views

Beyond the 8 built-in views, users define custom views in `tui.yaml`. Each custom view is a set of nodes (data sources) and edges (relationships), with a layout and render hints.

Custom views appear after the built-in views. Navigation: `1`-`8` for built-in, then `F1`-`F12` for custom views, or `/` to search by name.

```yaml
# tui.yaml
custom_views:
  - name: work
    title: "My Work"
    key: F1
    nodes:
      - id: issues
        source: "shell:gh issue list --repo kgatilin/deskd --state open --json number,title,labels"
        poll: 60s
        render: table
        columns: [number, title, labels]
      - id: prs
        source: "shell:gh pr list --state open --json number,title,headRefName"
        poll: 30s
        render: table
      - id: ci
        source: "shell:gh run list --limit 5 --json displayTitle,conclusion"
        poll: 60s
        render: table
    layout: vertical   # stack panels vertically

  - name: trading
    title: "Trading"
    key: F2
    nodes:
      - id: portfolio
        source: "shell:sc broker holdings --json"
        poll: 60s
        render: table
      - id: alerts
        source: "shell:sc broker price-alerts --json"
        poll: 300s
        render: table
```

Custom views have the same navigation as built-in views: `j/k` scroll, `Enter` expand, `Esc` back. Shell sources run with the deskd user's permissions.

## Global Navigation

| Key | Action |
|-----|--------|
| `1`-`8` | Switch to built-in view 1-8 |
| `F1`-`F12` | Switch to custom view (defined in tui.yaml) |
| `Tab` | Cycle focus between panels |
| `j` / `k` | Move selection down/up in lists |
| `g` / `G` | Jump to top/bottom |
| `Enter` | Drill into selected item |
| `Esc` | Back to previous view / cancel |
| `/` | Open filter/search prompt |
| `?` | Toggle help overlay |
| `q` | Quit TUI (deskd keeps running) |
| `Q` | Quit deskd entirely |
| `r` | Force refresh all data |

## Bus Communication Protocol

The TUI communicates with deskd exclusively through the bus. All operations map to bus messages.

### Query Pattern

```typescript
// TUI sends:
{ type: "message", target: "deskd:query", payload: {
  method: "agent_list",  // or: task_list, sm_query, usage_stats, schedule_list, ...
  params: { /* method-specific params */ },
  reply_to: "tui"
}}

// deskd responds:
{ type: "message", target: "tui", payload: {
  method: "agent_list",
  result: [ /* data */ ]
}}
```

### Mutation Pattern

```typescript
// TUI sends:
{ type: "message", target: "deskd:command", payload: {
  method: "task_cancel",  // or: sm_move, send_message, schedule_add, ...
  params: { id: "task-142" },
  reply_to: "tui"
}}

// deskd responds:
{ type: "message", target: "tui", payload: {
  method: "task_cancel",
  result: { success: true }
}}
```

### Required Bus API Additions in deskd

Currently the bus is message-passing only. For the TUI to query state, deskd needs a **query handler** on the bus that routes requests to the right store/service. This is a new component:

```rust
// src/app/bus_api.rs — handles query/command messages from bus clients like TUI
//
// Subscribes to: "deskd:query", "deskd:command"
// Routes to: TaskStore, StateMachineStore, AgentRegistry, UsageStats, ScheduleManager
```

**Methods to implement:**

| Method | Type | Maps to |
|--------|------|---------|
| `agent_list` | query | `agent_registry::list()` + `agent_status` |
| `agent_detail` | query | `agent_registry::load_state()` + process info |
| `agent_kill` | mutation | kill agent process |
| `agent_pause` | mutation | pause task dispatching |
| `task_list` | query | `TaskStore::list()` |
| `task_detail` | query | `TaskStore::get()` |
| `task_create` | mutation | `TaskStore::push()` |
| `task_cancel` | mutation | `TaskStore::cancel()` |
| `sm_list` | query | `StateMachineStore::list_instances()` |
| `sm_detail` | query | `StateMachineStore::get_instance()` |
| `sm_models` | query | loaded model definitions |
| `sm_create` | mutation | `StateMachineStore::create_instance()` |
| `sm_move` | mutation | workflow `handle_move_notification()` |
| `sm_cancel` | mutation | move to terminal state |
| `usage_stats` | query | `compute_stats()` |
| `schedule_list` | query | `ScheduleManager::list()` |
| `schedule_add` | mutation | `ScheduleManager::add()` |
| `schedule_remove` | mutation | `ScheduleManager::remove()` |
| `send_message` | mutation | bus publish to target |
| `inbox_list` | query | `list_inboxes()` |
| `inbox_read` | query | `read_inbox()` |
| `inbox_search` | query | `search_inbox()` |
| `bus_status` | query | connected clients list |

## Implementation Roadmap

### Step 1: Bus API in deskd (Rust)

Add `src/app/bus_api.rs` — a bus client that subscribes to `deskd:query` and `deskd:command`, routes to existing stores/services, responds to the sender.

This is the foundation. Without it, TUI can only watch bus messages but not query state.

**Acceptance criteria:**
- `bus_api` starts as part of `deskd serve`
- All 22 methods in the table above work
- Test: send a query message via `deskd agent send` to `deskd:query`, get valid JSON response
- No changes to existing CLI or MCP interfaces

### Step 2: TUI Scaffold (TypeScript/Ink)

Set up the project:
- `tui/` directory with package.json, tsconfig, Ink deps
- `bus.ts` — Unix socket client, connect/register/send/receive
- `app.tsx` — root component with view switching (1-8)
- Empty view components (just titles)
- `deskd tui` CLI command in Rust spawns the Bun process

**Acceptance criteria:**
- `deskd tui` launches alternate screen
- View switching with 1-8 works
- Bus connection established, raw messages visible in console
- `q` exits cleanly

### Step 3: Dashboard + Bus Stream

- `hooks/useAgents.ts`, `hooks/useBus.ts`, `hooks/useTasks.ts`, `hooks/useWorkflows.ts`
- `views/Dashboard.tsx` — 2×2 grid, all four panels, live updates
- `views/BusStream.tsx` — live tail, follow mode, filter syntax
- `components/StatusBar.tsx` — daily stats footer

**Acceptance criteria:**
- Dashboard shows real agent status, tasks, workflows from running deskd
- Bus Stream shows live messages in real time
- Filter syntax works (source:X, target:X, freetext)
- Follow mode auto-scrolls

### Step 4: Detail Views

- `views/AgentDetail.tsx` — full agent info, task history, filtered bus, send message
- `views/TaskDetail.tsx` — full task info, timeline, cancel
- `views/WorkflowDetail.tsx` — ASCII SM diagram, transition history, manual move

**Acceptance criteria:**
- `Enter` on any item in Dashboard drills into detail
- `Esc` goes back
- Send message to agent works
- Cancel task works
- Manual SM move works (selects from available transitions)

### Step 5: Management Views

- `views/CostTracker.tsx` — per-agent table, hourly chart, budget bar
- `views/TaskQueue.tsx` — full list, create task, cancel, filter by status
- `views/Schedules.tsx` — list, add, remove schedules and reminders

**Acceptance criteria:**
- Cost tracker shows real usage data, period switching works
- Task queue allows creating and cancelling tasks
- Schedules view shows cron schedules, allows add/remove

### Step 6: Remove ratatui TUI

- Remove `src/app/tui.rs`
- Remove ratatui/crossterm from Cargo.toml
- Remove `--features tui` flag
- `deskd tui` now always launches the Ink version

## Dependencies

### TypeScript (tui/package.json)

```json
{
  "name": "deskd-tui",
  "version": "0.1.0",
  "type": "module",
  "dependencies": {
    "ink": "^5.2",
    "react": "^18.3",
    "ink-text-input": "^6.0",
    "ink-select-input": "^6.0",
    "ink-spinner": "^5.0",
    "ink-table": "^4.0",
    "cli-boxes": "^4.0",
    "date-fns": "^4.1"
  },
  "devDependencies": {
    "typescript": "^5.7",
    "@types/react": "^18.3",
    "tsx": "^4.19"
  }
}
```

### Rust additions

```toml
# No new Rust dependencies for TUI itself.
# bus_api.rs uses existing crate dependencies.
# The `deskd tui` command just spawns a child process.
```

## Open Questions

1. **Bun vs Node:** Bun is faster and has better TypeScript support. But Node is more universally available. Recommendation: support both, prefer Bun if available.

2. **Agent stdout streaming:** Should TUI show live agent output (token by token)? High bandwidth but useful for debugging. Defer to after Step 5.

3. **Reconnection:** If deskd restarts, TUI should detect and reconnect. The bus client should have exponential backoff retry.

4. **Multi-workspace:** Currently single deskd instance. If multiple workspaces exist, TUI should let you pick which one to connect to.
