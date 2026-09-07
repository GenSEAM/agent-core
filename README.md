# @genseam/asl-agent-core

Autonomous AgentScript execution core, onion middleware pipeline, topological DAG dispatcher, and algebraic finite state machine engine in pure AgentScript (ASL).

## Overview

`@genseam/asl-agent-core` provides the foundational runtime primitives for autonomous agents operating in native AgentScript environments. It features zero foreign runtime dependencies, strict structural typing, deterministic algebraic state transitions, and an extensible onion middleware pipeline for intercepting and auditing tool calls.

## Architecture & Modules

### 1. Agent Execution Core (`asl-agent-core/core`)
Defines structured records for agent lifecycle management, prompt framing, tool registration, and event dispatching:
- **`AgentMessage` & `AgentContext`**: Strongly-typed message transcripts, role definitions (`system`, `user`, `assistant`, `tool`), and execution state.
- **`ToolDef`, `ToolCall` & `ToolResult`**: Formal tool registry and invocation framing with parameter specs.
- **`EventBus`**: In-memory pub/sub event bus supporting topic-based agent telemetry and audit loops.

### 2. Composable Onion Middleware (`asl-agent-core/onion`)
Provides an onion-layer interceptor architecture with topological dependency sorting:
- **Middleware Kinds**: `kind-pre-call`, `kind-post-call`, `kind-filter`, `kind-mutate`, and `kind-audit`.
- **Topological Sorting**: Resolves `before` and `after` ordering constraints among registered middlewares.
- **Execution Pipeline**: `dispatch-tool-call` runs interceptors in sequence, preserving an immutable audit log and yielding an `OnionDecision`.

### 3. Algebraic Finite State Machine (`asl-agent-core/fsm`)
A closed algebraic state machine modeling autonomous agent progression:
- **States (`AgentState`)**: `idle`, `planning`, `coding`, `reviewing`, `success`, `failed`.
- **Events (`AgentEvent`)**: `start`, `plan-ready`, `code-ready`, `review-pass`, `review-fail`, `reset`.
- **Transitions**: Pure function `(step state event) -> AgentState` guaranteeing deterministic transitions and explicit terminal state verification via `is-terminal-state`.

### 4. Topological DAG Engine (`asl-agent-core/dag`)
Directed acyclic graph scheduler for multi-step agent reasoning and task decomposition:
- Dependency resolution, cycle detection, and parallel execution wave extraction.

## Usage

### Finite State Machine
```scheme
(import (asl-agent-core/fsm :a fsm))

(let [(s0 (fsm/idle))
      (s1 (fsm/step s0 (fsm/start)))        ;; -> (planning)
      (s2 (fsm/step s1 (fsm/plan-ready)))   ;; -> (coding)
      (s3 (fsm/step s2 (fsm/code-ready)))   ;; -> (reviewing)
      (s4 (fsm/step s3 (fsm/review-pass)))] ;; -> (success)
  (fsm/is-terminal-state s4)) ;; => true
```

### Onion Middleware Pipeline
```scheme
(import (asl-agent-core/onion :a onion))

(let [(mw-auth (onion/make-middleware "auth" "Authentication" (onion/kind-filter) 10 [] []))
      (mw-log  (onion/make-middleware "log"  "Logger"         (onion/kind-audit)  20 [] []))
      (pipe    (onion/make-pipeline (list mw-auth mw-log)))
      (ctx     (onion/make-onion-context "call-1" "agent-alpha" "file-read" "{\"path\":\"main.asl\"}"))
      (dec     (onion/dispatch-tool-call pipe ctx))]
  (onion/proceed? dec))
```
