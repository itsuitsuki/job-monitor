#!/usr/bin/env python3
"""Dependency-free MCP stdio server for Job Monitor."""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any, Callable

from command_monitor_lib import (
    CommandMonitorError,
    get_command_monitor,
    list_command_monitors,
    start_command_monitor,
    stop_command_monitor,
)
from job_monitor_lib import (
    MonitorError,
    discover_hosts,
    list_events,
    list_tasks,
    monitor_status,
    poll_once,
    remove_task,
    snapshot_tasks,
    start_monitor,
    stop_monitor,
    upsert_host,
    upsert_task,
)


TOOLS = [
    {
        "name": "monitor",
        "description": (
            "Start a persistent background watcher for a read-only one-shot shell probe. "
            "The worker reruns the command, deduplicates normalized state, and sends a "
            "Paseo event to the originating agent only when state changes. It can exit "
            "automatically when terminal_regex matches."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "Short human-readable purpose of this monitor.",
                },
                "command": {
                    "type": "string",
                    "description": (
                        "One-shot read-only probe executed with bash -lc on each poll. "
                        "Do not include a polling loop; the worker owns the loop."
                    ),
                },
                "interval_seconds": {
                    "type": "number",
                    "minimum": 1,
                    "maximum": 86400,
                    "default": 30,
                },
                "probe_timeout_seconds": {
                    "type": "number",
                    "minimum": 1,
                    "maximum": 3600,
                    "default": 30,
                },
                "extract_regex": {
                    "type": "string",
                    "description": (
                        "Optional regex selecting the state from command output. "
                        "When it has exactly one capture group, that group becomes the state."
                    ),
                },
                "terminal_regex": {
                    "type": "string",
                    "description": "Optional regex identifying a completed terminal state.",
                },
                "error_regex": {
                    "type": "string",
                    "description": "Optional regex identifying an error state.",
                },
                "emit_initial": {
                    "type": "boolean",
                    "default": False,
                    "description": "Notify for the first observed non-error, non-terminal state.",
                },
                "exit_on_terminal": {
                    "type": "boolean",
                    "default": True,
                    "description": "Exit after a terminal state is observed and one notification attempt is recorded.",
                },
                "exit_on_error": {
                    "type": "boolean",
                    "default": False,
                    "description": "Exit after an error state is observed and one notification attempt is recorded.",
                },
                "max_runtime_seconds": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 31536000,
                    "default": 0,
                    "description": "Overall watcher lifetime; zero means unlimited.",
                },
                "cwd": {
                    "type": "string",
                    "description": (
                        "Probe working directory. Defaults to PASEO_AGENT_CWD or the "
                        "MCP server working directory."
                    ),
                },
                "agent_id": {
                    "type": "string",
                    "description": "Paseo agent to notify. Defaults to PASEO_AGENT_ID.",
                },
            },
            "required": ["description", "command"],
            "additionalProperties": False,
        },
    },
    {
        "name": "monitor_list",
        "description": "List persistent command monitors across MCP server and Codex restarts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "active_only": {"type": "boolean", "default": False},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "monitor_get",
        "description": (
            "Get one command monitor's specification, lifecycle, latest state, recent "
            "events, and worker log tail."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "monitor_id": {"type": "string"},
                "event_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 1000,
                    "default": 20,
                },
            },
            "required": ["monitor_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "monitor_stop",
        "description": (
            "Stop exactly one command monitor worker. This never stops or alters the "
            "job being observed."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"monitor_id": {"type": "string"}},
            "required": ["monitor_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "list_tasks",
        "description": "List every registered project, host, task, and probe configuration.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "upsert_host",
        "description": "Add or update a local or direct SSH host used by monitored tasks.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "host_id": {"type": "string"},
                "host": {
                    "type": "object",
                    "description": "Host object. Use {kind: local} or {kind: ssh, target: SSH_ALIAS}.",
                    "additionalProperties": True,
                },
            },
            "required": ["host_id", "host"],
            "additionalProperties": False,
        },
    },
    {
        "name": "upsert_task",
        "description": "Register or update a task. Probes support tmux globs, process regexes, logs, artifacts, GPU indices, and Slurm job IDs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "object",
                    "description": "Task with id, project, host, enabled, and probes fields.",
                    "additionalProperties": True,
                }
            },
            "required": ["task"],
            "additionalProperties": False,
        },
    },
    {
        "name": "remove_task",
        "description": "Remove exactly one registered task by ID. This does not stop or alter the underlying job.",
        "inputSchema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "snapshot",
        "description": "Inspect selected registered tasks now, or all enabled tasks when task_ids is omitted.",
        "inputSchema": {
            "type": "object",
            "properties": {"task_ids": {"type": "array", "items": {"type": "string"}}},
            "additionalProperties": False,
        },
    },
    {
        "name": "discover_running",
        "description": "Discover tmux sessions, common workload processes, and GPU compute processes on configured hosts.",
        "inputSchema": {
            "type": "object",
            "properties": {"host_ids": {"type": "array", "items": {"type": "string"}}},
            "additionalProperties": False,
        },
    },
    {
        "name": "poll_once",
        "description": "Poll all enabled tasks now, save the latest snapshot, and record status changes.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "start_monitor",
        "description": "Start the persistent background polling daemon. It records events but cannot wake a Codex conversation.",
        "inputSchema": {
            "type": "object",
            "properties": {"interval_seconds": {"type": "integer", "minimum": 10, "maximum": 86400}},
            "additionalProperties": False,
        },
    },
    {
        "name": "stop_monitor",
        "description": "Stop only the Job Monitor daemon. It never stops monitored jobs.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "monitor_status",
        "description": "Report whether the polling daemon is active and when it last checked tasks.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "list_events",
        "description": "Read recent task state changes and monitor errors.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}},
            "additionalProperties": False,
        },
    },
]


def call_tool(name: str, arguments: dict[str, Any]) -> Any:
    handlers: dict[str, Callable[[], Any]] = {
        "monitor": lambda: start_command_monitor(
            description=arguments["description"],
            command=arguments["command"],
            interval_seconds=arguments.get("interval_seconds", 30),
            probe_timeout_seconds=arguments.get("probe_timeout_seconds", 30),
            extract_regex=arguments.get("extract_regex"),
            terminal_regex=arguments.get("terminal_regex"),
            error_regex=arguments.get("error_regex"),
            emit_initial=arguments.get("emit_initial", False),
            exit_on_terminal=arguments.get("exit_on_terminal", True),
            exit_on_error=arguments.get("exit_on_error", False),
            max_runtime_seconds=arguments.get("max_runtime_seconds", 0),
            cwd=arguments.get("cwd"),
            agent_id=arguments.get("agent_id"),
        ),
        "monitor_list": lambda: list_command_monitors(
            arguments.get("active_only", False)
        ),
        "monitor_get": lambda: get_command_monitor(
            arguments["monitor_id"],
            arguments.get("event_limit", 20),
        ),
        "monitor_stop": lambda: stop_command_monitor(arguments["monitor_id"]),
        "list_tasks": list_tasks,
        "upsert_host": lambda: upsert_host(arguments["host_id"], arguments["host"]),
        "upsert_task": lambda: upsert_task(arguments["task"]),
        "remove_task": lambda: remove_task(arguments["task_id"]),
        "snapshot": lambda: snapshot_tasks(arguments.get("task_ids")),
        "discover_running": lambda: discover_hosts(arguments.get("host_ids")),
        "poll_once": poll_once,
        "start_monitor": lambda: start_monitor(arguments.get("interval_seconds")),
        "stop_monitor": stop_monitor,
        "monitor_status": monitor_status,
        "list_events": lambda: list_events(arguments.get("limit", 100)),
    }
    if name not in handlers:
        raise MonitorError(f"Unknown tool: {name}")
    return handlers[name]()


def send(message: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def success(request_id: Any, result: Any) -> None:
    send({"jsonrpc": "2.0", "id": request_id, "result": result})


def error(request_id: Any, code: int, message: str) -> None:
    send({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}})


def main() -> int:
    for raw_line in sys.stdin:
        if not raw_line.strip():
            continue
        request_id = None
        try:
            request = json.loads(raw_line)
            request_id = request.get("id")
            method = request.get("method")
            params = request.get("params") or {}
            if method == "initialize":
                success(
                    request_id,
                    {
                        "protocolVersion": params.get("protocolVersion", "2024-11-05"),
                        "capabilities": {"tools": {"listChanged": False}},
                        "serverInfo": {"name": "job-monitor", "version": "0.1.2"},
                    },
                )
            elif method == "ping":
                success(request_id, {})
            elif method == "tools/list":
                success(request_id, {"tools": TOOLS})
            elif method == "tools/call":
                result = call_tool(params.get("name", ""), params.get("arguments") or {})
                success(
                    request_id,
                    {
                        "content": [{"type": "text", "text": json.dumps(result, indent=2, sort_keys=True)}],
                        "structuredContent": result,
                        "isError": False,
                    },
                )
            elif request_id is not None:
                error(request_id, -32601, f"Method not found: {method}")
        except (
            CommandMonitorError,
            MonitorError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            if request_id is not None:
                success(
                    request_id,
                    {
                        "content": [{"type": "text", "text": str(exc)}],
                        "isError": True,
                    },
                )
        except Exception as exc:
            print(traceback.format_exc(), file=sys.stderr, flush=True)
            if request_id is not None:
                error(request_id, -32603, f"Internal error: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
