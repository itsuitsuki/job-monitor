#!/usr/bin/env python3
"""Core library for the generic job monitor plugin."""

from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_REGISTRY_PATH = Path("~/.config/job-monitor/registry.json").expanduser()
DEFAULT_STATE_DIR = Path("~/.local/state/job-monitor").expanduser()
WORKLOAD_EXECUTABLES = {
    "accelerate",
    "deepspeed",
    "julia",
    "mpirun",
    "python",
    "python3",
    "ray",
    "raylet",
    "srun",
    "torchrun",
    "vllm",
}
FAILED_SLURM_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "TIMEOUT",
}
RUNNING_SLURM_STATES = {
    "COMPLETING",
    "CONFIGURING",
    "PENDING",
    "RUNNING",
    "SUSPENDED",
}


class MonitorError(RuntimeError):
    """Raised for invalid registry data or failed monitor operations."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def registry_path() -> Path:
    return Path(os.environ.get("JOB_MONITOR_REGISTRY", DEFAULT_REGISTRY_PATH)).expanduser()


def state_dir() -> Path:
    return Path(os.environ.get("JOB_MONITOR_STATE_DIR", DEFAULT_STATE_DIR)).expanduser()


def default_registry() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "poll_interval_seconds": DEFAULT_INTERVAL_SECONDS,
        "hosts": {"local": {"kind": "local"}},
        "tasks": [],
        "notifications": {"desktop": False},
    }


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def load_registry(create: bool = True) -> dict[str, Any]:
    path = registry_path()
    if not path.exists():
        value = default_registry()
        if create:
            _atomic_write_json(path, value)
        return value
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MonitorError(f"Cannot read registry {path}: {exc}") from exc
    validate_registry(value)
    return value


def save_registry(value: dict[str, Any]) -> None:
    validate_registry(value)
    _atomic_write_json(registry_path(), value)


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MonitorError(f"{label} must be a non-empty string")
    return value


def _require_string_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise MonitorError(f"{label} must be a list of non-empty strings")
    return value


def validate_host(host_id: str, host: Any) -> None:
    _require_string(host_id, "host id")
    if not isinstance(host, dict):
        raise MonitorError(f"host {host_id} must be an object")
    kind = host.get("kind")
    if kind not in {"local", "ssh"}:
        raise MonitorError(f"host {host_id} kind must be local or ssh")
    if kind == "ssh":
        _require_string(host.get("target"), f"host {host_id}.target")
        options = _require_string_list(host.get("options", []), f"host {host_id}.options")
        if any("\n" in option or "\x00" in option for option in options):
            raise MonitorError(f"host {host_id}.options contains an invalid character")
    timeout = host.get("timeout_seconds", 15)
    if not isinstance(timeout, (int, float)) or not 1 <= timeout <= 300:
        raise MonitorError(f"host {host_id}.timeout_seconds must be between 1 and 300")


def validate_task(task: Any, hosts: dict[str, Any]) -> None:
    if not isinstance(task, dict):
        raise MonitorError("task must be an object")
    task_id = _require_string(task.get("id"), "task.id")
    _require_string(task.get("project"), f"task {task_id}.project")
    host_id = _require_string(task.get("host", "local"), f"task {task_id}.host")
    if host_id not in hosts:
        raise MonitorError(f"task {task_id} refers to unknown host {host_id}")
    if not isinstance(task.get("enabled", True), bool):
        raise MonitorError(f"task {task_id}.enabled must be a boolean")
    probes = task.get("probes", {})
    if not isinstance(probes, dict):
        raise MonitorError(f"task {task_id}.probes must be an object")
    _require_string_list(probes.get("tmux", []), f"task {task_id}.probes.tmux")
    _require_string_list(probes.get("process", []), f"task {task_id}.probes.process")
    _require_string_list(probes.get("slurm", []), f"task {task_id}.probes.slurm")
    gpu_ids = probes.get("gpu", [])
    if not isinstance(gpu_ids, list) or not all(isinstance(item, int) and item >= 0 for item in gpu_ids):
        raise MonitorError(f"task {task_id}.probes.gpu must be a list of non-negative integers")
    for probe_name in ("logs", "artifacts"):
        entries = probes.get(probe_name, [])
        if not isinstance(entries, list) or not all(isinstance(item, dict) for item in entries):
            raise MonitorError(f"task {task_id}.probes.{probe_name} must be a list of objects")
        for index, entry in enumerate(entries):
            _require_string(entry.get("path"), f"task {task_id}.probes.{probe_name}[{index}].path")
    for index, entry in enumerate(probes.get("logs", [])):
        _require_string_list(entry.get("error_patterns", []), f"task {task_id}.logs[{index}].error_patterns")
        _require_string_list(entry.get("success_patterns", []), f"task {task_id}.logs[{index}].success_patterns")
        tail_lines = entry.get("tail_lines", 200)
        if not isinstance(tail_lines, int) or not 1 <= tail_lines <= 5000:
            raise MonitorError(f"task {task_id}.logs[{index}].tail_lines must be between 1 and 5000")


def validate_registry(value: Any) -> None:
    if not isinstance(value, dict):
        raise MonitorError("registry must be an object")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise MonitorError(f"registry schema_version must be {SCHEMA_VERSION}")
    interval = value.get("poll_interval_seconds", DEFAULT_INTERVAL_SECONDS)
    if not isinstance(interval, int) or not 10 <= interval <= 86400:
        raise MonitorError("poll_interval_seconds must be between 10 and 86400")
    hosts = value.get("hosts")
    if not isinstance(hosts, dict) or not hosts:
        raise MonitorError("registry.hosts must be a non-empty object")
    for host_id, host in hosts.items():
        validate_host(host_id, host)
    tasks = value.get("tasks")
    if not isinstance(tasks, list):
        raise MonitorError("registry.tasks must be a list")
    seen: set[str] = set()
    for task in tasks:
        validate_task(task, hosts)
        if task["id"] in seen:
            raise MonitorError(f"duplicate task id {task['id']}")
        seen.add(task["id"])
    notifications = value.get("notifications", {})
    if not isinstance(notifications, dict) or not isinstance(notifications.get("desktop", False), bool):
        raise MonitorError("registry.notifications.desktop must be a boolean")


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class HostRunner:
    def __init__(self, host_id: str, host: dict[str, Any]):
        self.host_id = host_id
        self.host = host

    def run(self, args: list[str], timeout_seconds: float | None = None) -> CommandResult:
        timeout = timeout_seconds or float(self.host.get("timeout_seconds", 15))
        if self.host["kind"] == "local":
            command = args
        else:
            command = [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={max(1, int(timeout))}",
                *self.host.get("options", []),
                self.host["target"],
                shlex.join(args),
            ]
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return CommandResult(completed.returncode, completed.stdout, completed.stderr)
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            return CommandResult(124, stdout, f"{stderr}\nCommand timed out after {timeout:g} seconds".strip())
        except OSError as exc:
            return CommandResult(127, "", str(exc))


def _truncate(value: str, limit: int = 1000) -> str:
    value = value.strip()
    return value if len(value) <= limit else value[: limit - 3] + "..."


def _redact_command(command: str) -> str:
    patterns = (
        r"(?i)(api[_-]?key|token|password|passwd|secret)=([^\s]+)",
        r"(?i)(bearer\s+)([^\s]+)",
    )
    result = command
    for pattern in patterns:
        result = re.sub(pattern, lambda match: f"{match.group(1)}=<redacted>", result)
    return _truncate(result, 600)


def _tmux_snapshot(runner: HostRunner) -> tuple[list[dict[str, Any]], str | None]:
    result = runner.run(["tmux", "list-sessions", "-F", "#{session_name}\t#{session_created}\t#{session_windows}\t#{session_attached}"])
    if result.returncode != 0:
        message = result.stderr.strip()
        if "no server running" in message or "no sessions" in message:
            return [], None
        return [], _truncate(message or "tmux list-sessions failed")
    sessions = []
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) == 4:
            sessions.append(
                {
                    "name": fields[0],
                    "created_epoch": int(fields[1]) if fields[1].isdigit() else None,
                    "windows": int(fields[2]) if fields[2].isdigit() else None,
                    "attached": fields[3] == "1",
                }
            )
    return sessions, None


def _process_snapshot(runner: HostRunner) -> tuple[list[dict[str, Any]], str | None]:
    result = runner.run(["ps", "-eo", "pid=,etimes=,comm=,args="])
    if result.returncode != 0:
        return [], _truncate(result.stderr or "ps failed")
    processes = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 3)
        if len(fields) < 3:
            continue
        pid, elapsed, executable = fields[:3]
        command = fields[3] if len(fields) == 4 else executable
        processes.append(
            {
                "pid": int(pid) if pid.isdigit() else pid,
                "elapsed_seconds": int(elapsed) if elapsed.isdigit() else None,
                "executable": executable,
                "command": _redact_command(command),
            }
        )
    return processes, None


def _gpu_snapshot(runner: HostRunner) -> tuple[list[dict[str, Any]], str | None]:
    result = runner.run(
        [
            "nvidia-smi",
            "--query-compute-apps=gpu_uuid,pid,process_name,used_memory",
            "--format=csv,noheader,nounits",
        ]
    )
    if result.returncode != 0:
        return [], _truncate(result.stderr or "nvidia-smi failed")
    entries = []
    for line in result.stdout.splitlines():
        fields = [part.strip() for part in line.split(",", 3)]
        if len(fields) == 4:
            entries.append(
                {
                    "gpu_uuid": fields[0],
                    "pid": int(fields[1]) if fields[1].isdigit() else fields[1],
                    "process_name": fields[2],
                    "used_memory_mib": int(fields[3]) if fields[3].isdigit() else fields[3],
                }
            )
    return entries, None


def discover_host(host_id: str, host: dict[str, Any]) -> dict[str, Any]:
    runner = HostRunner(host_id, host)
    sessions, tmux_error = _tmux_snapshot(runner)
    processes, process_error = _process_snapshot(runner)
    gpus, gpu_error = _gpu_snapshot(runner)
    workloads = [item for item in processes if item["executable"].lower() in WORKLOAD_EXECUTABLES]
    errors = {
        key: value
        for key, value in {
            "tmux": tmux_error,
            "process": process_error,
            "gpu": gpu_error,
        }.items()
        if value
    }
    return {
        "host": host_id,
        "checked_at": utc_now(),
        "tmux_sessions": sessions,
        "workload_processes": workloads[:200],
        "gpu_processes": gpus,
        "errors": errors,
    }


def _match_processes(processes: list[dict[str, Any]], patterns: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
    matches: dict[str, list[dict[str, Any]]] = {}
    for pattern in patterns:
        try:
            regex = re.compile(pattern)
        except re.error as exc:
            matches[pattern] = [{"error": f"Invalid regular expression: {exc}"}]
            continue
        matches[pattern] = [entry for entry in processes if regex.search(entry["command"])]
    return matches


def _log_probe(runner: HostRunner, probe: dict[str, Any]) -> dict[str, Any]:
    path = probe["path"]
    result = runner.run(["tail", "-n", str(probe.get("tail_lines", 200)), "--", path])
    if result.returncode != 0:
        return {"path": path, "exists": False, "error": _truncate(result.stderr or "tail failed")}
    text = result.stdout
    error_matches = [pattern for pattern in probe.get("error_patterns", []) if re.search(pattern, text, re.MULTILINE)]
    success_matches = [pattern for pattern in probe.get("success_patterns", []) if re.search(pattern, text, re.MULTILINE)]
    return {
        "path": path,
        "exists": True,
        "error_matches": error_matches,
        "success_matches": success_matches,
        "last_lines": text.splitlines()[-min(20, probe.get("tail_lines", 200)) :],
    }


def _artifact_probe(runner: HostRunner, probe: dict[str, Any]) -> dict[str, Any]:
    path = probe["path"]
    result = runner.run(["stat", "-c", "%s\t%Y\t%F", "--", path])
    if result.returncode != 0:
        return {"path": path, "exists": False}
    fields = result.stdout.strip().split("\t", 2)
    size = int(fields[0]) if fields and fields[0].isdigit() else None
    minimum = probe.get("min_size_bytes", 0)
    valid = size is not None and size >= minimum
    return {
        "path": path,
        "exists": True,
        "size_bytes": size,
        "modified_epoch": int(fields[1]) if len(fields) > 1 and fields[1].isdigit() else None,
        "file_type": fields[2] if len(fields) > 2 else None,
        "valid": valid,
    }


def _slurm_probe(runner: HostRunner, job_ids: list[str]) -> list[dict[str, Any]]:
    if not job_ids:
        return []
    result = runner.run(["squeue", "-h", "-j", ",".join(job_ids), "-o", "%i\t%T\t%M\t%R"])
    if result.returncode != 0:
        return [{"error": _truncate(result.stderr or "squeue failed")}]
    entries = []
    for line in result.stdout.splitlines():
        fields = line.split("\t", 3)
        if len(fields) == 4:
            entries.append({"job_id": fields[0], "state": fields[1], "elapsed": fields[2], "reason_or_node": fields[3]})
    return entries


def snapshot_task(
    task: dict[str, Any],
    host: dict[str, Any],
    shared: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runner = HostRunner(task["host"], host)
    probes = task.get("probes", {})
    errors: list[str] = []
    if shared is None:
        sessions, tmux_error = _tmux_snapshot(runner)
        processes, process_error = _process_snapshot(runner)
        gpu_processes, gpu_error = _gpu_snapshot(runner)
        errors.extend(error for error in (tmux_error, process_error, gpu_error) if error)
    else:
        sessions = shared["tmux_sessions"]
        processes = shared["processes"]
        gpu_processes = shared["gpu_processes"]
        errors.extend(shared.get("errors", []))

    tmux_matches = {
        pattern: [session for session in sessions if fnmatch.fnmatchcase(session["name"], pattern)]
        for pattern in probes.get("tmux", [])
    }
    process_matches = _match_processes(processes, probes.get("process", []))
    log_results = [_log_probe(runner, probe) for probe in probes.get("logs", [])]
    artifact_results = [_artifact_probe(runner, probe) for probe in probes.get("artifacts", [])]
    slurm_results = _slurm_probe(runner, probes.get("slurm", []))

    selected_gpu_ids = set(probes.get("gpu", []))
    selected_gpu_processes: list[dict[str, Any]] = []
    if selected_gpu_ids:
        uuid_result = runner.run(["nvidia-smi", "--query-gpu=index,uuid", "--format=csv,noheader"])
        index_to_uuid: dict[int, str] = {}
        if uuid_result.returncode == 0:
            for line in uuid_result.stdout.splitlines():
                fields = [part.strip() for part in line.split(",", 1)]
                if len(fields) == 2 and fields[0].isdigit():
                    index_to_uuid[int(fields[0])] = fields[1]
            selected_uuids = {index_to_uuid[index] for index in selected_gpu_ids if index in index_to_uuid}
            selected_gpu_processes = [entry for entry in gpu_processes if entry["gpu_uuid"] in selected_uuids]
        else:
            errors.append(_truncate(uuid_result.stderr or "GPU index lookup failed"))

    failed = any(result.get("error_matches") for result in log_results)
    failed = failed or any(result.get("state", "").upper() in FAILED_SLURM_STATES for result in slurm_results)
    succeeded = any(result.get("success_matches") for result in log_results)
    required_artifacts = [result for probe, result in zip(probes.get("artifacts", []), artifact_results) if probe.get("required", True)]
    if required_artifacts and all(result.get("exists") and result.get("valid") for result in required_artifacts):
        succeeded = True
    running = any(tmux_matches.values()) or any(process_matches.values()) or bool(selected_gpu_processes)
    running = running or any(result.get("state", "").upper() in RUNNING_SLURM_STATES for result in slurm_results)
    if failed:
        status = "failed"
    elif succeeded:
        status = "succeeded"
    elif running:
        status = "running"
    else:
        status = "inactive"

    return {
        "id": task["id"],
        "project": task["project"],
        "host": task["host"],
        "status": status,
        "checked_at": utc_now(),
        "tmux": tmux_matches,
        "process": process_matches,
        "logs": log_results,
        "artifacts": artifact_results,
        "slurm": slurm_results,
        "gpu_processes": selected_gpu_processes,
        "probe_errors": errors,
    }


def snapshot_tasks(task_ids: list[str] | None = None) -> dict[str, Any]:
    registry = load_registry()
    selected = [task for task in registry["tasks"] if task.get("enabled", True)]
    if task_ids is not None:
        requested = set(task_ids)
        selected = [task for task in selected if task["id"] in requested]
        missing = requested - {task["id"] for task in selected}
        if missing:
            raise MonitorError(f"unknown or disabled task ids: {', '.join(sorted(missing))}")

    shared_by_host: dict[str, dict[str, Any]] = {}
    for host_id in {task["host"] for task in selected}:
        runner = HostRunner(host_id, registry["hosts"][host_id])
        sessions, tmux_error = _tmux_snapshot(runner)
        processes, process_error = _process_snapshot(runner)
        gpu_processes, gpu_error = _gpu_snapshot(runner)
        shared_by_host[host_id] = {
            "tmux_sessions": sessions,
            "processes": processes,
            "gpu_processes": gpu_processes,
            "errors": [error for error in (tmux_error, process_error, gpu_error) if error],
        }
    snapshots = [snapshot_task(task, registry["hosts"][task["host"]], shared_by_host[task["host"]]) for task in selected]
    return {"checked_at": utc_now(), "registry": str(registry_path()), "tasks": snapshots}


def list_tasks() -> dict[str, Any]:
    registry = load_registry()
    return {
        "registry": str(registry_path()),
        "poll_interval_seconds": registry["poll_interval_seconds"],
        "hosts": registry["hosts"],
        "tasks": registry["tasks"],
    }


def upsert_host(host_id: str, host: dict[str, Any]) -> dict[str, Any]:
    validate_host(host_id, host)
    registry = load_registry()
    registry["hosts"][host_id] = host
    save_registry(registry)
    return {"updated": host_id, "host": host, "registry": str(registry_path())}


def upsert_task(task: dict[str, Any]) -> dict[str, Any]:
    registry = load_registry()
    validate_task(task, registry["hosts"])
    replaced = False
    for index, existing in enumerate(registry["tasks"]):
        if existing["id"] == task["id"]:
            registry["tasks"][index] = task
            replaced = True
            break
    if not replaced:
        registry["tasks"].append(task)
    save_registry(registry)
    return {"updated": task["id"], "replaced": replaced, "registry": str(registry_path())}


def remove_task(task_id: str) -> dict[str, Any]:
    registry = load_registry()
    remaining = [task for task in registry["tasks"] if task["id"] != task_id]
    if len(remaining) == len(registry["tasks"]):
        raise MonitorError(f"unknown task id {task_id}")
    registry["tasks"] = remaining
    save_registry(registry)
    return {"removed": task_id, "registry": str(registry_path())}


def discover_hosts(host_ids: list[str] | None = None) -> dict[str, Any]:
    registry = load_registry()
    selected = host_ids or list(registry["hosts"])
    missing = set(selected) - set(registry["hosts"])
    if missing:
        raise MonitorError(f"unknown host ids: {', '.join(sorted(missing))}")
    return {
        "checked_at": utc_now(),
        "hosts": [discover_host(host_id, registry["hosts"][host_id]) for host_id in selected],
    }


def _load_last_state() -> dict[str, Any]:
    path = state_dir() / "latest.json"
    if not path.exists():
        return {"tasks": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"tasks": []}


def _append_event(event: dict[str, Any]) -> None:
    directory = state_dir()
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def _desktop_notify(event: dict[str, Any]) -> None:
    if shutil.which("notify-send") is None:
        return
    subprocess.run(
        ["notify-send", "Job Monitor", f"{event['task_id']}: {event['previous_status']} -> {event['status']}"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=5,
    )


def poll_once() -> dict[str, Any]:
    registry = load_registry()
    previous = _load_last_state()
    previous_statuses = {task["id"]: task.get("status") for task in previous.get("tasks", [])}
    current = snapshot_tasks()
    for task in current["tasks"]:
        old_status = previous_statuses.get(task["id"])
        new_status = task["status"]
        if old_status is not None and old_status != new_status:
            event = {
                "timestamp": current["checked_at"],
                "task_id": task["id"],
                "project": task["project"],
                "host": task["host"],
                "previous_status": old_status,
                "status": new_status,
            }
            _append_event(event)
            if registry.get("notifications", {}).get("desktop", False):
                _desktop_notify(event)
    _atomic_write_json(state_dir() / "latest.json", current)
    return current


def list_events(limit: int = 100) -> dict[str, Any]:
    if not 1 <= limit <= 1000:
        raise MonitorError("limit must be between 1 and 1000")
    path = state_dir() / "events.jsonl"
    if not path.exists():
        return {"events": [], "path": str(path)}
    lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
    events = []
    for line in lines:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return {"events": events, "path": str(path)}


def _pid_path() -> Path:
    return state_dir() / "daemon.pid"


def _daemon_matches(pid: int) -> bool:
    try:
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="replace")
    except OSError:
        return False
    return "job_monitor.py daemon" in command


def monitor_status() -> dict[str, Any]:
    pid_path = _pid_path()
    pid = None
    running = False
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
            running = _daemon_matches(pid)
        except (OSError, ValueError):
            running = False
    latest_path = state_dir() / "latest.json"
    latest = None
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8")).get("checked_at")
        except (OSError, json.JSONDecodeError):
            latest = None
    return {
        "running": running,
        "pid": pid if running else None,
        "registry": str(registry_path()),
        "state_dir": str(state_dir()),
        "last_checked_at": latest,
    }


def start_monitor(interval_seconds: int | None = None) -> dict[str, Any]:
    status = monitor_status()
    if status["running"]:
        return {**status, "started": False, "message": "Monitor is already running"}
    registry = load_registry()
    if interval_seconds is not None:
        if not 10 <= interval_seconds <= 86400:
            raise MonitorError("interval_seconds must be between 10 and 86400")
        registry["poll_interval_seconds"] = interval_seconds
        save_registry(registry)
    directory = state_dir()
    directory.mkdir(parents=True, exist_ok=True)
    script = Path(__file__).with_name("job_monitor.py").resolve()
    log_handle = (directory / "daemon.log").open("ab", buffering=0)
    process = subprocess.Popen(
        [sys.executable, str(script), "daemon"],
        stdin=subprocess.DEVNULL,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
    log_handle.close()
    _pid_path().write_text(f"{process.pid}\n", encoding="utf-8")
    return {**monitor_status(), "started": True}


def stop_monitor() -> dict[str, Any]:
    status = monitor_status()
    if not status["running"]:
        return {**status, "stopped": False, "message": "Monitor is not running"}
    pid = int(status["pid"])
    os.kill(pid, signal.SIGTERM)
    return {"running": False, "pid": pid, "stopped": True, "message": "SIGTERM sent to monitor daemon"}


def run_daemon() -> None:
    stop_event = threading.Event()

    def handle_stop(_signum: int, _frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    _pid_path().parent.mkdir(parents=True, exist_ok=True)
    _pid_path().write_text(f"{os.getpid()}\n", encoding="utf-8")
    try:
        while not stop_event.is_set():
            try:
                poll_once()
            except Exception as exc:  # Keep the monitor alive after transient host failures.
                _append_event({"timestamp": utc_now(), "type": "monitor_error", "error": str(exc)})
            interval = load_registry().get("poll_interval_seconds", DEFAULT_INTERVAL_SECONDS)
            stop_event.wait(interval)
    finally:
        try:
            if _pid_path().read_text(encoding="utf-8").strip() == str(os.getpid()):
                _pid_path().unlink()
        except OSError:
            pass
