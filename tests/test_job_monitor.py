#!/usr/bin/env python3
"""Tests for the generic Job Monitor core."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PLUGIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_DIR / "scripts"))

import job_monitor_lib as monitor  # noqa: E402


class JobMonitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        os.environ["JOB_MONITOR_REGISTRY"] = str(root / "registry.json")
        os.environ["JOB_MONITOR_STATE_DIR"] = str(root / "state")

    def tearDown(self) -> None:
        self.temporary.cleanup()
        os.environ.pop("JOB_MONITOR_REGISTRY", None)
        os.environ.pop("JOB_MONITOR_STATE_DIR", None)

    def test_registry_round_trip(self) -> None:
        registry = monitor.load_registry()
        self.assertEqual(registry["hosts"]["local"]["kind"], "local")
        result = monitor.upsert_task(
            {
                "id": "example/task",
                "project": "example",
                "host": "local",
                "enabled": True,
                "probes": {"process": ["definitely-not-a-real-process"]},
            }
        )
        self.assertFalse(result["replaced"])
        self.assertEqual(monitor.list_tasks()["tasks"][0]["id"], "example/task")

    def test_artifact_marks_task_succeeded(self) -> None:
        root = Path(self.temporary.name)
        artifact = root / "result.json"
        artifact.write_text("{}", encoding="utf-8")
        task = {
            "id": "example/artifact",
            "project": "example",
            "host": "local",
            "enabled": True,
            "probes": {"artifacts": [{"path": str(artifact), "min_size_bytes": 2}]},
        }
        snapshot = monitor.snapshot_task(task, {"kind": "local"})
        self.assertEqual(snapshot["status"], "succeeded")

    def test_log_error_has_precedence(self) -> None:
        root = Path(self.temporary.name)
        log = root / "job.log"
        log.write_text("training complete\nCUDA out of memory\n", encoding="utf-8")
        task = {
            "id": "example/log",
            "project": "example",
            "host": "local",
            "enabled": True,
            "probes": {
                "logs": [
                    {
                        "path": str(log),
                        "success_patterns": ["training complete"],
                        "error_patterns": ["out of memory"],
                    }
                ]
            },
        }
        snapshot = monitor.snapshot_task(task, {"kind": "local"})
        self.assertEqual(snapshot["status"], "failed")

    def test_unrequested_gpu_processes_do_not_mark_task_running(self) -> None:
        task = {
            "id": "example/no-gpu-probe",
            "project": "example",
            "host": "local",
            "enabled": True,
            "probes": {},
        }
        shared = {
            "tmux_sessions": [],
            "processes": [],
            "gpu_processes": [
                {"gpu_uuid": "GPU-other", "pid": 1, "process_name": "other", "used_memory_mib": 10}
            ],
            "errors": [],
        }
        snapshot = monitor.snapshot_task(task, {"kind": "local"}, shared)
        self.assertEqual(snapshot["status"], "inactive")
        self.assertEqual(snapshot["gpu_processes"], [])

    def test_mcp_lists_tools(self) -> None:
        requests = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}}),
                json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
                "",
            ]
        )
        completed = subprocess.run(
            [sys.executable, str(PLUGIN_DIR / "scripts" / "mcp_server.py")],
            input=requests,
            text=True,
            capture_output=True,
            check=True,
            env=os.environ.copy(),
        )
        responses = [json.loads(line) for line in completed.stdout.splitlines()]
        names = {tool["name"] for tool in responses[1]["result"]["tools"]}
        self.assertIn("monitor", names)
        self.assertIn("monitor_list", names)
        self.assertIn("monitor_get", names)
        self.assertIn("monitor_stop", names)
        self.assertIn("start_monitor", names)
        self.assertIn("discover_running", names)


if __name__ == "__main__":
    unittest.main()
