#!/usr/bin/env python3
"""Tests for Claude-style persistent command monitors."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


PLUGIN_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PLUGIN_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import command_monitor_lib as monitor  # noqa: E402


class CommandMonitorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.previous_environment = {
            name: os.environ.get(name)
            for name in (
                "JOB_MONITOR_COMMAND_STATE_DIR",
                "JOB_MONITOR_TMUX_BIN",
                "PASEO_AGENT_CWD",
                "PASEO_AGENT_ID",
                "PASEO_CLI",
                "FAKE_TMUX_LOG",
            )
        }
        os.environ["JOB_MONITOR_COMMAND_STATE_DIR"] = str(self.root / "state")

    def tearDown(self) -> None:
        for name, value in self.previous_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.temporary.cleanup()

    def _write_executable(self, name: str, source: str) -> Path:
        path = self.root / name
        path.write_text(source, encoding="utf-8")
        path.chmod(0o755)
        return path

    def test_worker_deduplicates_events_and_exits_on_terminal(self) -> None:
        counter = self.root / "counter.txt"
        probe = self._write_executable(
            "probe.py",
            """#!/usr/bin/env python3
from pathlib import Path

counter = Path(__file__).with_name("counter.txt")
index = int(counter.read_text() if counter.exists() else "0")
states = ["PENDING", "PENDING", "RUNNING", "RUNNING", "COMPLETE"]
print(states[min(index, len(states) - 1)])
counter.write_text(str(index + 1))
""",
        )
        notifications = self.root / "notifications.jsonl"
        fake_paseo = self._write_executable(
            "fake_paseo.py",
            f"""#!/usr/bin/env python3
import json
from pathlib import Path
import sys

prompt = Path(sys.argv[sys.argv.index("--prompt-file") + 1]).read_text()
with Path({str(notifications)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"argv": sys.argv[1:], "prompt": prompt}}) + "\\n")
""",
        )
        monitor_dir = self.root / "state" / "mon-test"
        monitor_dir.mkdir(parents=True)
        spec = {
            "schema_version": monitor.COMMAND_MONITOR_SCHEMA_VERSION,
            "monitor_id": "mon-test",
            "description": "sequence test",
            "command": str(probe),
            "interval_seconds": 0.01,
            "probe_timeout_seconds": 1,
            "extract_regex": None,
            "terminal_regex": "^COMPLETE$",
            "error_regex": None,
            "emit_initial": True,
            "exit_on_terminal": True,
            "exit_on_error": False,
            "max_runtime_seconds": 0,
            "cwd": str(self.root),
            "agent_id": "agent-test",
            "paseo_bin": str(fake_paseo),
            "bash_bin": "/bin/bash",
            "tmux_session": "jobmon-mon-test",
            "created_at": monitor.utc_now(),
        }
        (monitor_dir / "spec.json").write_text(json.dumps(spec), encoding="utf-8")
        (monitor_dir / "status.json").write_text("{}", encoding="utf-8")

        command = [
            sys.executable,
            str(SCRIPTS_DIR / "command_monitor_worker.py"),
            "--monitor-dir",
            str(monitor_dir),
        ]
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        events = [
            json.loads(line)
            for line in (monitor_dir / "events.jsonl").read_text().splitlines()
        ]
        self.assertEqual([event["state"] for event in events], ["PENDING", "RUNNING", "COMPLETE"])
        self.assertTrue(all(event["delivered"] for event in events))
        self.assertEqual(events[-1]["kind"], "terminal")
        calls = [json.loads(line) for line in notifications.read_text().splitlines()]
        self.assertEqual(len(calls), 3)
        self.assertTrue(all("<monitor-id>mon-test</monitor-id>" in call["prompt"] for call in calls))
        status = json.loads((monitor_dir / "status.json").read_text())
        self.assertEqual(status["lifecycle"], "terminal")
        self.assertIsNone(status["pid"])

        restarted = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(restarted.returncode, 0, restarted.stderr)
        events_after_restart = (monitor_dir / "events.jsonl").read_text().splitlines()
        self.assertEqual(len(events_after_restart), 3)
        calls_after_restart = notifications.read_text().splitlines()
        self.assertEqual(len(calls_after_restart), 3)

    def test_start_uses_create_then_send_keys(self) -> None:
        tmux_log = self.root / "tmux.jsonl"
        fake_tmux = self._write_executable(
            "fake_tmux.py",
            """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

with Path(os.environ["FAKE_TMUX_LOG"]).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[1] == "send-keys" and "command_monitor_worker.py" in sys.argv[4]:
    state_root = Path(os.environ["JOB_MONITOR_COMMAND_STATE_DIR"])
    ready_fifo = next(state_root.glob("*/ready.fifo"))
    with ready_fifo.open("w", encoding="utf-8") as handle:
        handle.write("ready\\n")
""",
        )
        fake_paseo = self._write_executable(
            "fake_paseo.py",
            "#!/usr/bin/env python3\nraise SystemExit(0)\n",
        )
        os.environ["JOB_MONITOR_TMUX_BIN"] = str(fake_tmux)
        os.environ["FAKE_TMUX_LOG"] = str(tmux_log)
        os.environ["PASEO_AGENT_ID"] = "agent-test"
        os.environ["PASEO_CLI"] = str(fake_paseo)
        os.environ["PASEO_AGENT_CWD"] = str(self.root)

        result = monitor.start_command_monitor(
            description="tmux order",
            command="printf PENDING",
        )
        calls = [json.loads(line) for line in tmux_log.read_text().splitlines()]
        self.assertEqual(calls[0][0:3], ["new-session", "-d", "-s"])
        self.assertEqual(calls[1][0], "set-window-option")
        self.assertEqual(calls[2][0:3], ["send-keys", "-t", result["tmux_session"]])
        self.assertTrue(calls[2][3].startswith("cd "))
        self.assertEqual(calls[3][0:3], ["send-keys", "-t", result["tmux_session"]])
        self.assertIn("command_monitor_worker.py", calls[3][3])

    def test_terminal_state_does_not_retry_after_notification_failure(self) -> None:
        calls = self.root / "notification-attempts.txt"
        fake_paseo = self._write_executable(
            "flaky_paseo.py",
            f"""#!/usr/bin/env python3
from pathlib import Path

path = Path({str(calls)!r})
attempt = int(path.read_text() if path.exists() else "0") + 1
path.write_text(str(attempt))
raise SystemExit(1)
""",
        )
        monitor_dir = self.root / "state" / "mon-retry"
        monitor_dir.mkdir(parents=True)
        spec = {
            "schema_version": monitor.COMMAND_MONITOR_SCHEMA_VERSION,
            "monitor_id": "mon-retry",
            "description": "retry delivery",
            "command": "printf 'COMPLETE\\n'",
            "interval_seconds": 0.01,
            "probe_timeout_seconds": 1,
            "extract_regex": None,
            "terminal_regex": "^COMPLETE$",
            "error_regex": None,
            "emit_initial": False,
            "exit_on_terminal": True,
            "exit_on_error": False,
            "max_runtime_seconds": 0,
            "cwd": str(self.root),
            "agent_id": "agent-test",
            "paseo_bin": str(fake_paseo),
            "bash_bin": "/bin/bash",
            "tmux_session": "jobmon-mon-retry",
            "created_at": monitor.utc_now(),
        }
        (monitor_dir / "spec.json").write_text(json.dumps(spec), encoding="utf-8")
        (monitor_dir / "status.json").write_text("{}", encoding="utf-8")
        completed = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "command_monitor_worker.py"),
                "--monitor-dir",
                str(monitor_dir),
            ],
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        events = [
            json.loads(line)
            for line in (monitor_dir / "events.jsonl").read_text().splitlines()
        ]
        self.assertEqual([event["delivered"] for event in events], [False])
        self.assertEqual(calls.read_text(), "1")
        status = json.loads((monitor_dir / "status.json").read_text())
        self.assertEqual(status["lifecycle"], "terminal")
        self.assertEqual(status["notification_failures"], 1)
        self.assertEqual(status["delivery_count"], 0)

        restarted = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "command_monitor_worker.py"),
                "--monitor-dir",
                str(monitor_dir),
            ],
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(restarted.returncode, 0, restarted.stderr)
        self.assertEqual(calls.read_text(), "1")
        self.assertEqual(len((monitor_dir / "events.jsonl").read_text().splitlines()), 1)

    def test_error_state_does_not_retry_after_notification_failure(self) -> None:
        calls = self.root / "error-notification-attempts.txt"
        fake_paseo = self._write_executable(
            "always_fail_paseo.py",
            f"""#!/usr/bin/env python3
from pathlib import Path

path = Path({str(calls)!r})
attempt = int(path.read_text() if path.exists() else "0") + 1
path.write_text(str(attempt))
raise SystemExit(1)
""",
        )
        monitor_dir = self.root / "state" / "mon-error"
        monitor_dir.mkdir(parents=True)
        spec = {
            "schema_version": monitor.COMMAND_MONITOR_SCHEMA_VERSION,
            "monitor_id": "mon-error",
            "description": "error delivery",
            "command": "printf 'FAILED\\n'; exit 1",
            "interval_seconds": 0.01,
            "probe_timeout_seconds": 1,
            "extract_regex": None,
            "terminal_regex": None,
            "error_regex": "^FAILED$",
            "emit_initial": False,
            "exit_on_terminal": True,
            "exit_on_error": True,
            "max_runtime_seconds": 0,
            "cwd": str(self.root),
            "agent_id": "agent-test",
            "paseo_bin": str(fake_paseo),
            "bash_bin": "/bin/bash",
            "tmux_session": "jobmon-mon-error",
            "created_at": monitor.utc_now(),
        }
        (monitor_dir / "spec.json").write_text(json.dumps(spec), encoding="utf-8")
        (monitor_dir / "status.json").write_text("{}", encoding="utf-8")
        command = [
            sys.executable,
            str(SCRIPTS_DIR / "command_monitor_worker.py"),
            "--monitor-dir",
            str(monitor_dir),
        ]
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        events = [
            json.loads(line)
            for line in (monitor_dir / "events.jsonl").read_text().splitlines()
        ]
        self.assertEqual(len(events), 1)
        self.assertFalse(events[0]["delivered"])
        self.assertEqual(calls.read_text(), "1")
        status = json.loads((monitor_dir / "status.json").read_text())
        self.assertEqual(status["lifecycle"], "error")

        restarted = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(restarted.returncode, 0, restarted.stderr)
        self.assertEqual(calls.read_text(), "1")
        self.assertEqual(len((monitor_dir / "events.jsonl").read_text().splitlines()), 1)

    def test_invalid_regex_is_rejected_before_tmux(self) -> None:
        with self.assertRaisesRegex(monitor.CommandMonitorError, "Invalid terminal_regex"):
            monitor.start_command_monitor(
                description="invalid",
                command="printf PENDING",
                terminal_regex="(",
                agent_id="agent-test",
            )


if __name__ == "__main__":
    unittest.main()
