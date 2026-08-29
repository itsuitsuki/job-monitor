#!/usr/bin/env python3
"""Entry point for one persistent command monitor worker."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from command_monitor_lib import CommandMonitorError, run_command_worker


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a persistent command monitor worker.")
    parser.add_argument("--monitor-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        return run_command_worker(args.monitor_dir)
    except CommandMonitorError as exc:
        print(f"command-monitor: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
