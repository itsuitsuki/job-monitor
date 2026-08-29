#!/usr/bin/env python3
"""Command-line entry point and daemon for Job Monitor."""

from __future__ import annotations

import argparse
import json
import sys

from job_monitor_lib import MonitorError, poll_once, run_daemon


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor registered jobs across local and SSH hosts.")
    parser.add_argument("command", choices=("poll", "daemon"))
    args = parser.parse_args()
    try:
        if args.command == "poll":
            print(json.dumps(poll_once(), indent=2, sort_keys=True))
        else:
            run_daemon()
    except MonitorError as exc:
        print(f"job-monitor: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
