# Job Monitor

[EN](README.md) | [JP](README.ja.md)

Persistent command and job monitoring through MCP, packaged for Pi and Codex.

The command monitor runs a one-shot probe in the background, reports changed states, and stops when the probe reaches a terminal state or the configured error policy says to stop:

```text
monitor(probe, interval, exit_on_error):
    previous_state = null

    while true:
        state = probe()

        if state != previous_state:
            report(state)
            previous_state = state

        if state == completed:
            break

        if state == errored and exit_on_error:
            break

        sleep(interval)
```

The implementation adds persistent state, event deduplication, tmux-backed workers, Paseo notifications, and structured local or SSH job probes.

## Requirements

- Python 3.10+
- Bash
- tmux
- Paseo CLI for command-monitor notifications
- Node.js and Pi for the Pi package entry point

## Use with Codex

This repository is not registered in a Codex marketplace. The supported Codex setup is to register the MCP server directly.

### Register the MCP server

Clone the repository and add its launcher to Codex:

```bash
git clone git@github.com:itsuitsuki/job-monitor.git
cd job-monitor
codex mcp add job-monitor -- "$PWD/scripts/launch_job_monitor_mcp"
```

Check the registration with:

```bash
codex mcp list
```

Start a new Codex session after registration. The `monitor`, `monitor_list`, `monitor_get`, and `monitor_stop` tools will then be available to Codex.

The repository also includes `.codex-plugin/plugin.json` as Codex plugin metadata. It does not register or publish the repository in any marketplace.

## Install in Pi

Install directly from GitHub while the npm package is unpublished:

```bash
pi install git:git@github.com:itsuitsuki/job-monitor.git
```

Then reload Pi with `/reload`. The package registers its MCP server from the installed package directory, so it does not depend on the current project directory. The MCP server is lazy and starts when one of its tools is used.

When published to npm, the package source will be:

```bash
pi install npm:pi-job-monitor
```

## Use as an MCP server

The repository includes `.mcp.json` for MCP hosts that support project-local server definitions. From the repository root:

```bash
./scripts/launch_job_monitor_mcp
```

The launcher resolves the repository location from its own path and starts the dependency-free Python MCP server.

## Command monitor tools

- `monitor`: start a persistent watcher for a one-shot read-only shell probe
- `monitor_list`: list watchers across MCP server and Codex restarts
- `monitor_get`: inspect a watcher's specification, lifecycle, latest state, events, and log
- `monitor_stop`: stop the watcher only; it never stops the job being observed

`monitor` owns the polling loop. The command should print one state snapshot and exit. Normalized output is deduplicated; `extract_regex` can isolate a stable state from verbose output. A changed state is sent through `paseo send --no-wait` as a `<monitor-event>` message. Each observed state is persisted before notification, so a failed delivery does not create duplicate events on the next interval. When `terminal_regex` or an enabled `error_regex` matches, the worker records the final state and exits after one notification attempt.

Example input:

```json
{
  "description": "Watch a submission",
  "command": "check-submission --format state",
  "interval_seconds": 5,
  "terminal_regex": "(?i)(complete|cancelled)",
  "error_regex": "(?i)(error|failed)",
  "emit_initial": false,
  "exit_on_terminal": true,
  "exit_on_error": true
}
```

The same tool can run a read-only SSH probe such as `ssh host 'tmux capture-pane ...'`. The plugin does not infer that a probe is read-only, so callers remain responsible for using non-mutating commands.

## Structured task registry

The task registry supports:

- tmux session name globs
- process command regular expressions
- log success and error regular expressions
- exact artifact paths and minimum sizes
- GPU indices and active compute processes
- Slurm job IDs

The default registry is `~/.config/job-monitor/registry.json`. Runtime snapshots and state-change events are stored under `~/.local/state/job-monitor/`.

## Development

```bash
npm test
```

The test suite uses Python's standard `unittest` module and does not require a Node build step.
