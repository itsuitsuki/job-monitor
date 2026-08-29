# Job Monitor

[EN](README.md) | [JP](README.ja.md)

MCP 経由で永続的なコマンド監視とジョブ監視を提供する、Pi および Codex 向けパッケージです。

コマンドモニターはバックグラウンドで one-shot probe を実行し、状態の変化を通知します。probe が終了状態に到達した場合、または設定したエラーポリシーによって停止が指定されている場合は終了します。

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

実装には、状態の永続化、イベントの重複排除、tmux ベースのワーカー、Paseo 通知、ローカルまたは SSH の構造化ジョブ probe が含まれます。

## 必要な環境

- Python 3.10 以降
- Bash
- tmux
- コマンドモニターの通知に使用する Paseo CLI
- Pi パッケージのエントリーポイントに使用する Node.js と Pi

## Codex で使用する

このリポジトリは Codex marketplace に登録されていません。Codex では MCP server を直接登録して使用します。

### MCP server を登録する

リポジトリを clone し、launcher を Codex に追加します。

```bash
git clone git@github.com:itsuitsuki/job-monitor.git
cd job-monitor
codex mcp add job-monitor -- "$PWD/scripts/launch_job_monitor_mcp"
```

登録を確認します。

```bash
codex mcp list
```

登録後に新しい Codex session を開始してください。`monitor`、`monitor_list`、`monitor_get`、`monitor_stop` のツールを Codex から使用できます。

リポジトリには Codex plugin metadata として `.codex-plugin/plugin.json` も含まれています。ただし、これによってリポジトリが marketplace に登録または公開されることはありません。

## Pi へのインストール

npm パッケージが公開されるまでは、GitHub から直接インストールできます。

```bash
pi install git:git@github.com:itsuitsuki/job-monitor.git
```

インストール後、Pi で `/reload` を実行してください。パッケージはインストール先のディレクトリから MCP server を登録するため、現在のプロジェクトディレクトリには依存しません。MCP server は lazy に起動し、そのツールが使用されたときに開始されます。

npm で公開した後は、次の方法でもインストールできます。

```bash
pi install npm:pi-job-monitor
```

## MCP server として使用する

MCP host がプロジェクトローカルの server 定義に対応している場合は、リポジトリに含まれる `.mcp.json` を使用できます。リポジトリのルートから次を実行してください。

```bash
./scripts/launch_job_monitor_mcp
```

launcher は自身のパスからリポジトリの場所を解決し、依存関係のない Python MCP server を起動します。

## コマンドモニターのツール

- `monitor`: one-shot の read-only shell probe を永続的に監視する
- `monitor_list`: MCP server と Codex の再起動をまたいで watcher を一覧表示する
- `monitor_get`: watcher の仕様、ライフサイクル、最新状態、イベント、ログを確認する
- `monitor_stop`: watcher だけを停止する。監視対象のジョブは停止しない

`monitor` が polling loop を管理します。command は 1 回分の状態スナップショットを出力して終了してください。正規化された出力は重複排除され、`extract_regex` を使うと詳細な出力から安定した状態を抽出できます。状態が変化すると、`paseo send --no-wait` によって `<monitor-event>` メッセージが送信されます。各状態は通知前に永続化されるため、通知に失敗しても次の interval で同じイベントが重複して生成されません。`terminal_regex`、または有効化された `error_regex` に一致すると、worker は最終状態を記録し、通知を 1 回試行して終了します。

入力例:

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

同じツールで、`ssh host 'tmux capture-pane ...'` のような read-only SSH probe も実行できます。plugin は probe が read-only かどうかを判定しないため、変更を伴わないコマンドを使用する責任は呼び出し側にあります。

## 構造化タスクレジストリ

タスクレジストリは次の probe に対応しています。

- tmux session name の glob
- process command の正規表現
- ログの成功・エラー正規表現
- 正確な artifact path と最小サイズ
- GPU index とアクティブな計算プロセス
- Slurm job ID

デフォルトのレジストリは `~/.config/job-monitor/registry.json` です。runtime snapshot と状態変化イベントは `~/.local/state/job-monitor/` に保存されます。

## 開発

```bash
npm test
```

テストスイートは Python 標準ライブラリの `unittest` を使用するため、Node の build step は必要ありません。
