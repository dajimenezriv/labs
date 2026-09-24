# TMUX

The **prefix key** is `Ctrl+b` by default. Press it, release, then press the next key.

## From the shell

| Command                     | What it does             |
| --------------------------- | ------------------------ |
| `tmux`                      | Start a session          |
| `tmux new -s work`          | Start a named session    |
| `tmux ls`                   | List sessions            |
| `tmux a`                    | Reattach to last session |
| `tmux a -t work`            | Reattach to a session    |
| `tmux kill-session -t work` | Kill a session           |

## Panes

| Key                     | What it does                                  |
| ----------------------- | --------------------------------------------- |
| `prefix "`              | Split horizontally (stacked)                  |
| `prefix %`              | Split vertically (side by side)               |
| `prefix ← ↑ ↓ →`        | Move between panes                            |
| `prefix z`              | Zoom pane to full screen (same key to unzoom) |
| `prefix x`              | Close pane (or just `exit`)                   |
| `prefix space`          | Cycle through layouts                         |
| `prefix Alt + 0-9`      | Change layout                                 |
| `prefix Alt + ← ↑ ↓ →`  | Resize pane                                   |
| `prefix Ctrl + ← ↑ ↓ →` | Resize pane                                   |

## Windows

Windows are like tabs — each one holds its own set of panes.

| Key                     | What it does              |
| ----------------------- | ------------------------- |
| `prefix c`              | New window                |
| `prefix n` / `prefix p` | Next / previous window    |
| `prefix 0-9`            | Jump to window by number  |
| `prefix w`              | Pick a window from a list |
| `prefix ,`              | Rename window             |
| `prefix &`              | Close window              |

## Sessions

| Key        | What it does                      |
| ---------- | --------------------------------- |
| `prefix d` | Detach (everything keeps running) |
| `prefix s` | Pick a session from a list        |
| `prefix $` | Rename session                    |

## Scrolling / copy mode

| Key                   | What it does              |
| --------------------- | ------------------------- |
| `prefix [`            | Enter copy mode           |
| `↑ ↓` / `PgUp` `PgDn` | Scroll while in copy mode |
| `q`                   | Exit copy mode            |

## Config

- `prefix ?` — list every key binding
- `~/.tmux.conf` — config file; remap the prefix to `Ctrl+a` here if `Ctrl+b` fights with your shell
- `prefix :` then `source-file ~/.tmux.conf` — reload config without restarting
