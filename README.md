# k4-pw

Headless PipeWire virtual audio bridge connecting an Elecraft K4 (`:9205`) directly to digital-mode software such as `wsjtx-improved`.

While `k4-audio` is running, it creates two ephemeral PipeWire virtual audio devices:
- **`Elecraft K4 RX`** (`k4_rx`, `Audio/Source/Virtual`) — Stereo 12 kHz RX audio from the K4 (`Left` = Main RX, `Right` = Sub RX)
- **`Elecraft K4 TX`** (`k4_tx`, `Audio/Sink`) — TX audio input sent to the K4

When `k4-audio` exits (or `wsjtx` closes via `scripts/k4-wsjtx`), the ephemeral PipeWire nodes are automatically removed.

## Prerequisites

1. Install `wsjtx-improved` and PipeWire CLI utilities:
   ```bash
   sudo apt install wsjtx-improved pipewire-utils
   ```
2. On the **Elecraft K4**, ensure multiple concurrent TCP clients are permitted:
   - **`CONFIG` -> `TCP/IP` -> `Max Clients >= 2`** (use `>= 3` if also running a Web UI simultaneously: `:9200` for WSJT-X CAT + `:9205` for `k4-audio` + Web UI).

## Configuration

Create `~/.config/k4-audio/config.env` from the example template:

```bash
mkdir -p ~/.config/k4-audio
cp configs/config.env.example ~/.config/k4-audio/config.env
```

Edit `~/.config/k4-audio/config.env` and set `K4_HOST` (and `K4_PASSWORD` or `K4_PASSWORD_FILE` if your K4 requires a network password).

## Quick Start (`k4-wsjtx`)

Launch the bridge and `wsjtx` together with automatic build, PipeWire readiness check, and clean teardown when `wsjtx` closes:

```bash
./scripts/k4-wsjtx
```

Any arguments passed to `./scripts/k4-wsjtx` are forwarded to `k4-audio` (for example, `./scripts/k4-wsjtx --host 192.168.1.50`).

## Manual CLI Usage

Build and run manually:

```bash
go build -o bin/k4-audio ./cmd/k4-audio
./bin/k4-audio --host <k4-ip>
```

Check whether PipeWire nodes are active and print the WSJT-X setup summary:

```bash
./bin/k4-audio check
```

### Key Flags

All `~/.config/k4-audio/config.env` settings can be overridden via CLI flags:
- `--host` / `--port=9205` — K4 IP/hostname and remote audio port
- `--cat-listen=127.0.0.1:9200` — Local TCP CAT listener for WSJT-X (multiplexes all CAT + `TX;`/`RX;` over the single `:9205` session so the K4 modulates from `:9205` network audio and only uses 1 K4 client slot)
- `--password` / `--password-file` — K4 network password (SHA-384 auth)
- `--rx-gain-db=0` / `--tx-gain-db=0` — RX and TX gain trims in dB
- `--buffer-ms=40` — RX jitter buffer target in ms
- `--silence-threshold=2` / `--tx-hold-ms=100` — TX audio gate peak threshold and hold tail
- `--tx-cat-on-audio=false` — Key `TX;`/`RX;` over `:9205` on TX audio (keep `false` when WSJT-X keys via CAT; set `true` if using WSJT-X `PTT=VOX`)
- `--backend=auto` — Audio backend (`auto`, `pulse`, `pw-cat`, `mock`)
- `--rx-node=k4_rx` / `--tx-node=k4_tx` / `--no-ephemeral-nodes=false` — PipeWire node configuration (if static nodes from `configs/50-k4-audio.conf` already exist, `k4-audio` reuses them automatically without tearing them down on exit)
- `--idle-timeout` — Auto-exit after `0` CAT clients remain connected for the given duration (defaults to `20s` automatically when activated via `k4-audio.socket`; `0` disables)
- `--once=false` / `--log-levels=false` — Disable reconnect or log periodic level/buffer stats

## WSJT-X (`wsjtx-improved`) Configuration

In `wsjtx-improved`:

- **Settings -> Radio**:
  - **Rig**: `Elecraft K4`
  - **CAT Control -> Network Server**: `127.0.0.1:9200` (multiplexes CAT and PTT over `k4-audio`'s `:9205` session so K4 routes network TX audio to its modulator)
  - **PTT Method**: `CAT`
  - **Mode**: `Data/Pkt`
- **Settings -> Audio**:
  - **Input**: `k4_rx` (`Elecraft K4 RX`) — select **`Left`** (Main RX) or **`Right`** (Sub RX)
  - **Output**: `k4_tx` (`Elecraft K4 TX`)

## Running Alongside `k4-js` Web UI

With `Max Clients >= 2` (or `>= 3` alongside WSJT-X CAT), `k4-audio` can run concurrently with the `k4-js` Web UI:
- Keep the Web UI audio codec on its default `EM1` (12 kHz PCM) rather than Opus (`EM2`/`EM3`), and avoid holding Web UI PTT while `wsjtx` is transmitting. (Leaving Web UI audio set to **`Audio Off`** is also convenient if you do not want FT8 audio playing through your desktop speakers.)
- Use `wsjtx-improved` on `:9200` for CAT/PTT and `k4-audio` on `:9205` for audio.

## On-Demand `systemd --user` Socket Activation (Recommended)

With `configs/50-k4-audio.conf` and `configs/k4-audio.socket`, no background `k4-audio` process runs when WSJT-X is closed (`0%` CPU, `0` LAN traffic even when the K4 is powered off):
1. `50-k4-audio.conf` registers low-priority (`100`) static `k4_rx` / `k4_tx` nodes in PipeWire so WSJT-X always finds its configured audio devices at startup without changing your desktop default mic/speakers.
2. `k4-audio.socket` listens passively on `127.0.0.1:9200` and launches `k4-audio.service` on demand as soon as WSJT-X connects.
3. When you close WSJT-X (or if the K4 is powered off and WSJT-X is idle at its Rig Control Error prompt), `k4-audio` exits cleanly `20s` after the last `:9200` client disconnects, handing `127.0.0.1:9200` back to `systemd`.

```bash
mkdir -p ~/.config/pipewire/pipewire.conf.d ~/.config/systemd/user
cp configs/50-k4-audio.conf ~/.config/pipewire/pipewire.conf.d/
cp configs/k4-audio.socket configs/k4-audio.service ~/.config/systemd/user/
systemctl --user restart pipewire pipewire-pulse wireplumber
systemctl --user daemon-reload
systemctl --user enable --now k4-audio.socket
```

Follow live CAT/PTT/session events at any time with:

```bash
journalctl --user -u k4-audio -f
```
