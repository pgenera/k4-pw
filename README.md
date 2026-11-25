# k4-pw

Headless PipeWire virtual audio + CAT bridge connecting an Elecraft K4 (`:9205`) directly to digital-mode software such as `wsjtx-improved`.

`k4-pw` exposes two virtual PipeWire audio devices and a local CAT listener (`127.0.0.1:9200`) multiplexed over a **single** K4 `:9205` TCP session:
- **`Elecraft K4 RX`** (`k4_rx`, `Audio/Source/Virtual`) — Stereo 12 kHz RX audio from the K4 (`Left` = Main RX, `Right` = Sub RX)
- **`Elecraft K4 TX`** (`k4_tx`, `Audio/Sink`) — TX audio input sent to the K4 (`DAP` network modulator)
- **`127.0.0.1:9200`** — Hamlib-compatible CAT + PTT proxy over `:9205` (uses only 1 client slot on the K4 and routes `:9205` TX audio when WSJT-X keys PTT via CAT)

## Prerequisites

1. Install Go (`>= 1.21`), `wsjtx-improved`, PipeWire CLI tools, and PulseAudio simple client library:
   ```bash
   sudo apt install golang-go wsjtx-improved pipewire-utils libpulse0
   ```
2. On the **Elecraft K4**, if you also plan to run another remote client (such as `k4-js` Web UI) at the same time:
   - **`CONFIG` -> `TCP/IP` -> `Max Clients >= 2`**.

## One-Step Install (On-Demand `systemd` Socket + Static PipeWire Nodes)

Run `./scripts/install.sh` to build `bin/k4-audio`, install low-priority static `k4_rx` / `k4_tx` PipeWire nodes, and enable `k4-audio.socket` on `127.0.0.1:9200`:

```bash
./scripts/install.sh --host <k4-hostname-or-ip> --password <k4-password>
```

How it works once installed:
1. **`50-k4-audio.conf`** registers `k4_rx` and `k4_tx` in PipeWire at priority `100` (suspended at `0%` CPU when idle, never overriding your default desktop mic/speakers) so WSJT-X always finds its configured audio devices at startup — even when the K4 is powered off.
2. **`k4-audio.socket`** listens passively on `127.0.0.1:9200` (`0%` CPU, `0` LAN traffic while WSJT-X is closed) and starts `k4-audio.service` automatically the moment WSJT-X connects.
3. **Auto-shutdown**: `20s` after WSJT-X closes (or `20s` after failing fast if the K4 is powered off), `k4-audio` sends `RRN;` and exits cleanly, handing `127.0.0.1:9200` back to `systemd`.

## Manual Configuration

If configuring manually instead of via `./scripts/install.sh`, create `~/.config/k4-audio/config.env` (`0600` permissions):

```bash
mkdir -p ~/.config/k4-audio
cp configs/config.env.example ~/.config/k4-audio/config.env
chmod 600 ~/.config/k4-audio/config.env
```

Edit `~/.config/k4-audio/config.env` and set `K4_HOST` (and `K4_PASSWORD` or `K4_PASSWORD_FILE`).

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

Because `k4-audio` multiplexes both WSJT-X CAT and audio over a single `:9205` connection, running alongside `k4-js` Web UI only requires **`Max Clients >= 2`** on the K4:
- Keep the Web UI audio codec on its default `EM1` (12 kHz PCM) rather than Opus (`EM2`/`EM3`), and avoid holding Web UI PTT while `wsjtx` is transmitting. (Leaving Web UI audio set to **`Audio Off`** is also convenient if you do not want FT8 audio playing through your desktop speakers.)
- Point WSJT-X at `127.0.0.1:9200` (local `k4-audio` CAT proxy) as usual.

## On-Demand `systemd --user` Socket Activation (Recommended)

With `configs/50-k4-audio.conf` and `configs/k4-audio.socket`, no background `k4-audio` process runs when WSJT-X is closed (`0%` CPU, `0` LAN traffic even when the K4 is powered off):
1. `50-k4-audio.conf` registers low-priority (`100`) static `k4_rx` / `k4_tx` nodes in PipeWire so WSJT-X always finds its configured audio devices at startup without changing your desktop default mic/speakers.
2. `k4-audio.socket` listens passively on `127.0.0.1:9200` and launches `k4-audio.service` on demand as soon as WSJT-X connects.
3. When you close WSJT-X (or if the K4 is powered off and WSJT-X is idle at its Rig Control Error prompt), `k4-audio` exits cleanly `20s` after the last `:9200` client disconnects, handing `127.0.0.1:9200` back to `systemd`.

```bash
mkdir -p ~/.config/pipewire/pipewire.conf.d ~/.config/systemd/user
cp configs/50-k4-audio.conf ~/.config/pipewire/pipewire.conf.d/
cp configs/k4-audio.socket configs/k4-audio.service ~/.config/systemd/user/
# Create nodes in the live session without restarting PipeWire (50-k4-audio.conf loads on future logins):
pw-cli create-node adapter '{ factory.name=support.null-audio-sink node.name="k4_rx" node.description="Elecraft K4 RX" media.class="Audio/Source/Virtual" audio.position="FL,FR" monitor.passthrough=true priority.driver=100 priority.session=100 object.linger=true }'
pw-cli create-node adapter '{ factory.name=support.null-audio-sink node.name="k4_tx" node.description="Elecraft K4 TX" media.class="Audio/Sink" audio.position="FL,FR" priority.driver=100 priority.session=100 object.linger=true }'
systemctl --user daemon-reload
systemctl --user enable --now k4-audio.socket
```

Follow live CAT/PTT/session events at any time with:

```bash
journalctl --user -u k4-audio -f
```
