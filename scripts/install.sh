#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT_DIR/bin/k4-audio"
CFG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/k4-audio"
CFG_FILE="$CFG_DIR/config.env"
PW_CONF_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/pipewire/pipewire.conf.d"
SYSTEMD_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"

K4_HOST_ARG=""
K4_PASSWORD_ARG=""

usage() {
    cat <<EOF
Usage: ./scripts/install.sh [--host <k4-host>] [--password <k4-password>]

Builds bin/k4-audio, installs static PipeWire nodes (k4_rx / k4_tx),
installs and enables systemd socket activation (127.0.0.1:9200 -> k4-audio.service),
and initializes ~/.config/k4-audio/config.env (0600) if not already present.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            K4_HOST_ARG="${2:-}"
            shift 2
            ;;
        --host=*)
            K4_HOST_ARG="${1#*=}"
            shift
            ;;
        --password)
            K4_PASSWORD_ARG="${2:-}"
            shift 2
            ;;
        --password=*)
            K4_PASSWORD_ARG="${1#*=}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

echo "==> Building $BIN"
mkdir -p "$ROOT_DIR/bin"
go -C "$ROOT_DIR" build -o "$BIN" ./cmd/k4-audio

mkdir -p "$CFG_DIR"
if [[ ! -f "$CFG_FILE" ]]; then
    echo "==> Creating $CFG_FILE"
    cp "$ROOT_DIR/configs/config.env.example" "$CFG_FILE"
    chmod 600 "$CFG_FILE"
else
    chmod 600 "$CFG_FILE"
    echo "==> Preserving existing $CFG_FILE"
fi

if [[ -n "$K4_HOST_ARG" ]]; then
    sed -i "s|^K4_HOST=.*|K4_HOST=$K4_HOST_ARG|" "$CFG_FILE"
fi
if [[ -n "$K4_PASSWORD_ARG" ]]; then
    if grep -q '^K4_PASSWORD=' "$CFG_FILE"; then
        sed -i "s|^K4_PASSWORD=.*|K4_PASSWORD=$K4_PASSWORD_ARG|" "$CFG_FILE"
    else
        sed -i "s|^# *K4_PASSWORD=.*|K4_PASSWORD=$K4_PASSWORD_ARG|" "$CFG_FILE"
    fi
fi

echo "==> Installing PipeWire virtual node config to $PW_CONF_DIR/50-k4-audio.conf"
mkdir -p "$PW_CONF_DIR"
cp "$ROOT_DIR/configs/50-k4-audio.conf" "$PW_CONF_DIR/50-k4-audio.conf"

echo "==> Installing systemd user socket and service to $SYSTEMD_DIR"
mkdir -p "$SYSTEMD_DIR"
cp "$ROOT_DIR/configs/k4-audio.socket" "$SYSTEMD_DIR/k4-audio.socket"
sed "s|^ExecStart=.*|ExecStart=$BIN|" "$ROOT_DIR/configs/k4-audio.service" > "$SYSTEMD_DIR/k4-audio.service"

echo "==> Ensuring k4_rx and k4_tx exist in live PipeWire session (without restarting PipeWire)"
systemctl --user daemon-reload
systemctl --user stop k4-audio.service 2>/dev/null || true
if ! "$BIN" check >/dev/null 2>&1; then
    pw-cli create-node adapter '{ factory.name=support.null-audio-sink node.name="k4_rx" node.description="Elecraft K4 RX" media.class="Audio/Source/Virtual" audio.position="FL,FR" monitor.passthrough=true priority.driver=100 priority.session=100 object.linger=true }' >/dev/null
    pw-cli create-node adapter '{ factory.name=support.null-audio-sink node.name="k4_tx" node.description="Elecraft K4 TX" media.class="Audio/Sink" audio.position="FL,FR" priority.driver=100 priority.session=100 object.linger=true }' >/dev/null
fi

echo "==> Enabling k4-audio.socket"
systemctl --user enable --now k4-audio.socket

echo "==> Verifying PipeWire nodes"
"$BIN" check
echo ""
echo "Done! Edit $CFG_FILE if needed, then configure WSJT-X:"
echo "  Settings -> Radio: Rig = Elecraft K4, Network Server = 127.0.0.1:9200, PTT Method = CAT, Mode = Data/Pkt"
echo "  Settings -> Audio: Input = k4_rx (Left), Output = k4_tx"
