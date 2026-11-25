//go:build linux

package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
)

const sdListenFDsStart = 3

// claimSystemdListener inspects LISTEN_PID / LISTEN_FDS and returns an inherited
// net.Listener for FD 3 if this process was socket-activated by systemd.
func claimSystemdListener() (net.Listener, error) {
	pidStr := os.Getenv("LISTEN_PID")
	fdsStr := os.Getenv("LISTEN_FDS")
	if pidStr == "" || fdsStr == "" {
		return nil, nil
	}

	// Always clear env vars so child processes (pw-cat, pw-link, pw-cli) never inherit them.
	_ = os.Unsetenv("LISTEN_PID")
	_ = os.Unsetenv("LISTEN_FDS")
	_ = os.Unsetenv("LISTEN_FDNAMES")

	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid != os.Getpid() {
		return nil, nil
	}
	nfds, err := strconv.Atoi(fdsStr)
	if err != nil || nfds < 1 {
		return nil, nil
	}

	syscall.CloseOnExec(sdListenFDsStart)
	f := os.NewFile(uintptr(sdListenFDsStart), "systemd-cat-listener")
	if f == nil {
		return nil, fmt.Errorf("invalid systemd socket fd %d", sdListenFDsStart)
	}
	defer f.Close()

	ln, err := net.FileListener(f)
	if err != nil {
		return nil, fmt.Errorf("systemd FileListener(fd=%d): %w", sdListenFDsStart, err)
	}
	return ln, nil
}
