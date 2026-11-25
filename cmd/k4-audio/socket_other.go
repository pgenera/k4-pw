//go:build !linux

package main

import "net"

func claimSystemdListener() (net.Listener, error) {
	return nil, nil
}
