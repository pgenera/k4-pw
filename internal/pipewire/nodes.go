package pipewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config defines the PipeWire virtual node names and human-readable descriptions.
type Config struct {
	RxName        string
	RxDescription string
	TxName        string
	TxDescription string
}

// DefaultConfig returns the standard Elecraft K4 virtual audio node configuration.
func DefaultConfig() Config {
	return Config{
		RxName:        "k4_rx",
		RxDescription: "Elecraft K4 RX",
		TxName:        "k4_tx",
		TxDescription: "Elecraft K4 TX",
	}
}

func (c Config) withDefaults() Config {
	def := DefaultConfig()
	if c.RxName == "" {
		c.RxName = def.RxName
	}
	if c.RxDescription == "" {
		c.RxDescription = def.RxDescription
	}
	if c.TxName == "" {
		c.TxName = def.TxName
	}
	if c.TxDescription == "" {
		c.TxDescription = def.TxDescription
	}
	return c
}

// VirtualNodes owns a live pw-cli session that keeps ephemeral PipeWire nodes alive until Close.
type VirtualNodes struct {
	cfg    Config
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	waitCh chan error

	outMu sync.Mutex
	out   bytes.Buffer

	closeOnce sync.Once
	closeErr  error
}

type syncBuffer struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (b syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len() < 16384 {
		remain := 16384 - b.buf.Len()
		if len(p) > remain {
			b.buf.Write(p[:remain])
		} else {
			b.buf.Write(p)
		}
	}
	return len(p), nil
}

func quotePWString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n', '\r':
			b.WriteByte(' ')
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// StartEphemeral spawns an interactive pw-cli process that creates ephemeral RX and TX virtual
// audio nodes (without object.linger) and waits up to 2.5s for both to register in PipeWire.
func StartEphemeral(ctx context.Context, cfg Config) (*VirtualNodes, error) {
	cfg = cfg.withDefaults()
	if cfg.RxName == cfg.TxName {
		return nil, fmt.Errorf("pipewire: RxName and TxName must differ (%q)", cfg.RxName)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Do not bind cmd lifetime to ctx directly so Close() can send "quit\n" cleanly.
	cmd := exec.Command("pw-cli")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pipewire: stdin pipe: %w", err)
	}

	vn := &VirtualNodes{
		cfg:    cfg,
		cmd:    cmd,
		stdin:  stdin,
		waitCh: make(chan error, 1),
	}
	sb := syncBuffer{mu: &vn.outMu, buf: &vn.out}
	cmd.Stdout = sb
	cmd.Stderr = sb

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("pipewire: start pw-cli: %w", err)
	}

	go func() {
		vn.waitCh <- cmd.Wait()
		close(vn.waitCh)
	}()

	rxCmd := fmt.Sprintf(
		`create-node adapter { factory.name=support.null-audio-sink node.name=%s node.description=%s media.class="Audio/Source/Virtual" audio.position="FL,FR" monitor.passthrough=true node.pause-on-idle=false }`,
		quotePWString(cfg.RxName), quotePWString(cfg.RxDescription),
	)
	txCmd := fmt.Sprintf(
		`create-node adapter { factory.name=support.null-audio-sink node.name=%s node.description=%s media.class="Audio/Sink" audio.position="FL,FR" priority.driver=100 priority.session=100 node.pause-on-idle=false }`,
		quotePWString(cfg.TxName), quotePWString(cfg.TxDescription),
	)

	if _, err := io.WriteString(stdin, rxCmd+"\n"+txCmd+"\n"); err != nil {
		_ = vn.Close()
		return nil, fmt.Errorf("pipewire: write create-node commands: %w", err)
	}

	deadline := time.NewTimer(2500 * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	var lastRx, lastTx bool
	var lastCheckErr error
	for {
		rxOK, txOK, checkErr := CheckNodesExist(cfg.RxName, cfg.TxName)
		if checkErr == nil && rxOK && txOK {
			return vn, nil
		}
		lastRx, lastTx, lastCheckErr = rxOK, txOK, checkErr

		select {
		case <-ctx.Done():
			_ = vn.Close()
			return nil, ctx.Err()
		case waitErr := <-vn.waitCh:
			vn.drainCloseNoWait()
			return nil, fmt.Errorf("pipewire: pw-cli exited prematurely (err=%v): %s", waitErr, vn.outputString())
		case <-deadline.C:
			_ = vn.Close()
			return nil, fmt.Errorf(
				"pipewire: timed out waiting for nodes (rx=%q ok=%v, tx=%q ok=%v, checkErr=%v, output=%q)",
				cfg.RxName, lastRx, cfg.TxName, lastTx, lastCheckErr, vn.outputString(),
			)
		case <-ticker.C:
		}
	}
}

func (v *VirtualNodes) outputString() string {
	v.outMu.Lock()
	defer v.outMu.Unlock()
	return strings.TrimSpace(v.out.String())
}

func (v *VirtualNodes) drainCloseNoWait() {
	v.closeOnce.Do(func() {
		if v.stdin != nil {
			_ = v.stdin.Close()
		}
	})
}

// Close sends "quit" to pw-cli, waits/terminates the process, and verifies both nodes disappear.
func (v *VirtualNodes) Close() error {
	if v == nil {
		return nil
	}
	v.closeOnce.Do(func() {
		if v.stdin != nil {
			_, _ = io.WriteString(v.stdin, "quit\n")
			_ = v.stdin.Close()
		}

		var waitErr error
		select {
		case waitErr = <-v.waitCh:
		case <-time.After(1 * time.Second):
			if v.cmd.Process != nil {
				_ = v.cmd.Process.Kill()
			}
			waitErr = <-v.waitCh
		}

		// Poll briefly until PipeWire drops the ephemeral nodes.
		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			rxOK, txOK, err := CheckNodesExist(v.cfg.RxName, v.cfg.TxName)
			if err != nil || (!rxOK && !txOK) {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		if waitErr != nil && !errors.Is(waitErr, io.EOF) {
			// Ignore exit error only if already killed after timeout? Surface real exit failures if any.
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) && exitErr.ExitCode() == -1 {
				v.closeErr = nil
				return
			}
			v.closeErr = waitErr
		}
	})
	return v.closeErr
}

// CheckNodesExist runs `pw-cli ls Node` and checks whether rxName and txName are currently registered.
func CheckNodesExist(rxName, txName string) (rxOK bool, txOK bool, err error) {
	out, err := exec.Command("pw-cli", "ls", "Node").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return false, false, fmt.Errorf("pw-cli ls Node: %w: %s", err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return false, false, fmt.Errorf("pw-cli ls Node: %w", err)
	}

	names := ParseNodeNames(string(out))
	for _, name := range names {
		if rxName != "" && name == rxName {
			rxOK = true
		}
		if txName != "" && name == txName {
			txOK = true
		}
	}
	return rxOK, txOK, nil
}

// ParseNodeNames extracts `node.name = "..."` values from `pw-cli ls Node` output.
func ParseNodeNames(output string) []string {
	var names []string
	inNode := true
	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "id ") {
			inNode = strings.Contains(line, "PipeWire:Interface:Node") || !strings.Contains(line, "PipeWire:Interface:")
			continue
		}
		if !inNode {
			continue
		}
		if !strings.HasPrefix(line, "node.name") {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(line, "node.name"))
		if !strings.HasPrefix(rest, "=") {
			continue
		}
		val := strings.TrimSpace(strings.TrimPrefix(rest, "="))
		if val == "" {
			continue
		}
		if unquoted, err := strconv.Unquote(val); err == nil {
			names = append(names, unquoted)
			continue
		}
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			names = append(names, val[1:len(val)-1])
		} else {
			names = append(names, val)
		}
	}
	return names
}
