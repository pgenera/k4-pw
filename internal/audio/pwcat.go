package audio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type cappedBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.max <= 0 {
		return len(p), nil
	}
	if len(p) >= c.max {
		c.buf = append(c.buf[:0], p[len(p)-c.max:]...)
		return len(p), nil
	}
	c.buf = append(c.buf, p...)
	if len(c.buf) > c.max {
		excess := len(c.buf) - c.max
		copy(c.buf, c.buf[excess:])
		c.buf = c.buf[:c.max]
	}
	return len(p), nil
}

func (c *cappedBuffer) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.TrimSpace(string(c.buf))
}

var playNodeSeq atomic.Uint64

type pwCatPlayback struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stderr     *cappedBuffer
	stopCh     chan struct{}
	tickerDone chan struct{}
	waitDone   chan struct{}
	waitErr    error
	closeOnce  sync.Once
}

func startPwCatPlayback(target string, sampleRate int, frameSamples int, ring *StereoRingBuffer) (*pwCatPlayback, error) {
	playNode := fmt.Sprintf("k4-audio-rx-%d-%d", os.Getpid(), playNodeSeq.Add(1))
	propsJSON := fmt.Sprintf("{\"node.name\":%q,\"node.autoconnect\":false}", playNode)
	args := []string{
		"--playback",
		"--raw",
		"--format=s16",
		fmt.Sprintf("--rate=%d", sampleRate),
		"--channels=2",
		"--channel-map=FL,FR",
		"--latency=20ms",
		"--target=0",
		"-P", propsJSON,
		"-",
	}

	cmd := exec.Command("pw-cat", args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("pw-cat playback stdin pipe: %w", err)
	}
	stderr := &cappedBuffer{max: 4096}
	cmd.Stdout = io.Discard
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start pw-cat playback: %w", err)
	}

	p := &pwCatPlayback{
		cmd:        cmd,
		stdin:      stdin,
		stderr:     stderr,
		stopCh:     make(chan struct{}),
		tickerDone: make(chan struct{}),
		waitDone:   make(chan struct{}),
	}

	go func() {
		p.waitErr = cmd.Wait()
		close(p.waitDone)
	}()

	interval := time.Duration(frameSamples) * time.Second / time.Duration(sampleRate)
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}

	go p.pumpLoop(interval, frameSamples, ring)

	if err := linkPlaybackPorts(playNode, target, p.waitDone, stderr, 2*time.Second); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func listNodeStereoPorts(flag, nodeName string) (fl, fr string, ok bool, err error) {
	out, err := exec.Command("pw-link", flag).Output()
	if err != nil {
		return "", "", false, err
	}
	prefix := nodeName + ":"
	var matched []string
	for _, rawLine := range strings.Split(string(out), "\n") {
		line := strings.TrimSpace(rawLine)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		matched = append(matched, line)
		switch {
		case strings.HasSuffix(line, "_FL") || strings.HasSuffix(line, ":FL"):
			fl = line
		case strings.HasSuffix(line, "_FR") || strings.HasSuffix(line, ":FR"):
			fr = line
		}
	}
	if fl != "" && fr != "" {
		return fl, fr, true, nil
	}
	if len(matched) >= 2 {
		return matched[0], matched[1], true, nil
	}
	return "", "", false, nil
}

func linkPlaybackPorts(playNode, target string, waitDone <-chan struct{}, stderr *cappedBuffer, timeout time.Duration) error {
	if _, err := exec.LookPath("pw-link"); err != nil {
		return nil
	}
	inFL, inFR, targetOK, err := listNodeStereoPorts("-i", target)
	if err != nil || !targetOK {
		// PipeWire daemon is unavailable or test uses a stub pw-cat without a real node.
		return nil
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-waitDone:
			return fmt.Errorf("pw-cat playback exited before linking (%s)", stderr.String())
		default:
		}
		outFL, outFR, playOK, _ := listNodeStereoPorts("-o", playNode)
		if playOK {
			if out, lerr := exec.Command("pw-link", outFL, inFL).CombinedOutput(); lerr != nil {
				return fmt.Errorf("pw-link %s -> %s: %w (%s)", outFL, inFL, lerr, strings.TrimSpace(string(out)))
			}
			if out, rerr := exec.Command("pw-link", outFR, inFR).CombinedOutput(); rerr != nil {
				return fmt.Errorf("pw-link %s -> %s: %w (%s)", outFR, inFR, rerr, strings.TrimSpace(string(out)))
			}
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for playback ports on %s to link to %s (%s)", playNode, target, stderr.String())
}

func (p *pwCatPlayback) pumpLoop(interval time.Duration, frameSamples int, ring *StereoRingBuffer) {
	defer close(p.tickerDone)
	defer func() { _ = p.stdin.Close() }()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	pcmBuf := make([]int16, frameSamples*2)
	rawBuf := make([]byte, frameSamples*4)

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			ring.Pull(pcmBuf)
			for i, s := range pcmBuf {
				binary.LittleEndian.PutUint16(rawBuf[i*2:i*2+2], uint16(s))
			}
			if err := writeFull(p.stdin, rawBuf); err != nil {
				return
			}
		}
	}
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (p *pwCatPlayback) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		close(p.stopCh)

		// Wait briefly for the ticker goroutine to close stdin cleanly; if a stalled
		// pipe blocks stdin.Write, terminate the child to unblock it safely.
		select {
		case <-p.tickerDone:
		case <-time.After(150 * time.Millisecond):
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
			}
			<-p.tickerDone
		}

		select {
		case <-p.waitDone:
		case <-time.After(250 * time.Millisecond):
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
			}
			select {
			case <-p.waitDone:
			case <-time.After(150 * time.Millisecond):
				if p.cmd.Process != nil {
					_ = p.cmd.Process.Kill()
				}
				<-p.waitDone
			}
		}

		// A killed child during teardown is expected; preserve unexpected errors only if needed.
		if p.waitErr != nil {
			var exitErr *exec.ExitError
			if errors.As(p.waitErr, &exitErr) && !exitErr.Success() {
				// Clean EOF after stdin close usually exits 0; ignore signal exit during forced teardown.
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
					return
				}
			}
			closeErr = p.waitErr
		}
	})
	return closeErr
}

type pwCatRecord struct {
	cmd       *exec.Cmd
	pr        *os.File
	stderr    *cappedBuffer
	rawBuf    []byte
	readMu    sync.Mutex
	closed    atomic.Bool
	waitDone  chan struct{}
	waitErr   error
	closeOnce sync.Once
}

func openPwCatRecord(target string, sampleRate int, frameSamples int) (*pwCatRecord, error) {
	pwArgs := []string{
		"--record",
		"--raw",
		"--format=s16",
		fmt.Sprintf("--rate=%d", sampleRate),
		"--channels=2",
		"--channel-map=FL,FR",
		"--latency=20ms",
		"--target=" + target,
		"-P", "stream.capture.sink=true",
		"-",
	}

	var cmd *exec.Cmd
	if stdbufPath, err := exec.LookPath("stdbuf"); err == nil {
		args := append([]string{"-o0", "pw-cat"}, pwArgs...)
		cmd = exec.Command(stdbufPath, args...)
	} else {
		cmd = exec.Command("pw-cat", pwArgs...)
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pw-cat record pipe: %w", err)
	}
	stderr := &cappedBuffer{max: 4096}
	cmd.Stdout = pw
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		return nil, fmt.Errorf("start pw-cat record: %w", err)
	}
	_ = pw.Close()

	r := &pwCatRecord{
		cmd:      cmd,
		pr:       pr,
		stderr:   stderr,
		rawBuf:   make([]byte, frameSamples*4),
		waitDone: make(chan struct{}),
	}
	go func() {
		r.waitErr = cmd.Wait()
		close(r.waitDone)
	}()
	return r, nil
}

func (r *pwCatRecord) ReadFrames(dst []int16, frameSamples int) (int, error) {
	needSamples := frameSamples * 2
	needBytes := needSamples * 2
	if len(dst) < needSamples {
		return 0, io.ErrShortBuffer
	}
	if r.closed.Load() {
		return 0, io.EOF
	}

	r.readMu.Lock()
	defer r.readMu.Unlock()

	if r.closed.Load() {
		return 0, io.EOF
	}
	if len(r.rawBuf) != needBytes {
		r.rawBuf = make([]byte, needBytes)
	}

	if _, err := io.ReadFull(r.pr, r.rawBuf); err != nil {
		if r.closed.Load() {
			return 0, io.EOF
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if msg := r.stderr.String(); msg != "" {
				return 0, fmt.Errorf("pw-cat record ended (%s): %w", msg, io.EOF)
			}
			return 0, io.EOF
		}
		return 0, fmt.Errorf("pw-cat record read: %w", err)
	}

	for i := 0; i < needSamples; i++ {
		dst[i] = int16(binary.LittleEndian.Uint16(r.rawBuf[i*2 : i*2+2]))
	}
	return frameSamples, nil
}

func (r *pwCatRecord) Close() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		_ = r.pr.Close()

		if r.cmd.Process != nil {
			_ = r.cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-r.waitDone:
		case <-time.After(150 * time.Millisecond):
			if r.cmd.Process != nil {
				_ = r.cmd.Process.Kill()
			}
			<-r.waitDone
		}
	})
	return nil
}
