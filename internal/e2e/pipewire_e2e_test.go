package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"k4-audio/internal/k4proto"
	"k4-audio/internal/pipewire"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buf.Len() < 64*1024 {
		remain := 64*1024 - s.buf.Len()
		if len(p) > remain {
			s.buf.Write(p[:remain])
		} else {
			s.buf.Write(p)
		}
	}
	return len(p), nil
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.TrimSpace(s.buf.String())
}

type simDebugStats struct {
	TotalConnections int64 `json:"totalConnections"`
	AuthPassed       int64 `json:"authPassed"`
	AuthFailed       int64 `json:"authFailed"`
	FaHz             int64 `json:"faHz"`
	FbHz             int64 `json:"fbHz"`
	ModeA            int   `json:"modeA"`
	ModeB            int   `json:"modeB"`
	TxAudioPacketsIn int64 `json:"txAudioPacketsIn"`
}

func requirePipeWireHost(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"pw-cli", "pw-cat", "wpctl"} {
		if _, err := exec.LookPath(bin); err != nil {
			if os.Getenv("E2E_REQUIRE_PIPEWIRE") == "1" {
				t.Fatalf("required tool %s not found: %v", bin, err)
			}
			t.Skipf("PipeWire tool %s not installed: %v", bin, err)
		}
	}

	_, _, checkErr := pipewire.CheckNodesExist("k4_rx_e2e", "k4_tx_e2e")
	if checkErr == nil {
		return
	}

	if os.Getenv("E2E_REQUIRE_PIPEWIRE") == "1" {
		t.Fatalf("PipeWire daemon not reachable (E2E_REQUIRE_PIPEWIRE=1): %v", checkErr)
	}

	runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = fmt.Sprintf("/run/user/%d", os.Getuid())
	}
	sockPath := filepath.Join(runtimeDir, "pipewire-0")
	if _, statErr := os.Stat(sockPath); statErr == nil {
		if conn, dialErr := net.DialTimeout("unix", sockPath, time.Second); dialErr == nil {
			_ = conn.Close()
			t.Fatalf("PipeWire socket %s is reachable but pw-cli failed: %v", sockPath, checkErr)
		}
	}

	t.Skipf("PipeWire daemon not reachable (sandbox): %v", checkErr)
}

func allocFreeLoopbackAddr(t *testing.T) (string, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.1:0: %v", err)
	}
	tcpAddr := ln.Addr().(*net.TCPAddr)
	port := tcpAddr.Port
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr, port
}

func startManagedProcess(t *testing.T, name string, args ...string) (*exec.Cmd, <-chan error, *syncBuffer) {
	t.Helper()
	cmd := exec.Command(name, args...)
	out := &syncBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s %v: %v", name, args, err)
	}
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
		close(waitCh)
	}()
	t.Cleanup(func() {
		select {
		case <-waitCh:
			return
		default:
		}
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
		}
		select {
		case <-waitCh:
			return
		case <-time.After(2 * time.Second):
		}
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
		}
		<-waitCh
	})
	return cmd, waitCh, out
}

func fetchSimStats(httpAddr string) (simDebugStats, error) {
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + httpAddr + "/debug/stats")
	if err != nil {
		return simDebugStats{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return simDebugStats{}, fmt.Errorf("GET /debug/stats status %d", resp.StatusCode)
	}
	var st simDebugStats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return simDebugStats{}, err
	}
	return st, nil
}

func waitForSimStats(t *testing.T, httpAddr string, timeout time.Duration, pred func(simDebugStats) bool, desc string) simDebugStats {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last simDebugStats
	var lastErr error
	for time.Now().Before(deadline) {
		st, err := fetchSimStats(httpAddr)
		if err == nil {
			last = st
			lastErr = nil
			if pred(st) {
				return st
			}
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for sim stats (%s): last=%+v err=%v", desc, last, lastErr)
	return simDebugStats{}
}

func inspectDefaultSink(t *testing.T) (id, name string) {
	t.Helper()
	out, err := exec.Command("wpctl", "inspect", "@DEFAULT_AUDIO_SINK@").CombinedOutput()
	if err != nil {
		t.Fatalf("wpctl inspect @DEFAULT_AUDIO_SINK@ failed: %v\noutput: %s", err, string(out))
	}
	for _, rawLine := range strings.Split(string(out), "\n") {
		line := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(rawLine), "*"))
		if id == "" && strings.HasPrefix(line, "id ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				id = strings.TrimSuffix(fields[1], ",")
			}
		}
		if name == "" && strings.HasPrefix(line, "node.name") {
			if eq := strings.IndexByte(line, '='); eq >= 0 {
				val := strings.TrimSpace(line[eq+1:])
				name = strings.Trim(val, "\"")
			}
		}
	}
	if id == "" || name == "" {
		t.Fatalf("failed to parse default sink id/name from wpctl output:\n%s", string(out))
	}
	return id, name
}

func resolveK4AudioBin(t *testing.T) string {
	t.Helper()
	binPath, err := filepath.Abs(filepath.Join("..", "..", "bin", "k4-audio"))
	if err == nil {
		if info, statErr := os.Stat(binPath); statErr == nil && !info.IsDir() {
			return binPath
		}
	}
	rootDir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	tmpBin := filepath.Join(t.TempDir(), "k4-audio")
	buildCmd := exec.Command("go", "-C", rootDir, "build", "-o", tmpBin, "./cmd/k4-audio")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build k4-audio fallback failed: %v\n%s", err, string(out))
	}
	return tmpBin
}

func computeStereoRMS(raw []byte) (leftRMS, rightRMS float64) {
	frames := len(raw) / 4
	if frames == 0 {
		return 0, 0
	}
	var sumL, sumR float64
	for i := 0; i < frames; i++ {
		off := i * 4
		l := float64(int16(binary.LittleEndian.Uint16(raw[off : off+2])))
		r := float64(int16(binary.LittleEndian.Uint16(raw[off+2 : off+4])))
		sumL += l * l
		sumR += r * r
	}
	return math.Sqrt(sumL / float64(frames)), math.Sqrt(sumR / float64(frames))
}

func generateSine1kStereo(frames int, amp float64) []byte {
	buf := make([]byte, frames*4)
	const freq = 1000.0
	const rate = 12000.0
	for i := 0; i < frames; i++ {
		s := int16(math.Round(amp * math.Sin(2.0*math.Pi*freq*float64(i)/rate)))
		off := i * 4
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(s))
		binary.LittleEndian.PutUint16(buf[off+2:off+4], uint16(s))
	}
	return buf
}

func TestLivePipeWireAndSimE2E(t *testing.T) {
	requirePipeWireHost(t)

	const (
		rxNode = "k4_rx_e2e"
		txNode = "k4_tx_e2e"
	)

	// Guard against stale nodes from a previously aborted run.
	if rxExists, txExists, err := pipewire.CheckNodesExist(rxNode, txNode); err == nil && (rxExists || txExists) {
		t.Fatalf("pre-existing E2E PipeWire nodes found (rx=%v tx=%v)", rxExists, txExists)
	}

	simAddr, simPort := allocFreeLoopbackAddr(t)
	httpAddr, _ := allocFreeLoopbackAddr(t)

	simDir := os.Getenv("K4_SIM_DIR")
	if simDir == "" {
		for _, candidate := range []string{"../../../k4-js/sim", "../../../k4/sim"} {
			if info, err := os.Stat(candidate); err == nil && info.IsDir() {
				simDir, _ = filepath.Abs(candidate)
				break
			}
		}
	}
	if simDir == "" {
		t.Skip("k4sim directory not found (set K4_SIM_DIR or clone k4-js alongside k4-pw)")
	}
	k4simBin := filepath.Join(simDir, "k4sim")
	var simCmd *exec.Cmd
	var simWait <-chan error
	var simOut *syncBuffer
	if info, err := os.Stat(k4simBin); err == nil && !info.IsDir() {
		simCmd, simWait, simOut = startManagedProcess(t, k4simBin, "-listen", simAddr, "-control-http", httpAddr)
	} else {
		simCmd, simWait, simOut = startManagedProcess(t, "go", "-C", simDir, "run", ".", "-listen", simAddr, "-control-http", httpAddr)
	}
	_ = simCmd

	// Wait for k4sim HTTP and TCP endpoints to start listening.
	waitForSimStats(t, httpAddr, 5*time.Second, func(st simDebugStats) bool { return true }, "k4sim HTTP ready")

	// Connect a lightweight CAT control client to configure VFO A (14.025 MHz CW beacon) vs VFO B (14.100 MHz noise).
	ctrlConn, err := net.DialTimeout("tcp", simAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial k4sim %s: %v\nk4sim output:\n%s", simAddr, err, simOut.String())
	}
	ctrlClosed := false
	t.Cleanup(func() {
		if !ctrlClosed {
			_ = ctrlConn.Close()
		}
	})

	if _, err := io.WriteString(ctrlConn, k4proto.ExpectedAuthHex("")); err != nil {
		t.Fatalf("control auth write: %v", err)
	}
	if _, err := ctrlConn.Write(k4proto.EncodeCAT("RDY;FA00014025000;MD3;FB00014100000;MD$2;SB1;")); err != nil {
		t.Fatalf("control CAT write: %v", err)
	}
	waitForSimStats(t, httpAddr, 3*time.Second, func(st simDebugStats) bool {
		return st.FaHz == 14025000 && st.FbHz == 14100000 && st.ModeA == 3 && st.ModeB == 2
	}, "faHz == 14025000")

	// k4sim's rxAudioFreqHz requires 80..4200 Hz audio beat (rfHz - FA);
	// offset VFO A by the 500 Hz CW pitch so the 14.025 MHz -58 dBm beacon produces a 500 Hz CW tone on Left.
	if _, err := ctrlConn.Write(k4proto.EncodeCAT("FA00014024500;IS0050;BW0050;")); err != nil {
		t.Fatalf("control CW beat offset CAT write: %v", err)
	}
	waitForSimStats(t, httpAddr, 3*time.Second, func(st simDebugStats) bool {
		return st.FaHz == 14024500
	}, "faHz == 14024500")

	_, _ = ctrlConn.Write(k4proto.EncodeCAT("RRN;"))
	_ = ctrlConn.Close()
	ctrlClosed = true

	beforeSinkID, beforeSinkName := inspectDefaultSink(t)
	t.Logf("default sink before k4-audio: id=%s name=%s", beforeSinkID, beforeSinkName)

	k4AudioBin := resolveK4AudioBin(t)
	bridgeCmd, bridgeWait, bridgeOut := startManagedProcess(
		t,
		k4AudioBin,
		"--host", "127.0.0.1",
		"--port", fmt.Sprintf("%d", simPort),
		"--rx-node", rxNode,
		"--tx-node", txNode,
		"--once",
		"--log-levels",
	)

	// Wait up to 4s for ephemeral PipeWire nodes to appear.
	nodeDeadline := time.Now().Add(4 * time.Second)
	nodesReady := false
	for time.Now().Before(nodeDeadline) {
		select {
		case err := <-bridgeWait:
			t.Fatalf("k4-audio exited prematurely (%v)\nk4-audio output:\n%s\nk4sim output:\n%s",
				err, bridgeOut.String(), simOut.String())
		default:
		}
		rxOK, txOK, err := pipewire.CheckNodesExist(rxNode, txNode)
		if err == nil && rxOK && txOK {
			nodesReady = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !nodesReady {
		t.Fatalf("PipeWire nodes (%s, %s) did not appear within 4s\nk4-audio output:\n%s",
			rxNode, txNode, bridgeOut.String())
	}

	afterSinkID, afterSinkName := inspectDefaultSink(t)
	if afterSinkID != beforeSinkID || afterSinkName != beforeSinkName {
		t.Fatalf("default sink hijacked! before=(%s, %q) after=(%s, %q)",
			beforeSinkID, beforeSinkName, afterSinkID, afterSinkName)
	}

	// Ensure k4-audio has connected/authenticated with k4sim and prefilled RX audio.
	waitForSimStats(t, httpAddr, 4*time.Second, func(st simDebugStats) bool {
		return st.AuthPassed >= 2
	}, "k4-audio authPassed >= 2")
	time.Sleep(200 * time.Millisecond)

	// Record ~1.2 s (14400 frames = 57600 bytes) of stereo S16LE from k4_rx_e2e.
	const rxBytesWant = 14400 * 4
	recCtx, recCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer recCancel()

	recCmd := exec.CommandContext(
		recCtx,
		"pw-cat",
		"--record",
		"--raw",
		"--format=s16",
		"--rate=12000",
		"--channels=2",
		"--channel-map=FL,FR",
		"--target="+rxNode,
		"-",
	)
	recStdout, err := recCmd.StdoutPipe()
	if err != nil {
		t.Fatalf("pw-cat record stdout pipe: %v", err)
	}
	var recStderr syncBuffer
	recCmd.Stderr = &recStderr
	if err := recCmd.Start(); err != nil {
		t.Fatalf("start pw-cat record: %v", err)
	}

	rxBuf := make([]byte, rxBytesWant)
	_, readErr := io.ReadFull(recStdout, rxBuf)
	pwLinks, _ := exec.Command("pw-link", "-l").CombinedOutput()
	wpStatus, _ := exec.Command("wpctl", "status").CombinedOutput()
	recCancel()
	_ = recCmd.Wait()
	if readErr != nil {
		t.Fatalf("read %d bytes from pw-cat record: %v (stderr=%q)\nk4-audio output:\n%s",
			rxBytesWant, readErr, recStderr.String(), bridgeOut.String())
	}

	leftRMS, rightRMS := computeStereoRMS(rxBuf)
	t.Logf("captured RX stereo RMS: leftRMS=%.2f rightRMS=%.2f ratio=%.2f", leftRMS, rightRMS, leftRMS/math.Max(rightRMS, 1e-6))
	if leftRMS <= 300 || leftRMS <= rightRMS*3.0 {
		t.Fatalf("stereo isolation check failed: leftRMS=%.2f rightRMS=%.2f (want leftRMS > 300 && leftRMS > rightRMS*3.0)\npw-link -l:\n%s\nwpctl status:\n%s\nk4-audio output:\n%s",
			leftRMS, rightRMS, string(pwLinks), string(wpStatus), bridgeOut.String())
	}

	// Play ~0.5 s (6000 frames) of 1 kHz sine wave (amp 16000) into k4_tx_e2e.
	txPCM := generateSine1kStereo(6000, 16000.0)
	playCtx, playCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer playCancel()

	playCmd := exec.CommandContext(
		playCtx,
		"pw-cat",
		"--playback",
		"--raw",
		"--format=s16",
		"--rate=12000",
		"--channels=2",
		"--channel-map=FL,FR",
		"--target="+txNode,
		"-",
	)
	playCmd.Stdin = bytes.NewReader(txPCM)
	var playOut syncBuffer
	playCmd.Stdout = &playOut
	playCmd.Stderr = &playOut
	if err := playCmd.Run(); err != nil {
		t.Fatalf("pw-cat playback failed: %v\noutput: %s\nk4-audio output:\n%s",
			err, playOut.String(), bridgeOut.String())
	}

	stAfterPlay := waitForSimStats(t, httpAddr, 3*time.Second, func(st simDebugStats) bool {
		return st.TxAudioPacketsIn > 0
	}, "txAudioPacketsIn > 0")
	t.Logf("TX stats after playback: txAudioPacketsIn=%d", stAfterPlay.TxAudioPacketsIn)

	// Wait 350 ms after playback ends so hold tail & buffers settle, then verify gating stops TX packets.
	time.Sleep(350 * time.Millisecond)
	stIdle1, err := fetchSimStats(httpAddr)
	if err != nil {
		t.Fatalf("fetchSimStats idle1: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	stIdle2, err := fetchSimStats(httpAddr)
	if err != nil {
		t.Fatalf("fetchSimStats idle2: %v", err)
	}
	t.Logf("TX gating check: idle1=%d idle2=%d", stIdle1.TxAudioPacketsIn, stIdle2.TxAudioPacketsIn)
	if stIdle2.TxAudioPacketsIn != stIdle1.TxAudioPacketsIn {
		t.Fatalf("TX audio packets continued during silence: idle1=%d idle2=%d",
			stIdle1.TxAudioPacketsIn, stIdle2.TxAudioPacketsIn)
	}

	// Send SIGINT to k4-audio and verify clean exit + ephemeral node removal.
	if err := bridgeCmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal SIGINT to k4-audio: %v", err)
	}
	select {
	case waitErr := <-bridgeWait:
		if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
			t.Fatalf("k4-audio exited with error on SIGINT: %v\noutput:\n%s", waitErr, bridgeOut.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("k4-audio did not exit within 5s of SIGINT\noutput:\n%s", bridgeOut.String())
	}

	goneDeadline := time.Now().Add(2 * time.Second)
	var rxStill, txStill bool
	for time.Now().Before(goneDeadline) {
		var checkErr error
		rxStill, txStill, checkErr = pipewire.CheckNodesExist(rxNode, txNode)
		if checkErr == nil && !rxStill && !txStill {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if rxStill || txStill {
		t.Fatalf("ephemeral nodes lingered after k4-audio exit: rxStill=%v txStill=%v", rxStill, txStill)
	}

	select {
	case sErr := <-simWait:
		if sErr != nil {
			t.Logf("k4sim exited early: %v", sErr)
		}
	default:
	}
}
