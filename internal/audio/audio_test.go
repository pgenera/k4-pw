package audio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConfigDefaultsAndOpenValidation(t *testing.T) {
	cfg := (Config{}).withDefaults()
	if cfg.RxTarget != "k4_rx" {
		t.Fatalf("RxTarget = %q, want k4_rx", cfg.RxTarget)
	}
	if cfg.TxTarget != "k4_tx" {
		t.Fatalf("TxTarget = %q, want k4_tx", cfg.TxTarget)
	}
	if cfg.SampleRate != 12000 {
		t.Fatalf("SampleRate = %d, want 12000", cfg.SampleRate)
	}
	if cfg.FrameSamples != 240 {
		t.Fatalf("FrameSamples = %d, want 240", cfg.FrameSamples)
	}
	if cfg.TargetBufferMs != 40 {
		t.Fatalf("TargetBufferMs = %d, want 40", cfg.TargetBufferMs)
	}
	if cfg.Backend != "auto" {
		t.Fatalf("Backend = %q, want auto", cfg.Backend)
	}

	if _, err := Open(context.Background(), Config{Backend: "invalid"}); err == nil {
		t.Fatal("expected error for unsupported backend")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, Config{Backend: "mock"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	eng, err := Open(context.Background(), Config{Backend: "mock"})
	if err != nil {
		t.Fatalf("Open mock: %v", err)
	}
	defer eng.Close()
	if _, ok := eng.(*MockEngine); !ok {
		t.Fatalf("expected *MockEngine, got %T", eng)
	}
}

func TestStereoRingBufferChannelIndependence(t *testing.T) {
	rb := NewStereoRingBuffer(100, 0, 100)

	// Push and pull across wrap-around multiple times with strictly distinct L/R channels.
	var seq int
	for iter := 0; iter < 10; iter++ {
		const nFrames = 37
		in := make([]int16, nFrames*2)
		for i := 0; i < nFrames; i++ {
			v := seq + i + 1
			in[i*2] = int16(v)
			in[i*2+1] = int16(-v * 2)
		}
		rb.Push(in)

		out := make([]int16, nFrames*2)
		realFrames, started := rb.Pull(out)
		if !started || realFrames != nFrames {
			t.Fatalf("iter %d: Pull = (%d, %v), want (%d, true)", iter, realFrames, started, nFrames)
		}
		for i := 0; i < nFrames; i++ {
			wantL := int16(seq + i + 1)
			wantR := int16(-(seq + i + 1) * 2)
			if out[i*2] != wantL || out[i*2+1] != wantR {
				t.Fatalf("iter %d frame %d: got (%d, %d), want (%d, %d)",
					iter, i, out[i*2], out[i*2+1], wantL, wantR)
			}
		}
		seq += nFrames
	}
	if rb.Underruns() != 0 {
		t.Fatalf("Underruns = %d, want 0", rb.Underruns())
	}
}

func TestStereoRingBufferPrefill(t *testing.T) {
	rb := NewStereoRingBuffer(1000, 480, 1000)

	dst := make([]int16, 240*2)
	for i := range dst {
		dst[i] = 999
	}

	// Empty pull before prefill: zero-filled, not started, no underrun counted.
	n, started := rb.Pull(dst)
	if started || n != 0 {
		t.Fatalf("empty prefill Pull = (%d, %v), want (0, false)", n, started)
	}
	if rb.Underruns() != 0 {
		t.Fatalf("Underruns before start = %d, want 0", rb.Underruns())
	}
	for i, v := range dst {
		if v != 0 {
			t.Fatalf("dst[%d] = %d, want 0", i, v)
		}
	}

	// Push 240 frames (< 480 prefill): still waits without consuming buffered frames.
	pkt1 := make([]int16, 240*2)
	for i := 0; i < 240; i++ {
		pkt1[i*2] = int16(i + 1)
		pkt1[i*2+1] = int16(-(i + 1))
	}
	rb.Push(pkt1)
	if rb.Started() {
		t.Fatal("Started() = true below prefill threshold")
	}
	n, started = rb.Pull(dst)
	if started || n != 0 {
		t.Fatalf("below-prefill Pull = (%d, %v), want (0, false)", n, started)
	}
	if rb.BufferedFrames() != 240 {
		t.Fatalf("BufferedFrames = %d, want 240", rb.BufferedFrames())
	}
	if rb.Underruns() != 0 {
		t.Fatalf("Underruns = %d, want 0", rb.Underruns())
	}

	// Push another 240 frames (>= 480 prefill): playback starts cleanly.
	pkt2 := make([]int16, 240*2)
	for i := 0; i < 240; i++ {
		pkt2[i*2] = int16(1000 + i)
		pkt2[i*2+1] = int16(-(1000 + i))
	}
	rb.Push(pkt2)
	if !rb.Started() {
		t.Fatal("Started() = false after reaching prefill")
	}

	n, started = rb.Pull(dst)
	if !started || n != 240 {
		t.Fatalf("started Pull = (%d, %v), want (240, true)", n, started)
	}
	if dst[0] != 1 || dst[1] != -1 || dst[239*2] != 240 || dst[239*2+1] != -240 {
		t.Fatalf("unexpected first packet samples: start=(%d,%d) end=(%d,%d)",
			dst[0], dst[1], dst[239*2], dst[239*2+1])
	}
	if rb.BufferedFrames() != 240 {
		t.Fatalf("BufferedFrames = %d, want 240", rb.BufferedFrames())
	}
	if rb.FramesIn() != 480 || rb.FramesOut() != 240 {
		t.Fatalf("FramesIn/Out = %d/%d, want 480/240", rb.FramesIn(), rb.FramesOut())
	}
}

func TestStereoRingBufferUnderrunFade(t *testing.T) {
	rb := NewStereoRingBuffer(500, 8, 500)

	// Push 10 frames with exact multiples of 16 so fade steps are exact integers.
	in := make([]int16, 10*2)
	for i := 0; i < 10; i++ {
		in[i*2] = 16000
		in[i*2+1] = -8000
	}
	rb.Push(in)

	// Pull 24 frames: 10 real frames + 14 fade-out frames (15/16 down to 2/16).
	dst1 := make([]int16, 24*2)
	n, started := rb.Pull(dst1)
	if !started || n != 10 {
		t.Fatalf("Pull 1 = (%d, %v), want (10, true)", n, started)
	}
	if rb.Underruns() != 1 {
		t.Fatalf("Underruns = %d, want 1", rb.Underruns())
	}
	for i := 0; i < 10; i++ {
		if dst1[i*2] != 16000 || dst1[i*2+1] != -8000 {
			t.Fatalf("real frame %d = (%d,%d), want (16000,-8000)", i, dst1[i*2], dst1[i*2+1])
		}
	}
	for step := 0; step < 14; step++ {
		f := 10 + step
		wantL := int16(16000 * (15 - step) / 16)
		wantR := int16(-8000 * (15 - step) / 16)
		if dst1[f*2] != wantL || dst1[f*2+1] != wantR {
			t.Fatalf("fade step %d (frame %d) = (%d,%d), want (%d,%d)",
				step, f, dst1[f*2], dst1[f*2+1], wantL, wantR)
		}
	}

	// Next pull of 24 frames finishes the remaining 2 fade steps (1/16, 0/16) then zeros.
	dst2 := make([]int16, 24*2)
	n, started = rb.Pull(dst2)
	if !started || n != 0 {
		t.Fatalf("Pull 2 = (%d, %v), want (0, true)", n, started)
	}
	if rb.Underruns() != 2 {
		t.Fatalf("Underruns = %d, want 2", rb.Underruns())
	}
	if dst2[0] != 1000 || dst2[1] != -500 {
		t.Fatalf("fade step 14 = (%d,%d), want (1000,-500)", dst2[0], dst2[1])
	}
	if dst2[2] != 0 || dst2[3] != 0 {
		t.Fatalf("fade step 15 = (%d,%d), want (0,0)", dst2[2], dst2[3])
	}
	for i := 2; i < 24; i++ {
		if dst2[i*2] != 0 || dst2[i*2+1] != 0 {
			t.Fatalf("post-fade frame %d = (%d,%d), want (0,0)", i, dst2[i*2], dst2[i*2+1])
		}
	}

	// Subsequent empty pull must stay silent and not replay stale samples.
	dst3 := make([]int16, 24*2)
	rb.Pull(dst3)
	for i := 0; i < 24; i++ {
		if dst3[i*2] != 0 || dst3[i*2+1] != 0 {
			t.Fatalf("steady silence frame %d = (%d,%d), want (0,0)", i, dst3[i*2], dst3[i*2+1])
		}
	}

	// Also verify transition from a full pull directly to an empty pull triggers fade.
	rb.Push(in[:8*2])
	full := make([]int16, 8*2)
	n, started = rb.Pull(full)
	if !started || n != 8 {
		t.Fatalf("recovery Pull = (%d, %v), want (8, true)", n, started)
	}
	emptyAfterFull := make([]int16, 20*2)
	n, _ = rb.Pull(emptyAfterFull)
	if n != 0 {
		t.Fatalf("emptyAfterFull realFrames = %d, want 0", n)
	}
	if emptyAfterFull[0] != 15000 || emptyAfterFull[1] != -7500 {
		t.Fatalf("full->empty fade frame 0 = (%d,%d), want (15000,-7500)",
			emptyAfterFull[0], emptyAfterFull[1])
	}
	if emptyAfterFull[15*2] != 0 || emptyAfterFull[15*2+1] != 0 {
		t.Fatalf("full->empty fade frame 15 = (%d,%d), want (0,0)",
			emptyAfterFull[15*2], emptyAfterFull[15*2+1])
	}
}

func TestStereoRingBufferOverflowClamping(t *testing.T) {
	// 2.0s capacity = 24000, prefill = 480 (40ms), maxLag = 3000 (250ms).
	rb := NewStereoRingBuffer(24000, 480, 3000)

	// Push 15 * 240 = 3600 frames -> should clamp to newest 3000 frames (dropping oldest 600).
	for pkt := 0; pkt < 15; pkt++ {
		frame := make([]int16, 240*2)
		for i := 0; i < 240; i++ {
			id := int16(pkt*240 + i + 1)
			frame[i*2] = id
			frame[i*2+1] = -id
		}
		rb.Push(frame)
	}

	if rb.BufferedFrames() != 3000 {
		t.Fatalf("BufferedFrames = %d, want 3000", rb.BufferedFrames())
	}
	if rb.DroppedFrames() != 600 {
		t.Fatalf("DroppedFrames = %d, want 600", rb.DroppedFrames())
	}

	out := make([]int16, 240*2)
	n, started := rb.Pull(out)
	if !started || n != 240 {
		t.Fatalf("Pull = (%d, %v), want (240, true)", n, started)
	}
	if out[0] != 601 || out[1] != -601 {
		t.Fatalf("first surviving sample = (%d,%d), want (601,-601)", out[0], out[1])
	}

	// Push single slice larger than capFrames.
	small := NewStereoRingBuffer(100, 10, 100)
	giant := make([]int16, 150*2)
	for i := 0; i < 150; i++ {
		giant[i*2] = int16(i + 1)
		giant[i*2+1] = int16(-(i + 1))
	}
	small.Push(giant)
	if small.BufferedFrames() != 100 || small.DroppedFrames() != 50 {
		t.Fatalf("giant push: buffered=%d dropped=%d, want 100/50",
			small.BufferedFrames(), small.DroppedFrames())
	}
	smallOut := make([]int16, 100*2)
	n, _ = small.Pull(smallOut)
	if n != 100 || smallOut[0] != 51 || smallOut[1] != -51 {
		t.Fatalf("giant pull: n=%d first=(%d,%d), want 100/(51,-51)", n, smallOut[0], smallOut[1])
	}
}

func TestStereoRingBufferConcurrentRace(t *testing.T) {
	rb := NewStereoRingBuffer(24000, 480, 3000)

	const totalPackets = 200
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		pkt := make([]int16, 240*2)
		for p := 1; p <= totalPackets; p++ {
			for i := 0; i < 240; i++ {
				val := int16((p*240 + i) % 20000)
				pkt[i*2] = val
				pkt[i*2+1] = -val
			}
			rb.Push(pkt)
			if p%10 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	go func() {
		defer wg.Done()
		dst := make([]int16, 240*2)
		for i := 0; i < 250; i++ {
			n, _ := rb.Pull(dst)
			for f := 0; f < n; f++ {
				if dst[f*2] != -dst[f*2+1] {
					t.Errorf("stereo invariant violated at pull %d frame %d: L=%d R=%d",
						i, f, dst[f*2], dst[f*2+1])
					return
				}
			}
			_ = rb.BufferedFrames()
			_ = rb.Underruns()
			time.Sleep(500 * time.Microsecond)
		}
	}()

	wg.Wait()
	if rb.FramesIn() != totalPackets*240 {
		t.Fatalf("FramesIn = %d, want %d", rb.FramesIn(), totalPackets*240)
	}
}

func TestMockEngineRoundtripAndClose(t *testing.T) {
	eng := NewMockEngine(240)

	rx1 := make([]int16, 240*2)
	for i := 0; i < 240; i++ {
		rx1[i*2] = int16(i + 10)
		rx1[i*2+1] = int16(-i - 10)
	}
	eng.PushRxStereo(rx1)

	st := eng.Stats()
	if st.RxFramesIn != 240 || st.RxFramesOut != 0 || st.BufferFillMs != 20.0 {
		t.Fatalf("unexpected stats after push: %+v", st)
	}
	if st.RxBackend != "mock" || st.TxBackend != "mock" {
		t.Fatalf("unexpected mock backends: %+v", st)
	}

	half := eng.DrainRx(100)
	if len(half) != 200 || half[0] != 10 || half[1] != -10 {
		t.Fatalf("DrainRx(100) len=%d first=(%d,%d)", len(half), half[0], half[1])
	}
	rest := eng.DrainRx(0)
	if len(rest) != 140*2 || rest[0] != 110 || rest[1] != -110 {
		t.Fatalf("DrainRx(0) len=%d first=(%d,%d)", len(rest), rest[0], rest[1])
	}
	if eng.DrainRx(0) != nil {
		t.Fatal("expected nil on empty DrainRx")
	}
	st = eng.Stats()
	if st.RxFramesOut != 240 || st.BufferFillMs != 0 {
		t.Fatalf("unexpected stats after drain: %+v", st)
	}

	// Queue TX in partial chunks summing to 240 frames.
	txChunkA := make([]int16, 100*2)
	txChunkB := make([]int16, 140*2)
	for i := 0; i < 100; i++ {
		txChunkA[i*2] = int16(1000 + i)
		txChunkA[i*2+1] = int16(2000 + i)
	}
	for i := 0; i < 140; i++ {
		txChunkB[i*2] = int16(3000 + i)
		txChunkB[i*2+1] = int16(4000 + i)
	}
	eng.QueueTx(txChunkA)
	eng.QueueTx(txChunkB)

	smallDst := make([]int16, 10)
	if _, err := eng.ReadTxStereo(smallDst); !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("expected io.ErrShortBuffer, got %v", err)
	}

	txOut := make([]int16, 240*2)
	n, err := eng.ReadTxStereo(txOut)
	if err != nil || n != 240 {
		t.Fatalf("ReadTxStereo = (%d, %v), want (240, nil)", n, err)
	}
	if txOut[0] != 1000 || txOut[1] != 2000 || txOut[100*2] != 3000 || txOut[100*2+1] != 4000 {
		t.Fatalf("unexpected TX boundary samples: [0]=(%d,%d) [100]=(%d,%d)",
			txOut[0], txOut[1], txOut[100*2], txOut[100*2+1])
	}
	if eng.Stats().TxFramesIn != 240 {
		t.Fatalf("TxFramesIn = %d, want 240", eng.Stats().TxFramesIn)
	}

	// Concurrent ReadTxStereo must block until Close(), then immediately return io.EOF.
	errCh := make(chan error, 1)
	go func() {
		_, readErr := eng.ReadTxStereo(txOut)
		errCh <- readErr
	}()

	time.Sleep(15 * time.Millisecond)
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case readErr := <-errCh:
		if !errors.Is(readErr, io.EOF) {
			t.Fatalf("blocked ReadTxStereo returned %v, want io.EOF", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("ReadTxStereo did not unblock after Close()")
	}

	if _, err := eng.ReadTxStereo(txOut); !errors.Is(err, io.EOF) {
		t.Fatalf("post-close ReadTxStereo returned %v, want io.EOF", err)
	}
}

func TestPwCatBackendLifecycleWithStubBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell stub test requires Unix")
	}

	tmpDir := t.TempDir()
	rxDump := filepath.Join(tmpDir, "rx.raw")

	// Prepare 1 deterministic 240-frame LE stereo payload: L=1234, R=-5678.
	txPattern := make([]byte, 240*4)
	lVal, rVal := int16(1234), int16(-5678)
	for i := 0; i < 240; i++ {
		binary.LittleEndian.PutUint16(txPattern[i*4:i*4+2], uint16(lVal))
		binary.LittleEndian.PutUint16(txPattern[i*4+2:i*4+4], uint16(rVal))
	}
	txInput := filepath.Join(tmpDir, "tx.raw")
	if err := os.WriteFile(txInput, txPattern, 0o600); err != nil {
		t.Fatal(err)
	}

	// Stub pw-cat handles both --playback (saves stdin to rx.raw) and --record
	// (emits tx.raw once, then blocks on sleep until killed by Close).
	stubScript := `#!/bin/sh
mode=""
for arg in "$@"; do
  case "$arg" in
    --playback) mode="playback" ;;
    --record) mode="record" ;;
  esac
done
if [ "$mode" = "playback" ]; then
  exec cat > "` + rxDump + `"
elif [ "$mode" = "record" ]; then
  cat "` + txInput + `"
  exec sleep 30
fi
exit 1
`
	pwCatPath := filepath.Join(tmpDir, "pw-cat")
	if err := os.WriteFile(pwCatPath, []byte(stubScript), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("PATH", tmpDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	eng, err := Open(context.Background(), Config{
		RxTarget:       "k4_rx_test",
		TxTarget:       "k4_tx_test",
		SampleRate:     12000,
		FrameSamples:   240,
		TargetBufferMs: 20,
		Backend:        "pw-cat",
	})
	if err != nil {
		t.Fatalf("Open pw-cat stub: %v", err)
	}

	// Push 240 frames (reaches 20ms prefill) and allow ticker ticks to flush to rxDump.
	rxFrame := make([]int16, 240*2)
	for i := 0; i < 240; i++ {
		rxFrame[i*2] = int16(500 + i)
		rxFrame[i*2+1] = int16(-500 - i)
	}
	eng.PushRxStereo(rxFrame)

	txDst := make([]int16, 240*2)
	n, err := eng.ReadTxStereo(txDst)
	if err != nil || n != 240 {
		t.Fatalf("ReadTxStereo = (%d, %v), want (240, nil)", n, err)
	}
	if txDst[0] != 1234 || txDst[1] != -5678 || txDst[239*2] != 1234 || txDst[239*2+1] != -5678 {
		t.Fatalf("unexpected decoded TX stereo samples: first=(%d,%d) last=(%d,%d)",
			txDst[0], txDst[1], txDst[239*2], txDst[239*2+1])
	}

	// Wait until the 20ms ticker drains rxFrame out of the ring buffer.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if eng.Stats().RxFramesOut >= 240 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := eng.Stats()
	if st.RxBackend != "pw-cat" || st.TxBackend != "pw-cat" {
		t.Fatalf("unexpected backends: %+v", st)
	}
	if st.RxFramesOut < 240 || st.TxFramesIn != 240 {
		t.Fatalf("unexpected frame counters: %+v", st)
	}

	// Second ReadTxStereo blocks on sleep 30; Close must unblock it and reap both subprocesses.
	errCh := make(chan error, 1)
	go func() {
		_, readErr := eng.ReadTxStereo(txDst)
		errCh <- readErr
	}()
	time.Sleep(15 * time.Millisecond)

	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case readErr := <-errCh:
		if !errors.Is(readErr, io.EOF) {
			t.Fatalf("blocked ReadTxStereo after Close = %v, want io.EOF", readErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadTxStereo failed to unblock on Close()")
	}

	// Verify the RX samples written into pw-cat stdin contain our non-zero stereo pattern.
	dumped, err := os.ReadFile(rxDump)
	if err != nil {
		t.Fatalf("read rxDump: %v", err)
	}
	if len(dumped) < 240*4 {
		t.Fatalf("rxDump too small: %d bytes", len(dumped))
	}
	found := false
	for off := 0; off+3 < len(dumped); off += 4 {
		l := int16(binary.LittleEndian.Uint16(dumped[off : off+2]))
		r := int16(binary.LittleEndian.Uint16(dumped[off+2 : off+4]))
		if l == 500 && r == -500 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected stereo sample (500, -500) in rxDump (%d bytes)", len(dumped))
	}
}

func TestLivePipeWireBackendsIntegration(t *testing.T) {
	if _, err := exec.LookPath("pw-cli"); err != nil {
		t.Skipf("pw-cli not installed: %v", err)
	}
	if _, err := exec.LookPath("pw-cat"); err != nil {
		t.Skipf("pw-cat not installed: %v", err)
	}
	if err := exec.Command("pw-cli", "info", "0").Run(); err != nil {
		t.Skipf("PipeWire daemon not reachable: %v", err)
	}

	pid := os.Getpid()
	rxName := fmt.Sprintf("k4_rx_audiotest_%d", pid)
	txName := fmt.Sprintf("k4_tx_audiotest_%d", pid)

	cmd := exec.Command("pw-cli")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("pw-cli stdin pipe: %v", err)
	}
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start pw-cli: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()
	t.Cleanup(func() {
		_, _ = io.WriteString(stdin, "quit\n")
		_ = stdin.Close()
		select {
		case <-waitDone:
		case <-time.After(time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-waitDone
		}
	})

	createCmds := fmt.Sprintf(
		"create-node adapter { factory.name=support.null-audio-sink node.name=%q node.description=%q media.class=\"Audio/Source/Virtual\" audio.position=\"FL,FR\" monitor.passthrough=true node.pause-on-idle=false }\n"+
			"create-node adapter { factory.name=support.null-audio-sink node.name=%q node.description=%q media.class=\"Audio/Sink\" audio.position=\"FL,FR\" priority.driver=100 priority.session=100 node.pause-on-idle=false }\n",
		rxName, "K4 Audio Test RX",
		txName, "K4 Audio Test TX",
	)
	if _, err := io.WriteString(stdin, createCmds); err != nil {
		t.Fatalf("write pw-cli commands: %v", err)
	}

	readyDeadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(readyDeadline) {
		raw, err := exec.Command("pw-cli", "ls", "Node").Output()
		if err == nil {
			txt := string(raw)
			if strings.Contains(txt, rxName) && strings.Contains(txt, txName) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	raw, err := exec.Command("pw-cli", "ls", "Node").Output()
	if err != nil || !strings.Contains(string(raw), rxName) || !strings.Contains(string(raw), txName) {
		t.Fatalf("ephemeral nodes did not appear (err=%v, pw-cli=%q)", err, outBuf.String())
	}

	for _, backend := range []string{"auto", "pw-cat"} {
		t.Run(backend, func(t *testing.T) {
			eng, err := Open(context.Background(), Config{
				RxTarget:       rxName,
				TxTarget:       txName,
				SampleRate:     12000,
				FrameSamples:   240,
				TargetBufferMs: 40,
				Backend:        backend,
			})
			if err != nil {
				t.Fatalf("Open(%s): %v", backend, err)
			}
			defer func() { _ = eng.Close() }()

			pkt := make([]int16, 240*2)
			for i := 0; i < 240; i++ {
				pkt[i*2] = int16(i * 10)
				pkt[i*2+1] = int16(-i * 10)
			}
			eng.PushRxStereo(pkt)
			eng.PushRxStereo(pkt)

			type readRes struct {
				frames int
				err    error
			}
			ch := make(chan readRes, 1)
			dst := make([]int16, 240*2)
			go func() {
				n, rerr := eng.ReadTxStereo(dst)
				ch <- readRes{frames: n, err: rerr}
			}()

			select {
			case res := <-ch:
				if res.err != nil || res.frames != 240 {
					t.Fatalf("ReadTxStereo(%s) = (%d, %v), want (240, nil)", backend, res.frames, res.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for ReadTxStereo(%s)", backend)
			}

			// Allow at least a couple of 20ms playback ticks to pull prefilled frames.
			time.Sleep(80 * time.Millisecond)
			st := eng.Stats()
			if st.RxBackend != "pw-cat" {
				t.Fatalf("RxBackend = %q, want pw-cat", st.RxBackend)
			}
			if backend == "pw-cat" && st.TxBackend != "pw-cat" {
				t.Fatalf("TxBackend = %q, want pw-cat", st.TxBackend)
			}
			if st.TxFramesIn != 240 || st.RxFramesIn != 480 || st.RxFramesOut == 0 {
				t.Fatalf("unexpected live stats (%s): %+v", backend, st)
			}

			if err := eng.Close(); err != nil {
				t.Fatalf("Close(%s): %v", backend, err)
			}
		})
	}
}
