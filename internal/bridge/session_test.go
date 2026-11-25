package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k4-audio/internal/audio"
	"k4-audio/internal/k4proto"
)

type receivedPkt struct {
	at      time.Time
	payload []byte
}

type fakeK4Conn struct {
	conn     net.Conn
	authHex  string
	authAt   time.Time
	pkts     chan receivedPkt
	writeMu  sync.Mutex
	closedCh chan struct{}
}

func (fc *fakeK4Conn) sendBytes(b []byte) error {
	fc.writeMu.Lock()
	defer fc.writeMu.Unlock()
	_ = fc.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	for len(b) > 0 {
		n, err := fc.conn.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func (fc *fakeK4Conn) sendCAT(cat string) error {
	return fc.sendBytes(k4proto.EncodeCAT(cat))
}

func (fc *fakeK4Conn) sendAudio(seq uint8, mode uint8, frames uint16, audioBytes []byte) error {
	return fc.sendBytes(k4proto.EncodeAudioPacket(seq, mode, frames, audioBytes))
}

func (fc *fakeK4Conn) sendRawPayload(payload []byte) error {
	return fc.sendBytes(k4proto.EncodeFrame(payload))
}

func (fc *fakeK4Conn) close() {
	_ = fc.conn.Close()
}

type fakeK4Server struct {
	ln     net.Listener
	host   string
	port   int
	conns  chan *fakeK4Conn
	errCh  chan error
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newFakeK4Server(t *testing.T) *fakeK4Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.1:0: %v", err)
	}
	tcpAddr := ln.Addr().(*net.TCPAddr)
	s := &fakeK4Server{
		ln:     ln,
		host:   tcpAddr.IP.String(),
		port:   tcpAddr.Port,
		conns:  make(chan *fakeK4Conn, 8),
		errCh:  make(chan error, 8),
		stopCh: make(chan struct{}),
	}

	s.wg.Add(1)
	go s.acceptLoop()
	t.Cleanup(s.close)
	return s
}

func (s *fakeK4Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				select {
				case s.errCh <- err:
				default:
				}
				return
			}
		}

		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			s.serveConn(conn)
		}(c)
	}
}

func (s *fakeK4Server) serveConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var authBuf [96]byte
	if _, err := io.ReadFull(conn, authBuf[:]); err != nil {
		select {
		case <-s.stopCh:
		default:
			select {
			case s.errCh <- err:
			default:
			}
		}
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	fc := &fakeK4Conn{
		conn:     conn,
		authHex:  string(authBuf[:]),
		authAt:   time.Now(),
		pkts:     make(chan receivedPkt, 256),
		closedCh: make(chan struct{}),
	}
	defer close(fc.closedCh)

	select {
	case s.conns <- fc:
	case <-s.stopCh:
		return
	}

	framer := &k4proto.StreamFramer{}
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			now := time.Now()
			for _, p := range framer.Push(buf[:n]) {
				select {
				case fc.pkts <- receivedPkt{at: now, payload: p}:
				case <-s.stopCh:
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *fakeK4Server) acceptConn(t *testing.T, timeout time.Duration) *fakeK4Conn {
	t.Helper()
	select {
	case fc := <-s.conns:
		return fc
	case err := <-s.errCh:
		t.Fatalf("fakeK4Server error: %v", err)
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for fakeK4Server connection")
	}
	return nil
}

func (s *fakeK4Server) close() {
	select {
	case <-s.stopCh:
		return
	default:
		close(s.stopCh)
	}
	_ = s.ln.Close()
	s.wg.Wait()
}

func waitForCATPacket(t *testing.T, fc *fakeK4Conn, timeout time.Duration) (receivedPkt, string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case pkt := <-fc.pkts:
			if cat, ok := k4proto.ParseCATPayload(pkt.payload); ok {
				return pkt, cat
			}
		case <-deadline:
			t.Fatalf("timed out waiting for CAT packet")
		}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for condition: %s", desc)
}

func makeStereoFrame(frames int, lVal, rVal int16) []int16 {
	out := make([]int16, frames*2)
	for i := 0; i < frames; i++ {
		out[i*2] = lVal
		out[i*2+1] = rVal
	}
	return out
}

func TestSessionAuthAndDeferredBootstrapTimer(t *testing.T) {
	srv := newFakeK4Server(t)
	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host:     srv.host,
		Port:     srv.port,
		Password: "my-secret-password",
	}, eng, log.New(io.Discard, "", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- sess.Run(ctx)
	}()

	fc := srv.acceptConn(t, 2*time.Second)
	wantAuth := k4proto.ExpectedAuthHex("my-secret-password")
	if fc.authHex != wantAuth || len(fc.authHex) != 96 {
		t.Fatalf("authHex = %q (len %d), want %q", fc.authHex, len(fc.authHex), wantAuth)
	}

	// Server stays quiet: bootstrap must wait ~250ms and never send #FRZ or #SPN.
	pkt, cat := waitForCATPacket(t, fc, 2*time.Second)
	elapsed := pkt.at.Sub(fc.authAt)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("bootstrap arrived too early (%v < 200ms) without incoming packet", elapsed)
	}
	wantBootstrap := "RDY;K41;ER1;AI5;EM1;SL0;TQ;"
	if cat != wantBootstrap {
		t.Fatalf("bootstrap CAT = %q, want %q", cat, wantBootstrap)
	}
	if strings.Contains(cat, "#FRZ") || strings.Contains(cat, "#SPN") {
		t.Fatalf("bootstrap unexpectedly emitted panadapter commands: %q", cat)
	}

	waitUntil(t, time.Second, "Connected true", func() bool { return sess.Stats().Connected })

	cancel()
	_, shutdownCAT := waitForCATPacket(t, fc, 2*time.Second)
	if shutdownCAT != "RRN;" {
		t.Fatalf("shutdown CAT = %q, want %q", shutdownCAT, "RRN;")
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() returned unexpected error on clean cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run() did not exit after cancel")
	}

	if sess.Stats().Connected {
		t.Fatalf("expected Connected=false after Run exit")
	}
}

func TestSessionDeferredBootstrapOnFirstPacketAndStereoRxAndPanIgnore(t *testing.T) {
	srv := newFakeK4Server(t)
	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host: srv.host,
		Port: srv.port,
	}, eng, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- sess.Run(ctx)
	}()

	fc := srv.acceptConn(t, 2*time.Second)

	// Send first valid packet immediately (CAT) -> should trigger bootstrap before 250ms.
	if err := fc.sendCAT("FA00014074000;"); err != nil {
		t.Fatalf("sendCAT: %v", err)
	}

	pkt, cat := waitForCATPacket(t, fc, time.Second)
	if pkt.at.Sub(fc.authAt) >= 220*time.Millisecond {
		t.Fatalf("expected early bootstrap on first packet, took %v", pkt.at.Sub(fc.authAt))
	}
	if cat != "RDY;K41;ER1;AI5;EM1;SL0;TQ;" {
		t.Fatalf("bootstrap CAT = %q", cat)
	}

	// Send TypePAN (0x02) and TypeMiniPAN (0x03) -> must be ignored cleanly.
	panPayload := append([]byte{k4proto.TypePAN}, bytes.Repeat([]byte{0xAA}, 512)...)
	miniPanPayload := append([]byte{k4proto.TypeMiniPAN}, bytes.Repeat([]byte{0x55}, 128)...)
	if err := fc.sendRawPayload(panPayload); err != nil {
		t.Fatalf("send TypePAN: %v", err)
	}
	if err := fc.sendRawPayload(miniPanPayload); err != nil {
		t.Fatalf("send TypeMiniPAN: %v", err)
	}

	// Send EM1 stereo packet with strictly distinct Left != Right (L=400, R=-300 -> x32 = 12800, -9600).
	em1R := int16(-300)
	em1Bytes := make([]byte, k4proto.FrameSamples20ms*4)
	for i := 0; i < k4proto.FrameSamples20ms; i++ {
		binary.LittleEndian.PutUint16(em1Bytes[i*4:i*4+2], uint16(400))
		binary.LittleEndian.PutUint16(em1Bytes[i*4+2:i*4+4], uint16(em1R))
	}
	if err := fc.sendAudio(1, k4proto.EncodeModePCM16, k4proto.FrameSamples20ms, em1Bytes); err != nil {
		t.Fatalf("send EM1 audio: %v", err)
	}

	// Send EM0 stereo packet (L=-200<<8, R=350<<8 -> x32 = -6400, 11200).
	em0L := int32(-200 << 8)
	em0Bytes := make([]byte, k4proto.FrameSamples20ms*8)
	for i := 0; i < k4proto.FrameSamples20ms; i++ {
		binary.LittleEndian.PutUint32(em0Bytes[i*8:i*8+4], uint32(em0L))
		binary.LittleEndian.PutUint32(em0Bytes[i*8+4:i*8+8], uint32(350<<8))
	}
	if err := fc.sendAudio(2, k4proto.EncodeModePCM24, k4proto.FrameSamples20ms, em0Bytes); err != nil {
		t.Fatalf("send EM0 audio: %v", err)
	}

	waitUntil(t, 2*time.Second, "2 RX audio packets decoded", func() bool {
		return sess.Stats().RxAudioPkts == 2 && eng.Stats().RxFramesIn == k4proto.FrameSamples20ms*2
	})

	rx := eng.DrainRx(0)
	if len(rx) != k4proto.FrameSamples20ms*4 {
		t.Fatalf("drained %d samples, want %d", len(rx), k4proto.FrameSamples20ms*4)
	}
	for i := 0; i < k4proto.FrameSamples20ms; i++ {
		l, r := rx[i*2], rx[i*2+1]
		if l != 12800 || r != -9600 {
			t.Fatalf("EM1 frame %d = (%d, %d), want (12800, -9600)", i, l, r)
		}
	}
	off := k4proto.FrameSamples20ms * 2
	for i := 0; i < k4proto.FrameSamples20ms; i++ {
		l, r := rx[off+i*2], rx[off+i*2+1]
		if l != -6400 || r != 11200 {
			t.Fatalf("EM0 frame %d = (%d, %d), want (-6400, 11200)", i, l, r)
		}
	}

	cancel()
	<-runDone
}

func TestSessionTxGatingHoldTailCatOnAudioAndK4TxActive(t *testing.T) {
	srv := newFakeK4Server(t)
	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host:             srv.host,
		Port:             srv.port,
		SilenceThreshold: 2,
		TxHoldTail:       80 * time.Millisecond,
		TxCatOnAudio:     true,
	}, eng, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- sess.Run(ctx)
	}()

	fc := srv.acceptConn(t, 2*time.Second)
	if err := fc.sendCAT("AI5;"); err != nil {
		t.Fatalf("sendCAT: %v", err)
	}
	_, boot := waitForCATPacket(t, fc, time.Second)
	if boot != "RDY;K41;ER1;AI5;EM1;SL0;TQ;" {
		t.Fatalf("bootstrap = %q", boot)
	}

	// 1. Sub-threshold frame (peak=1 < 2) must be gated and emit nothing.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 1, 0))
	waitUntil(t, time.Second, "eng read sub-threshold frame", func() bool {
		return eng.Stats().TxFramesIn == k4proto.FrameSamples20ms
	})
	select {
	case pkt := <-fc.pkts:
		t.Fatalf("unexpected packet sent for silent frame: %x", pkt.payload)
	case <-time.After(40 * time.Millisecond):
	}
	if sess.Stats().TxAudioPkts != 0 {
		t.Fatalf("TxAudioPkts = %d, want 0", sess.Stats().TxAudioPkts)
	}

	// 2. Active frame (peak=3200 >= 2) with TxCatOnAudio -> sends TX; then TypeAudio pkt 0.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 3200, 0))
	_, txCmd := waitForCATPacket(t, fc, time.Second)
	if txCmd != "TX;" {
		t.Fatalf("expected TX; before audio, got %q", txCmd)
	}
	var audioPkt receivedPkt
	select {
	case audioPkt = <-fc.pkts:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for active TX audio packet")
	}
	hdr, data, ok := k4proto.ParseAudioPayload(audioPkt.payload)
	if !ok || hdr.EncodeMode != k4proto.EncodeModePCM16 || hdr.FrameSamplesPerCh != k4proto.FrameSamples20ms || len(data) != k4proto.FrameSamples20ms*4 {
		t.Fatalf("unexpected TX audio header/len: ok=%v hdr=%+v len=%d", ok, hdr, len(data))
	}
	// 3200 / 32 = 100 wire sample on both channels
	if sL := int16(binary.LittleEndian.Uint16(data[0:2])); sL != 100 {
		t.Fatalf("wire TX L = %d, want 100", sL)
	}

	// 3. Silent frame immediately within 80ms hold tail -> still transmitted.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 0, 0))
	select {
	case audioPkt = <-fc.pkts:
		hdr2, _, ok2 := k4proto.ParseAudioPayload(audioPkt.payload)
		if !ok2 || hdr2.Seq != 1 {
			t.Fatalf("expected hold-tail audio seq=1, got ok=%v hdr=%+v", ok2, hdr2)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for hold-tail TX audio packet")
	}

	// 4. Wait for hold tail (80ms) expiry -> RX; emitted automatically by hold timer.
	_, rxCmd := waitForCATPacket(t, fc, time.Second)
	if rxCmd != "RX;" {
		t.Fatalf("expected RX; on hold tail expiry, got %q", rxCmd)
	}

	// Silent frame after hold tail expiry must be gated again.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 0, 0))
	waitUntil(t, time.Second, "eng read gated frame", func() bool {
		return eng.Stats().TxFramesIn == k4proto.FrameSamples20ms*4
	})
	select {
	case pkt := <-fc.pkts:
		t.Fatalf("unexpected packet sent after hold tail expired: %x", pkt.payload)
	case <-time.After(40 * time.Millisecond):
	}

	// 5. K4TxActive tracking: TQ1;/TQ0;, TX;/RX;, and IF index 25 '1'/'0'.
	if err := fc.sendCAT("TQ1;"); err != nil {
		t.Fatalf("send TQ1: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive true via TQ1", func() bool { return sess.Stats().K4TxActive })

	// Silent frame while K4TxActive=true must be transmitted!
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 0, 0))
	select {
	case audioPkt = <-fc.pkts:
		hdr3, _, ok3 := k4proto.ParseAudioPayload(audioPkt.payload)
		if !ok3 || hdr3.Seq != 2 {
			t.Fatalf("expected TQ1-gated audio seq=2, got ok=%v hdr=%+v", ok3, hdr3)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for TQ1-gated TX audio packet")
	}

	if err := fc.sendCAT("TQ0;"); err != nil {
		t.Fatalf("send TQ0: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive false via TQ0", func() bool { return !sess.Stats().K4TxActive })

	// Verify TX;/RX; and IF index 25 '1'/'0'.
	if err := fc.sendCAT("TX;"); err != nil {
		t.Fatalf("send TX: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive true via TX;", func() bool { return sess.Stats().K4TxActive })
	if err := fc.sendCAT("RX;"); err != nil {
		t.Fatalf("send RX: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive false via RX;", func() bool { return !sess.Stats().K4TxActive })

	// Build real K4 hardware IF frames (space at [25], TX flag at [28]) as well as compact synthetic frames.
	ifHwTx := "IF00014074000     +000000 0016000001 ;"
	if err := fc.sendCAT(ifHwTx); err != nil {
		t.Fatalf("send real K4 IF TX: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive true via real K4 IF[28]", func() bool { return sess.Stats().K4TxActive })

	ifHwRx := "IF00014074000     +000000 0006000001 ;"
	if err := fc.sendCAT(ifHwRx); err != nil {
		t.Fatalf("send real K4 IF RX: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive false via real K4 IF[28]", func() bool { return !sess.Stats().K4TxActive })

	// Also verify compact synthetic IF frame with index 25 = '1', then '0'.
	ifTx := "IF00014074000     +00000010003000000;"
	if len(ifTx) <= 25 || ifTx[25] != '1' {
		t.Fatalf("test bug: invalid IF TX frame %q", ifTx)
	}
	if err := fc.sendCAT(ifTx); err != nil {
		t.Fatalf("send IF TX: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive true via IF[25]", func() bool { return sess.Stats().K4TxActive })

	ifRx := "IF00014074000     +00000000003000000;"
	if err := fc.sendCAT(ifRx); err != nil {
		t.Fatalf("send IF RX: %v", err)
	}
	waitUntil(t, time.Second, "K4TxActive false via IF[25]", func() bool { return !sess.Stats().K4TxActive })

	// Real K4 AI5 echo behavior: K4 echoes TX; when TxCatOnAudio keys TX, and replies DA0; (not RX;) on RX;.
	// Ensure unkeying immediately clears K4TxActive so silent frames do not keep streaming after RX;.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 3200, 0))
	_, txEchoTrig := waitForCATPacket(t, fc, time.Second)
	if txEchoTrig != "TX;" {
		t.Fatalf("expected TX;, got %q", txEchoTrig)
	}
	if err := fc.sendCAT("TX;"); err != nil {
		t.Fatalf("send echoed TX;: %v", err)
	}
	select {
	case <-fc.pkts:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for audio packet")
	}
	waitUntil(t, time.Second, "K4TxActive true after echoed TX;", func() bool { return sess.Stats().K4TxActive })

	_, rxAfterEcho := waitForCATPacket(t, fc, time.Second)
	if rxAfterEcho != "RX;" {
		t.Fatalf("expected RX; after hold tail, got %q", rxAfterEcho)
	}
	if err := fc.sendCAT("DA0;"); err != nil {
		t.Fatalf("send DA0;: %v", err)
	}
	if sess.Stats().K4TxActive {
		t.Fatalf("expected K4TxActive=false immediately after TxCatOnAudio RX;")
	}
	prevFramesIn := eng.Stats().TxFramesIn
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 0, 0))
	waitUntil(t, time.Second, "eng read silent frame after echoed TX;", func() bool {
		return eng.Stats().TxFramesIn > prevFramesIn
	})
	select {
	case pkt := <-fc.pkts:
		t.Fatalf("unexpected TX packet after echoed TX; + RX; unkey: %x", pkt.payload)
	case <-time.After(40 * time.Millisecond):
	}

	// 6. Trigger TxCatOnAudio again and cancel immediately before hold tail expires ->
	// shutdown must emit RX; followed by RRN;.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 1000, 1000))
	_, txCmd2 := waitForCATPacket(t, fc, time.Second)
	if txCmd2 != "TX;" {
		t.Fatalf("expected TX;, got %q", txCmd2)
	}
	select {
	case <-fc.pkts:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for audio packet before cancel")
	}

	cancel()
	_, end1 := waitForCATPacket(t, fc, time.Second)
	_, end2 := waitForCATPacket(t, fc, time.Second)
	if end1 != "RX;" || end2 != "RRN;" {
		t.Fatalf("shutdown sequence = (%q, %q), want (\"RX;\", \"RRN;\")", end1, end2)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run() did not exit")
	}
}

func TestSessionPingPongRTTAndReconnect(t *testing.T) {
	srv := newFakeK4Server(t)
	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host:        srv.host,
		Port:        srv.port,
		Reconnect:   true,
		DialTimeout: time.Second,
	}, eng, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- sess.Run(ctx)
	}()

	// First connection: trigger early bootstrap, wait for 1 Hz PING, reply PONG after 15ms.
	fc1 := srv.acceptConn(t, 2*time.Second)
	if err := fc1.sendCAT("FA00007074000;"); err != nil {
		t.Fatalf("sendCAT: %v", err)
	}
	_, boot1 := waitForCATPacket(t, fc1, time.Second)
	if boot1 != "RDY;K41;ER1;AI5;EM1;SL0;TQ;" {
		t.Fatalf("boot1 = %q", boot1)
	}

	_, pingCAT := waitForCATPacket(t, fc1, 2500*time.Millisecond)
	if !strings.HasPrefix(pingCAT, "PING") || !strings.HasSuffix(pingCAT, ";") {
		t.Fatalf("expected PING<unix>;, got %q", pingCAT)
	}
	arg := strings.TrimSuffix(strings.TrimPrefix(pingCAT, "PING"), ";")
	if _, err := strconv.ParseInt(arg, 10, 64); err != nil {
		t.Fatalf("PING argument %q is not an integer unix timestamp: %v", arg, err)
	}

	time.Sleep(15 * time.Millisecond)
	if err := fc1.sendCAT("PONG" + arg + ";"); err != nil {
		t.Fatalf("send PONG: %v", err)
	}
	waitUntil(t, time.Second, "RttMs >= 10", func() bool {
		return sess.Stats().RttMs >= 10
	})

	// Drop first connection from server -> session must reconnect.
	fc1.close()
	<-fc1.closedCh

	fc2 := srv.acceptConn(t, 3*time.Second)
	if err := fc2.sendCAT("FA00007074000;"); err != nil {
		t.Fatalf("sendCAT conn2: %v", err)
	}
	_, boot2 := waitForCATPacket(t, fc2, time.Second)
	if boot2 != "RDY;K41;ER1;AI5;EM1;SL0;TQ;" {
		t.Fatalf("boot2 = %q", boot2)
	}

	cancel()
	_, rrn := waitForCATPacket(t, fc2, time.Second)
	if rrn != "RRN;" {
		t.Fatalf("reconnect shutdown CAT = %q, want RRN;", rrn)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run() did not exit after cancel")
	}
}

func TestSessionNoReconnectReturnsDialError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	_ = ln.Close()

	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host:        addr.IP.String(),
		Port:        addr.Port,
		Reconnect:   false,
		DialTimeout: 500 * time.Millisecond,
	}, eng, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = sess.Run(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected immediate dial error when Reconnect=false, got %v", err)
	}
}

func TestSessionGainsLogLevelsEmptyPongAndBackoff(t *testing.T) {
	srv := newFakeK4Server(t)
	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	var logBuf bytes.Buffer
	sess := NewSession(Config{
		Host:             srv.host,
		Port:             srv.port,
		RxGainDb:         6.020599913279624,  // +6 dB (~2x)
		TxGainDb:         -6.020599913279624, // -6 dB (~0.5x)
		SilenceThreshold: 2,
		LogLevels:        true,
	}, eng, log.New(&logBuf, "", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- sess.Run(ctx)
	}()

	fc := srv.acceptConn(t, 2*time.Second)
	if err := fc.sendCAT("FA00014074000;"); err != nil {
		t.Fatalf("sendCAT: %v", err)
	}
	_, _ = waitForCATPacket(t, fc, time.Second)

	// RX +6 dB: wire 256 * 32 * 2 = 16384 (L) and -128 * 32 * 2 = -8192 (R).
	em1Bytes := make([]byte, k4proto.FrameSamples20ms*4)
	rVal := int16(-128)
	for i := 0; i < k4proto.FrameSamples20ms; i++ {
		binary.LittleEndian.PutUint16(em1Bytes[i*4:i*4+2], uint16(256))
		binary.LittleEndian.PutUint16(em1Bytes[i*4+2:i*4+4], uint16(rVal))
	}
	if err := fc.sendAudio(1, k4proto.EncodeModePCM16, k4proto.FrameSamples20ms, em1Bytes); err != nil {
		t.Fatalf("sendAudio: %v", err)
	}
	waitUntil(t, time.Second, "RX frame with gain decoded", func() bool {
		return eng.Stats().RxFramesIn == k4proto.FrameSamples20ms
	})
	rx := eng.DrainRx(0)
	if rx[0] != 16384 || rx[1] != -8192 {
		t.Fatalf("RX with +6dB gain = (%d, %d), want (16384, -8192)", rx[0], rx[1])
	}

	// TX -6 dB: input 16384 -> *0.5 / 32 = 256 on wire.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 16384, 0))
	var txPkt receivedPkt
	select {
	case txPkt = <-fc.pkts:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for TX packet")
	}
	_, txData, ok := k4proto.ParseAudioPayload(txPkt.payload)
	if !ok || len(txData) < 4 {
		t.Fatalf("invalid TX audio packet")
	}
	if gotL := int16(binary.LittleEndian.Uint16(txData[0:2])); gotL != 256 {
		t.Fatalf("TX wire sample with -6dB gain = %d, want 256", gotL)
	}

	// Verify 1 Hz PING followed by bare PONG; (without echoed timestamp) still updates RTT.
	_, pingCAT := waitForCATPacket(t, fc, 2500*time.Millisecond)
	if !strings.HasPrefix(pingCAT, "PING") {
		t.Fatalf("expected PING, got %q", pingCAT)
	}
	time.Sleep(12 * time.Millisecond)
	if err := fc.sendCAT("PONG;"); err != nil {
		t.Fatalf("send bare PONG;: %v", err)
	}
	waitUntil(t, time.Second, "RttMs >= 10 on bare PONG;", func() bool {
		return sess.Stats().RttMs >= 10
	})

	cancel()
	<-runDone

	if !strings.Contains(logBuf.String(), "tx levels:") {
		t.Fatalf("expected LogLevels output in logger, got %q", logBuf.String())
	}

	// Verify bounded exponential backoff 0.5s .. 10s.
	b := minBackoff
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for i, w := range want {
		b = nextBackoff(b)
		if b != w {
			t.Fatalf("backoff step %d = %v, want %v", i, b, w)
		}
	}
}

func TestCATProxyMultiplexesAllCATOver9205(t *testing.T) {
	srv9205 := newFakeK4Server(t)

	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host:           srv9205.host,
		Port:           srv9205.port,
		PassthroughCAT: true,
	}, eng, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- sess.Run(ctx) }()

	fc := srv9205.acceptConn(t, 2*time.Second)
	// Trigger immediate passthrough bootstrap (ER1;AI0;K40;EM1;SL0;) via an audio frame.
	if err := fc.sendAudio(0, k4proto.EncodeModePCM16, k4proto.FrameSamples20ms, make([]byte, k4proto.FrameSamples20ms*4)); err != nil {
		t.Fatalf("sendAudio: %v", err)
	}
	_, boot := waitForCATPacket(t, fc, time.Second)
	if boot != "ER1;AI0;K40;EM1;SL0;" {
		t.Fatalf("passthrough bootstrap = %q, want %q", boot, "ER1;AI0;K40;EM1;SL0;")
	}

	proxy, err := StartCATProxy(ctx, CATProxyConfig{
		ListenAddr: "127.0.0.1:0",
	}, sess, nil)
	if err != nil {
		t.Fatalf("StartCATProxy: %v", err)
	}
	defer proxy.Close()

	client, err := net.DialTimeout("tcp", proxy.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()

	// Before TX;, loud pre-PTT audio on k4_tx must NOT emit unkeyed 0x01 packets.
	prevPreTx := eng.Stats().TxFramesIn
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 30000, 30000))
	waitUntil(t, time.Second, "pre-TX frame consumed", func() bool {
		return eng.Stats().TxFramesIn > prevPreTx
	})
	select {
	case pkt := <-fc.pkts:
		if _, _, isAudio := k4proto.ParseAudioPayload(pkt.payload); isAudio {
			t.Fatalf("unexpected pre-TX unkeyed 0x01 packet: %x", pkt.payload)
		}
	case <-time.After(30 * time.Millisecond):
	}

	// Send Hamlib sequence "ID;IF;TX;TQ;" -> ALL commands arrive on :9205!
	if _, err := client.Write([]byte("ID;IF;TX;TQ;")); err != nil {
		t.Fatalf("write proxy: %v", err)
	}
	var got9205 []string
	for len(got9205) < 4 {
		_, c := waitForCATPacket(t, fc, time.Second)
		if strings.HasPrefix(c, "PING") {
			continue
		}
		got9205 = append(got9205, c)
	}
	want9205 := []string{"ID;", "IF;", "TX;", "TQ;"}
	for i := range want9205 {
		if got9205[i] != want9205[i] {
			t.Fatalf("9205 cmd[%d] = %q, want %q", i, got9205[i], want9205[i])
		}
	}

	// K4 :9205 replies with 38-byte IF (preserving trailing space!), PONG; (filtered), DAPM (filtered), and TQ1;.
	wantIF := "IF00014074000     +000000 0016000001 ;"
	if err := fc.sendCAT("ID017;" + wantIF + "PONG;DAPM0001800000011;TQ1;"); err != nil {
		t.Fatalf("sendCAT reply: %v", err)
	}

	replyBuf := make([]byte, 256)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	var gotReply string
	wantReply := "ID017;" + wantIF + "TQ1;"
	for len(gotReply) < len(wantReply) {
		n, rerr := client.Read(replyBuf)
		if n > 0 {
			gotReply += string(replyBuf[:n])
		}
		if rerr != nil {
			t.Fatalf("read proxy reply (%q): %v", gotReply, rerr)
		}
	}
	if gotReply != wantReply {
		t.Fatalf("proxy reply = %q, want %q", gotReply, wantReply)
	}

	// While TX; is active, active audio frames are sent as 0x01 packets.
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 12000, 12000))
	select {
	case pkt := <-fc.pkts:
		if _, _, isAudio := k4proto.ParseAudioPayload(pkt.payload); !isAudio {
			t.Fatalf("expected 0x01 audio packet during active TX;, got %x", pkt.payload)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for 0x01 audio packet during active TX;")
	}

	// Send RX; -> emits RX; on :9205, filters DA0;, and permanently blocks any
	// post-RX buffered PulseAudio tail frames (even >300ms later) until the next TX;.
	if _, err := client.Write([]byte("RX;")); err != nil {
		t.Fatalf("write RX;: %v", err)
	}
	for {
		_, c := waitForCATPacket(t, fc, time.Second)
		if strings.HasPrefix(c, "PING") {
			continue
		}
		if c != "RX;" {
			t.Fatalf("expected RX; on :9205, got %q", c)
		}
		break
	}
	if err := fc.sendCAT("DA0;"); err != nil {
		t.Fatalf("send DA0;: %v", err)
	}

	// Even if K4 briefly reports TQ1; before settling, proxy RX; keeps 0x01 strictly off.
	if err := fc.sendCAT("TQ1;"); err != nil {
		t.Fatalf("send TQ1;: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	prevFrames := eng.Stats().TxFramesIn
	eng.QueueTx(makeStereoFrame(k4proto.FrameSamples20ms, 30000, 30000))
	waitUntil(t, time.Second, "post-RX loud tail frame consumed", func() bool {
		return eng.Stats().TxFramesIn > prevFrames
	})
	select {
	case pkt := <-fc.pkts:
		if _, _, isAudio := k4proto.ParseAudioPayload(pkt.payload); isAudio {
			t.Fatalf("unexpected residual audio packet sent after proxy RX;: %x", pkt.payload)
		}
	case <-time.After(40 * time.Millisecond):
	}

	cancel()
	<-runDone
}

func TestCATProxyPreboundListenerIdleExitAndOfflineFastFail(t *testing.T) {
	// 1. Offline K4 fast-fail check: pick an unused port that immediately refuses connections.
	closedLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen dummy: %v", err)
	}
	offlinePort := closedLn.Addr().(*net.TCPAddr).Port
	_ = closedLn.Close()

	eng := audio.NewMockEngine(k4proto.FrameSamples20ms)
	defer eng.Close()

	sess := NewSession(Config{
		Host:           "127.0.0.1",
		Port:           offlinePort,
		PassthroughCAT: true,
		Reconnect:      true,
		DialTimeout:    300 * time.Millisecond,
	}, eng, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- sess.Run(ctx) }()

	// Pre-bind CAT listener (simulating systemd socket activation FD).
	preLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen prebound: %v", err)
	}
	idleCh := make(chan struct{}, 1)
	proxy, err := StartCATProxy(ctx, CATProxyConfig{
		Listener:    preLn,
		IdleTimeout: 120 * time.Millisecond,
		OnIdle: func() {
			select {
			case idleCh <- struct{}{}:
			default:
			}
			cancel()
		},
	}, sess, nil)
	if err != nil {
		t.Fatalf("StartCATProxy: %v", err)
	}
	defer proxy.Close()

	// Wait briefly for first dial failure to mark K4 offline.
	waitUntil(t, time.Second, "hasDialFailed true", func() bool {
		return sess.hasDialFailed.Load()
	})

	// Second SendClientCAT while K4 is offline must fail fast (<50ms instead of 2.5s timeout).
	t0 := time.Now()
	if sendErr := sess.SendClientCAT(ctx, "PS;"); sendErr == nil {
		t.Fatalf("expected error from SendClientCAT while K4 is offline")
	}
	if elapsed := time.Since(t0); elapsed > 100*time.Millisecond {
		t.Fatalf("expected fast fail (<100ms) while K4 offline, took %v", elapsed)
	}

	// Connect a client to hold off idle shutdown longer than 120ms.
	c1, err := net.DialTimeout("tcp", proxy.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	time.Sleep(180 * time.Millisecond)
	select {
	case <-idleCh:
		t.Fatalf("OnIdle fired while client was still connected")
	default:
	}

	// Disconnect last client -> OnIdle must fire after ~120ms and cancel Run().
	_ = c1.Close()
	select {
	case <-idleCh:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for OnIdle after last client disconnected")
	}

	select {
	case runErr := <-runDone:
		if runErr != nil {
			t.Fatalf("Run() error on idle cancel: %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatalf("Run() did not exit after OnIdle cancel")
	}
}
