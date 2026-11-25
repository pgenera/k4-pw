package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k4-audio/internal/audio"
	"k4-audio/internal/k4proto"
)

const (
	defaultPort             = 9205
	defaultSilenceThreshold = int16(2)
	defaultTxHoldTail       = 100 * time.Millisecond
	defaultDialTimeout      = 5 * time.Second
	bootstrapDelay          = 250 * time.Millisecond
	pingInterval            = 1 * time.Second
	minBackoff              = 500 * time.Millisecond
	maxBackoff              = 10 * time.Second
	writeTimeout            = 5 * time.Second
	shutdownWriteTimeout    = 500 * time.Millisecond
	readBufferSize          = 32 * 1024
	bootstrapCAT            = "RDY;K41;ER1;AI5;EM1;SL0;TQ;"
	bootstrapCATPassthrough = "ER1;AI0;K40;EM1;SL0;"
)

// Config configures a K4 TCP audio/CAT bridge session on port 9205.
type Config struct {
	Host             string
	Port             int
	Password         string
	RxGainDb         float64
	TxGainDb         float64
	SilenceThreshold int16
	TxHoldTail       time.Duration
	TxCatOnAudio     bool
	PassthroughCAT   bool
	Reconnect        bool
	LogLevels        bool
	DialTimeout      time.Duration
}

func (c Config) withDefaults() Config {
	if c.Port <= 0 {
		c.Port = defaultPort
	}
	if c.SilenceThreshold <= 0 {
		c.SilenceThreshold = defaultSilenceThreshold
	}
	if c.TxHoldTail <= 0 {
		c.TxHoldTail = defaultTxHoldTail
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	return c
}

// SessionStats reports live connection, RTT, audio packet counters, and radio TX state.
type SessionStats struct {
	Connected   bool
	RttMs       int64
	RxAudioPkts uint64
	TxAudioPkts uint64
	K4TxActive  bool
}

type clientCATReq struct {
	raw  string
	done chan error
}

// Session bridges stereo audio between a local audio.Engine and a K4 TCP :9205 endpoint.
type Session struct {
	cfg    Config
	eng    audio.Engine
	logger *log.Logger

	connected   atomic.Bool
	rttMs       atomic.Int64
	rxAudioPkts atomic.Uint64
	txAudioPkts atomic.Uint64
	k4TxActive  atomic.Bool

	clientCATCh chan clientCATReq

	catSinkMu sync.RWMutex
	catSink   func(string)

	pingMu           sync.Mutex
	pendingPingToken string
	pendingPingSent  time.Time
	hasPendingPing   bool
}

// NewSession creates a K4 audio/CAT bridge Session.
func NewSession(cfg Config, eng audio.Engine, logger *log.Logger) *Session {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Session{
		cfg:         cfg.withDefaults(),
		eng:         eng,
		logger:      logger,
		clientCATCh: make(chan clientCATReq),
	}
}

// SetCATReplySink registers an optional callback that receives non-internal CAT replies from :9205.
func (s *Session) SetCATReplySink(fn func(string)) {
	s.catSinkMu.Lock()
	s.catSink = fn
	s.catSinkMu.Unlock()
}

// SendClientCAT sends a raw semicolon-terminated CAT command (e.g. from Hamlib/WSJT-X)
// over the active :9205 connection, updating TX audio gating when TX;/RX; is sent.
func (s *Session) SendClientCAT(ctx context.Context, rawCmd string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	req := clientCATReq{
		raw:  rawCmd,
		done: make(chan error, 1),
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case s.clientCATCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("bridge: timed out waiting for active :9205 session")
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("bridge: timed out completing :9205 CAT write")
	}
}

// Stats returns a snapshot of the session's live operational counters and state.
func (s *Session) Stats() SessionStats {
	return SessionStats{
		Connected:   s.connected.Load(),
		RttMs:       s.rttMs.Load(),
		RxAudioPkts: s.rxAudioPkts.Load(),
		TxAudioPkts: s.txAudioPkts.Load(),
		K4TxActive:  s.k4TxActive.Load(),
	}
}

// Run connects to the configured K4 endpoint and runs the RX/TX audio and CAT loops
// until ctx is cancelled or an unrecoverable error occurs (when Reconnect is false).
func (s *Session) Run(ctx context.Context) error {
	if s.cfg.Host == "" {
		return errors.New("bridge: host is required")
	}
	if s.eng == nil {
		return errors.New("bridge: audio engine is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return nil
	}

	stopRun := make(chan struct{})
	defer close(stopRun)

	txCh := make(chan []int16)
	txErrCh := make(chan error, 1)
	txBufPool := make(chan []int16, 2)
	txBufPool <- make([]int16, k4proto.FrameSamples20ms*2)
	txBufPool <- make([]int16, k4proto.FrameSamples20ms*2)

	go s.txCaptureLoop(stopRun, txCh, txBufPool, txErrCh)

	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	dialer := &net.Dialer{Timeout: s.cfg.DialTimeout}
	backoff := minBackoff

	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !s.cfg.Reconnect {
				return fmt.Errorf("bridge dial %s: %w", addr, err)
			}
			s.logger.Printf("k4 connect %s failed: %v; retrying in %s", addr, err, backoff)
			if !sleepWithContext(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff)
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
		}

		established, txErr, connErr := s.runConn(ctx, conn, txCh, txBufPool, txErrCh)
		if ctx.Err() != nil {
			return nil
		}
		if txErr != nil {
			return txErr
		}
		if connErr == nil {
			return nil
		}
		if !s.cfg.Reconnect {
			return connErr
		}

		if established {
			backoff = minBackoff
		}
		s.logger.Printf("k4 connection %s ended: %v; reconnecting in %s", addr, connErr, backoff)
		if !sleepWithContext(ctx, backoff) {
			return nil
		}
		if !established {
			backoff = nextBackoff(backoff)
		}
	}
}

func (s *Session) txCaptureLoop(stopRun <-chan struct{}, txCh chan<- []int16, bufPool chan []int16, txErrCh chan<- error) {
	needSamples := k4proto.FrameSamples20ms * 2
	for {
		var buf []int16
		select {
		case buf = <-bufPool:
		case <-stopRun:
			return
		}
		if cap(buf) < needSamples {
			buf = make([]int16, needSamples)
		} else {
			buf = buf[:needSamples]
		}

		frames, err := s.eng.ReadTxStereo(buf)
		if err != nil {
			select {
			case <-stopRun:
				return
			default:
			}
			select {
			case txErrCh <- err:
			default:
			}
			return
		}
		if frames <= 0 {
			select {
			case bufPool <- buf:
			default:
			}
			continue
		}

		sampleCount := frames * 2
		if sampleCount > len(buf) {
			sampleCount = len(buf)
		}
		select {
		case txCh <- buf[:sampleCount]:
		case <-stopRun:
			return
		}
	}
}

func (s *Session) runConn(
	ctx context.Context,
	conn net.Conn,
	txCh <-chan []int16,
	txBufPool chan<- []int16,
	txErrCh <-chan error,
) (established bool, txErr error, connErr error) {
	connStart := time.Now()
	authHex := k4proto.ExpectedAuthHex(s.cfg.Password)
	if err := writePacket(conn, []byte(authHex), writeTimeout); err != nil {
		_ = conn.Close()
		return false, nil, fmt.Errorf("bridge auth write: %w", err)
	}

	s.connected.Store(true)
	s.clearPendingPing()
	defer func() {
		s.connected.Store(false)
		s.k4TxActive.Store(false)
		s.clearPendingPing()
	}()

	firstPacketCh := make(chan struct{})
	rxErrCh := make(chan error, 1)
	rxDone := make(chan struct{})
	go func() {
		defer close(rxDone)
		s.rxLoop(conn, firstPacketCh, rxErrCh)
	}()

	bootstrapTimer := time.NewTimer(bootstrapDelay)
	defer bootstrapTimer.Stop()

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	holdTimer := time.NewTimer(time.Hour)
	holdTimer.Stop()
	defer holdTimer.Stop()

	var (
		activeTxCh   <-chan []int16
		bootstrapped bool
		gotPacket    bool
		audioTxKeyed bool
		proxyTxKeyed bool
		holdUntil    time.Time
		txSeq        uint8
		txEM1Scratch = make([]byte, 0, k4proto.FrameSamples20ms*4)
		txFrameCount uint64
	)

	sendBootstrap := func() error {
		if bootstrapped {
			return nil
		}
		bootstrapTimer.Stop()
		bootStr := bootstrapCAT
		if s.cfg.PassthroughCAT {
			bootStr = bootstrapCATPassthrough
		}
		if err := writePacket(conn, k4proto.EncodeCAT(bootStr), writeTimeout); err != nil {
			return fmt.Errorf("bridge bootstrap write: %w", err)
		}
		bootstrapped = true
		activeTxCh = txCh
		return nil
	}

	gracefulClose := func() {
		if audioTxKeyed || proxyTxKeyed {
			_ = writePacket(conn, k4proto.EncodeCAT("RX;"), shutdownWriteTimeout)
			audioTxKeyed = false
			proxyTxKeyed = false
			s.k4TxActive.Store(false)
		}
		_ = writePacket(conn, k4proto.EncodeCAT("RRN;"), shutdownWriteTimeout)
		_ = conn.Close()
		<-rxDone
	}

	abortClose := func() {
		_ = conn.Close()
		<-rxDone
	}

	isEstablished := func() bool {
		return gotPacket || time.Since(connStart) >= 2*time.Second
	}

	for {
		select {
		case <-ctx.Done():
			gracefulClose()
			return isEstablished(), nil, nil

		case err := <-txErrCh:
			if ctx.Err() != nil {
				gracefulClose()
				return isEstablished(), nil, nil
			}
			gracefulClose()
			return isEstablished(), err, nil

		case err := <-rxErrCh:
			abortClose()
			return isEstablished(), nil, err

		case <-firstPacketCh:
			firstPacketCh = nil
			gotPacket = true
			if err := sendBootstrap(); err != nil {
				abortClose()
				return isEstablished(), nil, err
			}

		case <-bootstrapTimer.C:
			if err := sendBootstrap(); err != nil {
				abortClose()
				return isEstablished(), nil, err
			}

		case <-pingTicker.C:
			if !bootstrapped {
				continue
			}
			now := time.Now()
			tok := strconv.FormatInt(now.Unix(), 10)
			s.setPendingPing(tok, now)
			if err := writePacket(conn, k4proto.EncodeCAT("PING"+tok+";"), writeTimeout); err != nil {
				abortClose()
				return isEstablished(), nil, fmt.Errorf("bridge ping write: %w", err)
			}

		case req := <-s.clientCATCh:
			if err := sendBootstrap(); err != nil {
				req.done <- err
				abortClose()
				return isEstablished(), nil, err
			}
			cmdUpper := strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(req.raw, ";")))
			outRaw := req.raw
			if isPTTKeyCmd(cmdUpper) {
				outRaw = "TX;"
				if !proxyTxKeyed {
					if fl, ok := s.eng.(interface{ FlushTx() }); ok {
						fl.FlushTx()
					}
					drainPendingTx(activeTxCh, txBufPool)
				}
				holdUntil = time.Time{}
				proxyTxKeyed = true
				s.k4TxActive.Store(true)
			} else if isPTTUnkeyCmd(cmdUpper) {
				outRaw = "RX;"
				proxyTxKeyed = false
				audioTxKeyed = false
				holdUntil = time.Time{}
				holdTimer.Stop()
				s.k4TxActive.Store(false)
			}
			if err := writePacket(conn, k4proto.EncodeCAT(outRaw), writeTimeout); err != nil {
				werr := fmt.Errorf("bridge client CAT write: %w", err)
				req.done <- werr
				abortClose()
				return isEstablished(), nil, werr
			}
			req.done <- nil

		case <-holdTimer.C:
			if !audioTxKeyed {
				continue
			}
			now := time.Now()
			if now.Before(holdUntil) {
				holdTimer.Reset(holdUntil.Sub(now))
				continue
			}
			if err := writePacket(conn, k4proto.EncodeCAT("RX;"), writeTimeout); err != nil {
				abortClose()
				return isEstablished(), nil, fmt.Errorf("bridge tx-cat RX write: %w", err)
			}
			audioTxKeyed = false
			s.k4TxActive.Store(false)

		case pcm := <-activeTxCh:
			em1Bytes, rms, peak := k4proto.EncodeTxStereoS16ToEM1(pcm, s.cfg.TxGainDb, txEM1Scratch)
			txEM1Scratch = em1Bytes
			returnTxBuf(txBufPool, pcm)

			if ctx.Err() != nil {
				gracefulClose()
				return isEstablished(), nil, nil
			}

			now := time.Now()
			audioActive := peak >= s.cfg.SilenceThreshold
			var withinHold bool
			if audioActive {
				holdUntil = now.Add(s.cfg.TxHoldTail)
				withinHold = true
				if s.cfg.TxCatOnAudio {
					holdTimer.Reset(s.cfg.TxHoldTail)
					if !audioTxKeyed {
						if err := writePacket(conn, k4proto.EncodeCAT("TX;"), writeTimeout); err != nil {
							abortClose()
							return isEstablished(), nil, fmt.Errorf("bridge tx-cat TX write: %w", err)
						}
						audioTxKeyed = true
					}
				}
			} else {
				withinHold = !holdUntil.IsZero() && now.Before(holdUntil)
				if !withinHold && audioTxKeyed {
					holdTimer.Stop()
					if err := writePacket(conn, k4proto.EncodeCAT("RX;"), writeTimeout); err != nil {
						abortClose()
						return isEstablished(), nil, fmt.Errorf("bridge tx-cat RX write: %w", err)
					}
					audioTxKeyed = false
					s.k4TxActive.Store(false)
				}
			}

			k4Tx := proxyTxKeyed || s.k4TxActive.Load()
			var shouldSend bool
			if s.cfg.PassthroughCAT && !s.cfg.TxCatOnAudio {
				// In CAT-multiplexed mode (WSJT-X PTT=CAT), stream 20ms 0x01 audio frames
				// strictly while PTT is explicitly keyed via TX;..RX; (matching k4-js txAudioAllowed).
				// Never send 0x01 frames before TX; or after RX;, even while PulseAudio drains
				// buffered tail audio, as unkeyed 0x01 frames trigger K4 DAPM replay and hold TQ1.
				shouldSend = proxyTxKeyed
			} else {
				shouldSend = audioActive || withinHold || k4Tx
			}
			if shouldSend && len(em1Bytes) > 0 {
				pkt := k4proto.EncodeAudioPacket(txSeq, k4proto.EncodeModePCM16, k4proto.FrameSamples20ms, em1Bytes)
				if err := writePacket(conn, pkt, writeTimeout); err != nil {
					abortClose()
					return isEstablished(), nil, fmt.Errorf("bridge audio write: %w", err)
				}
				txSeq++
				s.txAudioPkts.Add(1)
			}

			txFrameCount++
			if s.cfg.LogLevels && (txFrameCount == 1 || txFrameCount%50 == 0) {
				s.logger.Printf("tx levels: peak=%d rms=%.4f send=%v k4_tx=%v", peak, rms, shouldSend, k4Tx)
			}
		}
	}
}

func (s *Session) rxLoop(conn net.Conn, firstPacketCh chan struct{}, rxErrCh chan<- error) {
	readBuf := make([]byte, readBufferSize)
	rxScratch := make([]int16, 0, k4proto.FrameSamples20ms*2)
	framer := &k4proto.StreamFramer{}
	var firstOnce sync.Once

	for {
		n, err := conn.Read(readBuf)
		if n > 0 {
			payloads := framer.Push(readBuf[:n])
			if len(payloads) > 0 {
				firstOnce.Do(func() {
					close(firstPacketCh)
				})
			}
			for _, payload := range payloads {
				if len(payload) == 0 {
					continue
				}
				switch payload[0] {
				case k4proto.TypePAN, k4proto.TypeMiniPAN:
					// Ignore panadapter payloads without extra allocations.
					continue

				case k4proto.TypeAudio:
					hdr, data, ok := k4proto.ParseAudioPayload(payload)
					if !ok {
						continue
					}
					stereo, ok := k4proto.DecodeRxToStereoS16(hdr, data, s.cfg.RxGainDb, rxScratch)
					if !ok {
						continue
					}
					rxScratch = stereo
					if len(stereo) > 0 {
						s.eng.PushRxStereo(stereo)
						s.rxAudioPkts.Add(1)
					}

				case k4proto.TypeCAT:
					catText, ok := k4proto.ParseCATPayload(payload)
					if !ok {
						continue
					}
					s.handleCAT(catText)
				}
			}
		}
		if err != nil {
			rxErrCh <- err
			return
		}
	}
}

func parseIFTxState(frame string) (tx bool, ok bool) {
	// Standard Elecraft K3/K4 hardware IF format:
	// "IF" (0..1) + 11-digit freq (2..12) + 5 spaces (13..17) + "+yyyy" (18..22) +
	// RIT (23) + XIT (24) + " " (25) + "00" (26..27) + TX (28) + mode (29) ...
	if len(frame) > 28 && frame[25] == ' ' {
		switch frame[28] {
		case '1':
			return true, true
		case '0':
			return false, true
		}
	}
	if len(frame) > 25 {
		switch frame[25] {
		case '1':
			return true, true
		case '0':
			return false, true
		}
	}
	return false, false
}

func isInternalBridgeCAT(trimmedFrame string) bool {
	return strings.HasPrefix(trimmedFrame, "PONG") ||
		strings.HasPrefix(trimmedFrame, "DAPM") ||
		trimmedFrame == "DA0" ||
		strings.HasPrefix(trimmedFrame, "MEDF") ||
		strings.HasPrefix(trimmedFrame, "SIFP") ||
		strings.HasPrefix(trimmedFrame, "SICP") ||
		strings.HasPrefix(trimmedFrame, "SIRF") ||
		strings.HasPrefix(trimmedFrame, "SIDA") ||
		strings.HasPrefix(trimmedFrame, "SIDD") ||
		strings.HasPrefix(trimmedFrame, "SIDU") ||
		strings.HasPrefix(trimmedFrame, "SIRC") ||
		strings.HasPrefix(trimmedFrame, "SID0")
}

// filterClientCATReplies preserves exact untrimmed CAT segments (such as 38-byte IF... ; frames)
// while stripping internal :9205 keepalive/DAP/telemetry frames (PONG, DAPM, DA0, MEDF, SI*).
func filterClientCATReplies(catText string) string {
	var b strings.Builder
	i := 0
	n := len(catText)
	for i < n {
		semi := strings.IndexByte(catText[i:], ';')
		if semi < 0 {
			break
		}
		seg := catText[i : i+semi+1]
		trimmed := strings.TrimSpace(catText[i : i+semi])
		if trimmed != "" && !isInternalBridgeCAT(trimmed) {
			b.WriteString(seg)
		}
		i += semi + 1
	}
	return b.String()
}

func (s *Session) handleCAT(catText string) {
	frames := k4proto.SplitCATFrames(catText)
	for _, frame := range frames {
		switch {
		case frame == "TQ1" || frame == "TQX1" || frame == "TX":
			s.k4TxActive.Store(true)
		case frame == "TQ0" || frame == "TQX0" || frame == "RX" || frame == "DA0":
			s.k4TxActive.Store(false)
		case strings.HasPrefix(frame, "IF"):
			if tx, ok := parseIFTxState(frame); ok {
				s.k4TxActive.Store(tx)
			}
		case strings.HasPrefix(frame, "PONG"):
			s.recordPong(strings.TrimPrefix(frame, "PONG"))
		}
	}

	s.catSinkMu.RLock()
	sink := s.catSink
	s.catSinkMu.RUnlock()
	if sink != nil {
		if forwarded := filterClientCATReplies(catText); forwarded != "" {
			sink(forwarded)
		}
	}
}

func (s *Session) setPendingPing(token string, sentAt time.Time) {
	s.pingMu.Lock()
	s.pendingPingToken = token
	s.pendingPingSent = sentAt
	s.hasPendingPing = true
	s.pingMu.Unlock()
}

func (s *Session) clearPendingPing() {
	s.pingMu.Lock()
	s.pendingPingToken = ""
	s.pendingPingSent = time.Time{}
	s.hasPendingPing = false
	s.pingMu.Unlock()
}

func (s *Session) recordPong(token string) {
	s.pingMu.Lock()
	if !s.hasPendingPing {
		s.pingMu.Unlock()
		return
	}
	if token == "" || token == s.pendingPingToken || !s.pendingPingSent.IsZero() {
		rtt := time.Since(s.pendingPingSent).Milliseconds()
		if rtt < 0 {
			rtt = 0
		}
		s.rttMs.Store(rtt)
		s.hasPendingPing = false
	}
	s.pingMu.Unlock()
}

func drainPendingTx(txCh <-chan []int16, pool chan<- []int16) {
	if txCh == nil {
		return
	}
	for {
		select {
		case pcm := <-txCh:
			returnTxBuf(pool, pcm)
		default:
			return
		}
	}
}

func returnTxBuf(pool chan<- []int16, buf []int16) {
	if cap(buf) == 0 {
		return
	}
	select {
	case pool <- buf[:cap(buf)]:
	default:
	}
}

func writePacket(conn net.Conn, data []byte, timeout time.Duration) error {
	if timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	for len(data) > 0 {
		n, err := conn.Write(data)
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

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}
