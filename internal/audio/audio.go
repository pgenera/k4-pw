package audio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Config configures the PipeWire/PulseAudio stereo audio engine.
type Config struct {
	RxTarget       string
	TxTarget       string
	SampleRate     int
	FrameSamples   int
	TargetBufferMs int
	Backend        string
}

func (c Config) withDefaults() Config {
	if c.RxTarget == "" {
		c.RxTarget = "k4_rx"
	}
	if c.TxTarget == "" {
		c.TxTarget = "k4_tx"
	}
	if c.SampleRate <= 0 {
		c.SampleRate = 12000
	}
	if c.FrameSamples <= 0 {
		c.FrameSamples = 240
	}
	if c.TargetBufferMs <= 0 {
		c.TargetBufferMs = 40
	}
	if c.Backend == "" {
		c.Backend = "auto"
	}
	return c
}

// Stats reports live RX/TX frame counts and jitter buffer fill.
type Stats struct {
	RxFramesIn   uint64
	RxFramesOut  uint64
	RxUnderruns  uint64
	TxFramesIn   uint64
	BufferFillMs float64
	RxBackend    string
	TxBackend    string
}

// Engine bridges decoded stereo RX frames to local audio output and captures
// local stereo TX frames to transmit to the radio.
type Engine interface {
	PushRxStereo(interleaved []int16)
	ReadTxStereo(dst []int16) (frames int, err error)
	Stats() Stats
	Close() error
}

type txCapture interface {
	ReadFrames(dst []int16, frameSamples int) (int, error)
	Close() error
}

type pipewireEngine struct {
	cfg        Config
	rxRing     *StereoRingBuffer
	rxPlayback *pwCatPlayback
	txCap      txCapture
	rxBackend  string
	txBackend  string
	txFramesIn atomic.Uint64
	closed     atomic.Bool
	closeOnce  sync.Once
}

// Open initializes an audio Engine according to cfg.Backend ("auto", "pulse", "pw-cat", "mock").
func Open(ctx context.Context, cfg Config) (Engine, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	cfg = cfg.withDefaults()
	switch cfg.Backend {
	case "auto", "pulse", "pw-cat", "mock":
	default:
		return nil, fmt.Errorf("unsupported audio backend %q (want auto, pulse, pw-cat, or mock)", cfg.Backend)
	}

	if cfg.Backend == "mock" {
		return NewMockEngine(cfg.FrameSamples), nil
	}

	capFrames := cfg.SampleRate * 2
	prefillFrames := (cfg.SampleRate * cfg.TargetBufferMs) / 1000
	maxLagFrames := cfg.SampleRate / 4 // 250 ms clamp
	if maxLagFrames < prefillFrames*2 {
		maxLagFrames = prefillFrames * 2
	}
	rxRing := NewStereoRingBuffer(capFrames, prefillFrames, maxLagFrames)

	rxPlayback, err := startPwCatPlayback(cfg.RxTarget, cfg.SampleRate, cfg.FrameSamples, rxRing)
	if err != nil {
		return nil, err
	}

	var txCap txCapture
	var txBackend string
	if cfg.Backend == "auto" || cfg.Backend == "pulse" {
		if pulseCap, pulseErr := openPulseSimpleRecord(cfg.TxTarget+".monitor", cfg.SampleRate); pulseErr == nil {
			txCap = pulseCap
			txBackend = "pulse"
		}
	}
	if txCap == nil {
		pwCap, pwErr := openPwCatRecord(cfg.TxTarget, cfg.SampleRate, cfg.FrameSamples)
		if pwErr != nil {
			_ = rxPlayback.Close()
			return nil, pwErr
		}
		txCap = pwCap
		txBackend = "pw-cat"
	}

	return &pipewireEngine{
		cfg:        cfg,
		rxRing:     rxRing,
		rxPlayback: rxPlayback,
		txCap:      txCap,
		rxBackend:  "pw-cat",
		txBackend:  txBackend,
	}, nil
}

func (e *pipewireEngine) PushRxStereo(interleaved []int16) {
	if e.closed.Load() {
		return
	}
	e.rxRing.Push(interleaved)
}

func (e *pipewireEngine) ReadTxStereo(dst []int16) (int, error) {
	if len(dst) < e.cfg.FrameSamples*2 {
		return 0, io.ErrShortBuffer
	}
	if e.closed.Load() {
		return 0, io.EOF
	}
	frames, err := e.txCap.ReadFrames(dst, e.cfg.FrameSamples)
	if frames > 0 {
		e.txFramesIn.Add(uint64(frames))
	}
	if err != nil && e.closed.Load() {
		return 0, io.EOF
	}
	return frames, err
}

func (e *pipewireEngine) FlushTx() {
	if fl, ok := e.txCap.(interface{ Flush() }); ok {
		fl.Flush()
	}
}

func (e *pipewireEngine) Stats() Stats {
	buffered := e.rxRing.BufferedFrames()
	return Stats{
		RxFramesIn:   e.rxRing.FramesIn(),
		RxFramesOut:  e.rxRing.FramesOut(),
		RxUnderruns:  e.rxRing.Underruns(),
		TxFramesIn:   e.txFramesIn.Load(),
		BufferFillMs: float64(buffered) * 1000.0 / float64(e.cfg.SampleRate),
		RxBackend:    e.rxBackend,
		TxBackend:    e.txBackend,
	}
}

func (e *pipewireEngine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		txErr := e.txCap.Close()
		rxErr := e.rxPlayback.Close()
		closeErr = errors.Join(rxErr, txErr)
	})
	return closeErr
}
