package audio

import (
	"io"
	"sync"
)

const (
	defaultMockSampleRate = 12000
	defaultMockCapFrames  = 48000 // 4.0 s @ 12 kHz
)

// MockEngine is an in-memory deterministic Engine for unit and integration tests.
type MockEngine struct {
	mu           sync.Mutex
	txCond       *sync.Cond
	frameSamples int
	sampleRate   int
	rxSamples    []int16
	txSamples    []int16
	rxFramesIn   uint64
	rxFramesOut  uint64
	txFramesIn   uint64
	closed       bool
}

// NewMockEngine constructs a MockEngine yielding frameSamples stereo frames per ReadTxStereo call.
func NewMockEngine(frameSamples int) *MockEngine {
	if frameSamples <= 0 {
		frameSamples = 240
	}
	m := &MockEngine{
		frameSamples: frameSamples,
		sampleRate:   defaultMockSampleRate,
	}
	m.txCond = sync.NewCond(&m.mu)
	return m
}

// PushRxStereo appends interleaved [L, R, ...] RX stereo frames into a bounded queue.
func (m *MockEngine) PushRxStereo(interleaved []int16) {
	nFrames := len(interleaved) / 2
	if nFrames == 0 {
		return
	}
	nSamples := nFrames * 2

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}
	m.rxFramesIn += uint64(nFrames)

	maxSamples := defaultMockCapFrames * 2
	if nSamples >= maxSamples {
		m.rxSamples = append(m.rxSamples[:0], interleaved[nSamples-maxSamples:nSamples]...)
		return
	}
	m.rxSamples = append(m.rxSamples, interleaved[:nSamples]...)
	if len(m.rxSamples) > maxSamples {
		excess := len(m.rxSamples) - maxSamples
		copy(m.rxSamples, m.rxSamples[excess:])
		m.rxSamples = m.rxSamples[:maxSamples]
	}
}

// DrainRx pops up to maxFrames stereo frames pushed via PushRxStereo.
// If maxFrames <= 0, all available frames are drained.
func (m *MockEngine) DrainRx(maxFrames int) []int16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	availFrames := len(m.rxSamples) / 2
	if availFrames == 0 {
		return nil
	}
	takeFrames := availFrames
	if maxFrames > 0 && maxFrames < availFrames {
		takeFrames = maxFrames
	}
	takeSamples := takeFrames * 2
	out := make([]int16, takeSamples)
	copy(out, m.rxSamples[:takeSamples])

	rem := len(m.rxSamples) - takeSamples
	copy(m.rxSamples, m.rxSamples[takeSamples:])
	m.rxSamples = m.rxSamples[:rem]
	m.rxFramesOut += uint64(takeFrames)
	return out
}

// QueueTx enqueues interleaved stereo TX samples to be returned by ReadTxStereo.
func (m *MockEngine) QueueTx(interleaved []int16) {
	nFrames := len(interleaved) / 2
	if nFrames == 0 {
		return
	}
	nSamples := nFrames * 2

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}

	maxSamples := defaultMockCapFrames * 2
	if nSamples >= maxSamples {
		m.txSamples = append(m.txSamples[:0], interleaved[nSamples-maxSamples:nSamples]...)
	} else {
		m.txSamples = append(m.txSamples, interleaved[:nSamples]...)
		if len(m.txSamples) > maxSamples {
			excess := len(m.txSamples) - maxSamples
			copy(m.txSamples, m.txSamples[excess:])
			m.txSamples = m.txSamples[:maxSamples]
		}
	}
	m.txCond.Broadcast()
}

// ReadTxStereo blocks until frameSamples stereo frames are queued or Close is called.
func (m *MockEngine) ReadTxStereo(dst []int16) (int, error) {
	needSamples := m.frameSamples * 2
	if len(dst) < needSamples {
		return 0, io.ErrShortBuffer
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for !m.closed && len(m.txSamples) < needSamples {
		m.txCond.Wait()
	}
	if m.closed {
		return 0, io.EOF
	}

	copy(dst[:needSamples], m.txSamples[:needSamples])
	rem := len(m.txSamples) - needSamples
	copy(m.txSamples, m.txSamples[needSamples:])
	m.txSamples = m.txSamples[:rem]
	m.txFramesIn += uint64(m.frameSamples)
	return m.frameSamples, nil
}

// Stats returns counters and current RX buffer fill in milliseconds.
func (m *MockEngine) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()

	buffered := len(m.rxSamples) / 2
	return Stats{
		RxFramesIn:   m.rxFramesIn,
		RxFramesOut:  m.rxFramesOut,
		RxUnderruns:  0,
		TxFramesIn:   m.txFramesIn,
		BufferFillMs: float64(buffered) * 1000.0 / float64(m.sampleRate),
		RxBackend:    "mock",
		TxBackend:    "mock",
	}
}

// Close closes the mock engine and unblocks any waiting ReadTxStereo call with io.EOF.
func (m *MockEngine) Close() error {
	m.mu.Lock()
	m.closed = true
	m.txCond.Broadcast()
	m.mu.Unlock()
	return nil
}
