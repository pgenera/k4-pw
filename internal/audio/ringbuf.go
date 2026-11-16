package audio

import (
	"math"
	"sync"
)

const underrunFadeFrames = 16

// StereoRingBuffer is a thread-safe circular buffer of interleaved int16 stereo
// frames ([L0, R0, L1, R1, ...]). It supports prefill gating, 16-frame linear
// underrun fade-out, and oldest-frame dropping when buffered lag exceeds maxLagFrames.
type StereoRingBuffer struct {
	mu            sync.Mutex
	buf           []int16
	capFrames     int
	head          int
	tail          int
	count         int
	prefillFrames int
	maxLagFrames  int
	started       bool
	inUnderrun    bool
	fadeRemaining int
	lastL         int16
	lastR         int16
	underruns     uint64
	framesIn      uint64
	framesOut     uint64
	droppedFrames uint64
}

// NewStereoRingBuffer allocates a stereo ring buffer sized in stereo frames.
// If capFrames <= 0, it defaults to 24000 frames (2.0 s at 12 kHz).
func NewStereoRingBuffer(capFrames, prefillFrames, maxLagFrames int) *StereoRingBuffer {
	if capFrames <= 0 {
		capFrames = 24000
	}
	if prefillFrames < 0 {
		prefillFrames = 0
	}
	if prefillFrames > capFrames {
		prefillFrames = capFrames
	}
	if maxLagFrames <= 0 || maxLagFrames > capFrames {
		maxLagFrames = capFrames
	}
	if maxLagFrames < prefillFrames {
		maxLagFrames = prefillFrames
	}
	return &StereoRingBuffer{
		buf:           make([]int16, capFrames*2),
		capFrames:     capFrames,
		prefillFrames: prefillFrames,
		maxLagFrames:  maxLagFrames,
	}
}

// Push appends interleaved [L, R, ...] stereo samples to the buffer.
// If a trailing odd sample is provided, it is ignored.
func (r *StereoRingBuffer) Push(interleaved []int16) {
	nFrames := len(interleaved) / 2
	if nFrames == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.framesIn += uint64(nFrames)

	if nFrames > r.capFrames {
		discard := nFrames - r.capFrames
		r.droppedFrames += uint64(discard)
		interleaved = interleaved[discard*2:]
		nFrames = r.capFrames
	}

	if r.count+nFrames > r.capFrames {
		drop := r.count + nFrames - r.capFrames
		r.dropOldestLocked(drop)
	}

	r.writeLocked(interleaved[:nFrames*2], nFrames)

	if r.maxLagFrames > 0 && r.count > r.maxLagFrames {
		excess := r.count - r.maxLagFrames
		r.dropOldestLocked(excess)
	}

	if !r.started && r.count >= r.prefillFrames && r.count > 0 {
		r.started = true
	}
}

// Pull fills dst with interleaved stereo frames. Before prefill completes, it
// zero-fills dst and returns (0, false) without counting an underrun. Once
// started, any shortfall triggers a 16-frame fade-out and increments Underruns.
func (r *StereoRingBuffer) Pull(dst []int16) (realFrames int, started bool) {
	needFrames := len(dst) / 2
	if len(dst)%2 != 0 {
		dst[len(dst)-1] = 0
	}
	if needFrames == 0 {
		r.mu.Lock()
		s := r.started
		r.mu.Unlock()
		return 0, s
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		clear(dst[:needFrames*2])
		return 0, false
	}

	if r.count >= needFrames {
		r.readLocked(dst[:needFrames*2], needFrames)
		r.lastL = dst[needFrames*2-2]
		r.lastR = dst[needFrames*2-1]
		r.inUnderrun = false
		r.fadeRemaining = 0
		r.framesOut += uint64(needFrames)
		return needFrames, true
	}

	r.underruns++
	avail := r.count
	if avail > 0 {
		r.readLocked(dst[:avail*2], avail)
		r.lastL = dst[avail*2-2]
		r.lastR = dst[avail*2-1]
		r.framesOut += uint64(avail)
		r.inUnderrun = true
		r.fadeRemaining = underrunFadeFrames
	} else if !r.inUnderrun {
		r.inUnderrun = true
		r.fadeRemaining = underrunFadeFrames
	}

	for f := avail; f < needFrames; f++ {
		if r.fadeRemaining > 0 {
			r.fadeRemaining--
			gain := float64(r.fadeRemaining) / float64(underrunFadeFrames)
			dst[f*2] = int16(math.Round(float64(r.lastL) * gain))
			dst[f*2+1] = int16(math.Round(float64(r.lastR) * gain))
			if r.fadeRemaining == 0 {
				r.lastL = 0
				r.lastR = 0
			}
		} else {
			dst[f*2] = 0
			dst[f*2+1] = 0
		}
	}

	return avail, true
}

func (r *StereoRingBuffer) dropOldestLocked(frames int) {
	if frames <= 0 {
		return
	}
	if frames >= r.count {
		r.droppedFrames += uint64(r.count)
		r.head = 0
		r.tail = 0
		r.count = 0
		return
	}
	r.head = (r.head + frames) % r.capFrames
	r.count -= frames
	r.droppedFrames += uint64(frames)
}

func (r *StereoRingBuffer) writeLocked(samples []int16, nFrames int) {
	first := r.capFrames - r.tail
	if first > nFrames {
		first = nFrames
	}
	copy(r.buf[r.tail*2:], samples[:first*2])
	if first < nFrames {
		copy(r.buf[0:], samples[first*2:nFrames*2])
	}
	r.tail = (r.tail + nFrames) % r.capFrames
	r.count += nFrames
}

func (r *StereoRingBuffer) readLocked(dst []int16, nFrames int) {
	first := r.capFrames - r.head
	if first > nFrames {
		first = nFrames
	}
	copy(dst[:first*2], r.buf[r.head*2:(r.head+first)*2])
	if first < nFrames {
		rem := nFrames - first
		copy(dst[first*2:nFrames*2], r.buf[0:rem*2])
	}
	r.head = (r.head + nFrames) % r.capFrames
	r.count -= nFrames
}

// BufferedFrames returns the number of stereo frames currently waiting in the buffer.
func (r *StereoRingBuffer) BufferedFrames() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Underruns returns the count of Pull calls after playback started that lacked a full frame.
func (r *StereoRingBuffer) Underruns() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.underruns
}

// FramesIn returns the total stereo frames passed to Push.
func (r *StereoRingBuffer) FramesIn() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.framesIn
}

// FramesOut returns the total real stereo frames read out via Pull.
func (r *StereoRingBuffer) FramesOut() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.framesOut
}

// DroppedFrames returns the number of oldest stereo frames discarded on overflow or lag clamping.
func (r *StereoRingBuffer) DroppedFrames() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.droppedFrames
}

// Started reports whether prefill has completed and playback has started.
func (r *StereoRingBuffer) Started() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started
}
