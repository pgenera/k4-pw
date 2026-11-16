//go:build cgo && linux

package audio

/*
#cgo linux LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	int32_t format;
	uint32_t rate;
	uint8_t channels;
} k4_pa_sample_spec;

typedef struct {
	uint32_t maxlength;
	uint32_t tlength;
	uint32_t prebuf;
	uint32_t minreq;
	uint32_t fragsize;
} k4_pa_buffer_attr;

typedef void* (*fn_pa_simple_new)(
	const char *server,
	const char *name,
	int32_t dir,
	const char *dev,
	const char *stream_name,
	const k4_pa_sample_spec *ss,
	const void *map,
	const k4_pa_buffer_attr *attr,
	int *error
);
typedef int  (*fn_pa_simple_read)(void *s, void *data, size_t bytes, int *error);
typedef void (*fn_pa_simple_free)(void *s);

static void* call_pa_simple_new(
	void *fn,
	const char *name,
	int32_t dir,
	const char *dev,
	const char *stream_name,
	const k4_pa_sample_spec *ss,
	const k4_pa_buffer_attr *attr,
	int *error
) {
	return ((fn_pa_simple_new)fn)(NULL, name, dir, dev, stream_name, ss, NULL, attr, error);
}

static int call_pa_simple_read(void *fn, void *s, void *data, size_t bytes, int *error) {
	return ((fn_pa_simple_read)fn)(s, data, bytes, error);
}

static void call_pa_simple_free(void *fn, void *s) {
	((fn_pa_simple_free)fn)(s);
}
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
	"unsafe"
)

const (
	paSampleS16LE   = 3
	paStreamRecord  = 2
	pulseChunkBytes = 960
	pulseRingBytes  = 7680
)

type pulseSimpleDynLib struct {
	handle     unsafe.Pointer
	simpleNew  unsafe.Pointer
	simpleRead unsafe.Pointer
	simpleFree unsafe.Pointer
	loadErr    error
}

var (
	pulseLibOnce sync.Once
	pulseLib     pulseSimpleDynLib
)

func loadPulseSimpleLib() *pulseSimpleDynLib {
	pulseLibOnce.Do(func() {
		tryOpen := func(libName string) unsafe.Pointer {
			cs := C.CString(libName)
			defer C.free(unsafe.Pointer(cs))
			return C.dlopen(cs, C.RTLD_LAZY)
		}

		handle := tryOpen("libpulse-simple.so.0")
		if handle == nil {
			handle = tryOpen("libpulse-simple.so")
		}
		if handle == nil {
			pulseLib.loadErr = errors.New("dlopen(libpulse-simple.so.0) failed")
			return
		}
		pulseLib.handle = handle

		lookup := func(sym string) (unsafe.Pointer, error) {
			cs := C.CString(sym)
			defer C.free(unsafe.Pointer(cs))
			ptr := C.dlsym(handle, cs)
			if ptr == nil {
				return nil, fmt.Errorf("dlsym(%s) failed", sym)
			}
			return ptr, nil
		}

		syms := []struct {
			name string
			dst  *unsafe.Pointer
		}{
			{"pa_simple_new", &pulseLib.simpleNew},
			{"pa_simple_read", &pulseLib.simpleRead},
			{"pa_simple_free", &pulseLib.simpleFree},
		}
		for _, s := range syms {
			ptr, err := lookup(s.name)
			if err != nil {
				pulseLib.loadErr = err
				return
			}
			*s.dst = ptr
		}
	})
	return &pulseLib
}

type pulseSimpleReader struct {
	lib       *pulseSimpleDynLib
	handle    unsafe.Pointer
	mu        sync.Mutex
	cond      *sync.Cond
	ring      []byte
	head      int
	tail      int
	avail     int
	closed    bool
	readErr   error
	loopDone  chan struct{}
	closeOnce sync.Once
}

func openPulseSimpleRecord(device string, sampleRate int) (*pulseSimpleReader, error) {
	lib := loadPulseSimpleLib()
	if lib.loadErr != nil {
		return nil, lib.loadErr
	}
	if sampleRate <= 0 {
		return nil, fmt.Errorf("invalid sample rate: %d", sampleRate)
	}

	cName := C.CString("k4-audio")
	defer C.free(unsafe.Pointer(cName))
	cDev := C.CString(device)
	defer C.free(unsafe.Pointer(cDev))
	cStream := C.CString("k4-tx")
	defer C.free(unsafe.Pointer(cStream))

	ss := C.k4_pa_sample_spec{
		format:   C.int32_t(paSampleS16LE),
		rate:     C.uint32_t(sampleRate),
		channels: C.uint8_t(2),
	}
	attr := C.k4_pa_buffer_attr{
		maxlength: C.uint32_t(pulseRingBytes),
		tlength:   C.uint32_t(^uint32(0)),
		prebuf:    C.uint32_t(^uint32(0)),
		minreq:    C.uint32_t(^uint32(0)),
		fragsize:  C.uint32_t(pulseChunkBytes),
	}

	var cerr C.int
	handle := C.call_pa_simple_new(
		lib.simpleNew,
		cName,
		C.int32_t(paStreamRecord),
		cDev,
		cStream,
		&ss,
		&attr,
		&cerr,
	)
	if handle == nil {
		return nil, fmt.Errorf("pa_simple_new(%q) failed: %d", device, int(cerr))
	}

	r := &pulseSimpleReader{
		lib:      lib,
		handle:   handle,
		ring:     make([]byte, pulseRingBytes),
		loopDone: make(chan struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	go r.readLoop()
	return r, nil
}

func (r *pulseSimpleReader) readLoop() {
	defer close(r.loopDone)
	defer C.call_pa_simple_free(r.lib.simpleFree, r.handle)

	var chunk [pulseChunkBytes]byte
	var cerr C.int

	for {
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return
		}

		rc := C.call_pa_simple_read(
			r.lib.simpleRead,
			r.handle,
			unsafe.Pointer(&chunk[0]),
			C.size_t(len(chunk)),
			&cerr,
		)

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		if rc < 0 {
			r.readErr = fmt.Errorf("pa_simple_read failed: %d", int(cerr))
			r.cond.Broadcast()
			r.mu.Unlock()
			return
		}

		r.pushBytesLocked(chunk[:])
		r.cond.Broadcast()
		r.mu.Unlock()
	}
}

func (r *pulseSimpleReader) pushBytesLocked(p []byte) {
	capB := len(r.ring)
	if len(p) >= capB {
		p = p[len(p)-capB:]
		copy(r.ring, p)
		r.head = 0
		r.tail = 0
		r.avail = capB
		return
	}
	if r.avail+len(p) > capB {
		drop := r.avail + len(p) - capB
		r.head = (r.head + drop) % capB
		r.avail -= drop
	}
	first := capB - r.tail
	if first > len(p) {
		first = len(p)
	}
	copy(r.ring[r.tail:], p[:first])
	if first < len(p) {
		copy(r.ring[0:], p[first:])
	}
	r.tail = (r.tail + len(p)) % capB
	r.avail += len(p)
}

func (r *pulseSimpleReader) popBytesLocked(dst []byte) {
	capB := len(r.ring)
	n := len(dst)
	first := capB - r.head
	if first > n {
		first = n
	}
	copy(dst[:first], r.ring[r.head:r.head+first])
	if first < n {
		copy(dst[first:n], r.ring[0:n-first])
	}
	r.head = (r.head + n) % capB
	r.avail -= n
}

func (r *pulseSimpleReader) ReadFrames(dst []int16, frameSamples int) (int, error) {
	needSamples := frameSamples * 2
	needBytes := needSamples * 2
	if len(dst) < needSamples {
		return 0, io.ErrShortBuffer
	}

	raw := make([]byte, needBytes)

	r.mu.Lock()
	for !r.closed && r.readErr == nil && r.avail < needBytes {
		r.cond.Wait()
	}
	if r.closed {
		r.mu.Unlock()
		return 0, io.EOF
	}
	if r.avail < needBytes && r.readErr != nil {
		err := r.readErr
		r.mu.Unlock()
		return 0, err
	}
	r.popBytesLocked(raw)
	r.mu.Unlock()

	for i := 0; i < needSamples; i++ {
		dst[i] = int16(binary.LittleEndian.Uint16(raw[i*2 : i*2+2]))
	}
	return frameSamples, nil
}

func (r *pulseSimpleReader) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cond.Broadcast()
		r.mu.Unlock()

		// Only the readLoop calls pa_simple_free upon exit to avoid concurrent C use-after-free.
		select {
		case <-r.loopDone:
		case <-time.After(250 * time.Millisecond):
		}
	})
	return nil
}
