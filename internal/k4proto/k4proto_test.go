package k4proto

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"math"
	"reflect"
	"testing"
)

func TestExpectedAuthHex(t *testing.T) {
	emptySum := sha512.Sum384(nil)
	wantEmpty := hex.EncodeToString(emptySum[:])
	if got := ExpectedAuthHex(""); got != wantEmpty || len(got) != 96 {
		t.Fatalf("ExpectedAuthHex(\"\") = %q (len %d), want %q", got, len(got), wantEmpty)
	}

	pwSum := sha512.Sum384([]byte("secret-k4-pass"))
	wantPW := hex.EncodeToString(pwSum[:])
	if got := ExpectedAuthHex("secret-k4-pass"); got != wantPW || len(got) != 96 {
		t.Fatalf("ExpectedAuthHex(pw) = %q, want %q", got, wantPW)
	}
}

func TestEncodeParseCATAndAudio(t *testing.T) {
	catPkt := EncodeCAT("FA00014074000;")
	framer := &StreamFramer{}
	payloads := framer.Push(catPkt)
	if len(payloads) != 1 {
		t.Fatalf("expected 1 CAT payload, got %d", len(payloads))
	}
	cat, ok := ParseCATPayload(payloads[0])
	if !ok || cat != "FA00014074000;" {
		t.Fatalf("ParseCATPayload = (%q, %v), want (\"FA00014074000;\", true)", cat, ok)
	}
	if _, _, ok := ParseAudioPayload(payloads[0]); ok {
		t.Fatalf("ParseAudioPayload unexpectedly matched CAT payload")
	}
	if _, ok := ParseCATPayload([]byte{TypeCAT, 0x00}); ok {
		t.Fatalf("ParseCATPayload unexpectedly accepted short payload")
	}

	rawAudio := []byte{0x01, 0x02, 0x03, 0x04}
	audioPkt := EncodeAudioPacket(42, EncodeModePCM16, FrameSamples20ms, rawAudio)
	payloads = framer.Push(audioPkt)
	if len(payloads) != 1 {
		t.Fatalf("expected 1 audio payload, got %d", len(payloads))
	}
	hdr, data, ok := ParseAudioPayload(payloads[0])
	if !ok {
		t.Fatalf("ParseAudioPayload returned false")
	}
	if hdr.Ver != 0x01 || hdr.Seq != 42 || hdr.EncodeMode != EncodeModePCM16 ||
		hdr.FrameSamplesPerCh != FrameSamples20ms || hdr.SampleRateCode != SampleRateCode0 {
		t.Fatalf("unexpected AudioHeader: %+v", hdr)
	}
	if !bytes.Equal(data, rawAudio) {
		t.Fatalf("audio data = %x, want %x", data, rawAudio)
	}
	if _, ok := ParseCATPayload(payloads[0]); ok {
		t.Fatalf("ParseCATPayload unexpectedly matched audio payload")
	}
	if _, _, ok := ParseAudioPayload([]byte{TypeAudio, 1, 2, 3, 4, 5}); ok {
		t.Fatalf("ParseAudioPayload unexpectedly accepted short payload")
	}
}

func TestStreamFramerSplitAndCorruptMarkers(t *testing.T) {
	pkt1 := EncodeCAT("FA00014074000;")
	pkt2 := EncodeCAT("FB00014025000;")

	falseZeroLen := []byte{0xFE, 0xFD, 0xFC, 0xFB, 0x00, 0x00, 0x00, 0x00, 0xAA, 0xBB}
	falseHugeLen := []byte{0xFE, 0xFD, 0xFC, 0xFB, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD}
	falseBadEnd := []byte{0xFE, 0xFD, 0xFC, 0xFB, 0x00, 0x00, 0x00, 0x04, 'T', 'E', 'S', 'T', 0x01, 0x02, 0x03, 0x04}

	var stream []byte
	stream = append(stream, 0xFE, 0xFD) // partial start noise
	stream = append(stream, falseZeroLen...)
	stream = append(stream, falseHugeLen...)
	stream = append(stream, falseBadEnd...)
	stream = append(stream, pkt1...)
	stream = append(stream, 0x00, 0x11, 0xFE, 0xFD) // noise ending in partial start
	stream = append(stream, pkt2...)

	framer := &StreamFramer{}
	var decoded [][]byte
	for i := 0; i < len(stream); {
		step := (i % 3) + 1
		if i+step > len(stream) {
			step = len(stream) - i
		}
		decoded = append(decoded, framer.Push(stream[i:i+step])...)
		i += step
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2 decoded packets, got %d", len(decoded))
	}
	cat1, ok1 := ParseCATPayload(decoded[0])
	cat2, ok2 := ParseCATPayload(decoded[1])
	if !ok1 || cat1 != "FA00014074000;" {
		t.Fatalf("pkt1 = (%q, %v)", cat1, ok1)
	}
	if !ok2 || cat2 != "FB00014025000;" {
		t.Fatalf("pkt2 = (%q, %v)", cat2, ok2)
	}
	if framer.PacketsDecoded != 2 {
		t.Fatalf("PacketsDecoded = %d, want 2", framer.PacketsDecoded)
	}
	if framer.BadLengths < 2 {
		t.Fatalf("expected BadLengths >= 2, got %d", framer.BadLengths)
	}
	if framer.BadEndMarkers < 1 {
		t.Fatalf("expected BadEndMarkers >= 1, got %d", framer.BadEndMarkers)
	}
}

func TestSplitCATFrames(t *testing.T) {
	input := "FA00014074000;TB020CQ DE W6ELE K ; 73 ;;TB$020CQ DE W6ELE K ; 73 ;;MD3;"
	got := SplitCATFrames(input)
	want := []string{
		"FA00014074000",
		"TB020CQ DE W6ELE K ; 73 ;",
		"TB$020CQ DE W6ELE K ; 73 ;",
		"MD3",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SplitCATFrames() = %#v, want %#v", got, want)
	}
}

func TestDbToLinearAndSoftClip(t *testing.T) {
	if got := DbToLinear(0); math.Abs(got-1.0) > 1e-12 {
		t.Fatalf("DbToLinear(0) = %v, want 1.0", got)
	}
	if got := DbToLinear(20); math.Abs(got-10.0) > 1e-12 {
		t.Fatalf("DbToLinear(20) = %v, want 10.0", got)
	}
	if got := DbToLinear(-20); math.Abs(got-0.1) > 1e-12 {
		t.Fatalf("DbToLinear(-20) = %v, want 0.1", got)
	}

	for _, x := range []float64{-0.9, -0.5, 0, 0.5, 0.9} {
		if got := SoftClip(x); got != x {
			t.Fatalf("SoftClip(%v) = %v, expected exact identity inside [-0.9, 0.9]", x, got)
		}
	}
	over := SoftClip(1.5)
	if over <= 0.9 || over >= 1.0 {
		t.Fatalf("SoftClip(1.5) = %v, expected in (0.9, 1.0)", over)
	}
	if math.Abs(SoftClip(-1.5)+over) > 1e-12 {
		t.Fatalf("SoftClip not symmetric: +1.5 -> %v, -1.5 -> %v", over, SoftClip(-1.5))
	}
	if SoftClip(math.NaN()) != 0 {
		t.Fatalf("SoftClip(NaN) should return 0")
	}
}

func s16u(v int16) uint16 { return uint16(v) }
func s32u(v int32) uint32 { return uint32(v) }

func TestDecodeRxEM1AndEM0Roundtrip(t *testing.T) {
	// EM1: wire +/-512 -> x32 -> +/-16384 -> /32 TX -> +/-512
	em1Bytes := make([]byte, 8)
	binary.LittleEndian.PutUint16(em1Bytes[0:2], s16u(512))
	binary.LittleEndian.PutUint16(em1Bytes[2:4], s16u(512))
	binary.LittleEndian.PutUint16(em1Bytes[4:6], s16u(-512))
	binary.LittleEndian.PutUint16(em1Bytes[6:8], s16u(-512))

	hdr1 := AudioHeader{Ver: 1, Seq: 1, EncodeMode: EncodeModePCM16, FrameSamplesPerCh: 2}
	scratch := make([]int16, 0, 16)
	basePtr := &scratch[:16][0]
	stereo, ok := DecodeRxToStereoS16(hdr1, em1Bytes, 0, scratch)
	if !ok {
		t.Fatalf("EM1 decode returned ok=false")
	}
	if &stereo[0] != basePtr {
		t.Fatalf("DecodeRxToStereoS16 reallocated dst slice instead of reusing capacity")
	}
	wantStereo := []int16{16384, 16384, -16384, -16384}
	if !reflect.DeepEqual(stereo, wantStereo) {
		t.Fatalf("EM1 decoded = %v, want %v", stereo, wantStereo)
	}

	txBytes, rms, peak := EncodeTxStereoS16ToEM1(stereo, 0, nil)
	if !bytes.Equal(txBytes, em1Bytes) {
		t.Fatalf("EM1 roundtrip txBytes = %x, want %x", txBytes, em1Bytes)
	}
	if math.Abs(rms-0.5) > 1e-9 || peak != 16384 {
		t.Fatalf("EM1 roundtrip rms=%v peak=%d, want 0.5 and 16384", rms, peak)
	}

	// EM0: 24-bit signed in S32LE (+/-131072 = +/-512<<8 -> x32 -> +/-16384)
	em0Bytes := make([]byte, 16)
	binary.LittleEndian.PutUint32(em0Bytes[0:4], s32u(131072))
	binary.LittleEndian.PutUint32(em0Bytes[4:8], s32u(131072))
	binary.LittleEndian.PutUint32(em0Bytes[8:12], s32u(-131072))
	binary.LittleEndian.PutUint32(em0Bytes[12:16], s32u(-131072))

	hdr0 := AudioHeader{Ver: 1, Seq: 2, EncodeMode: EncodeModePCM24, FrameSamplesPerCh: 2}
	stereo0, ok := DecodeRxToStereoS16(hdr0, em0Bytes, 0, nil)
	if !ok {
		t.Fatalf("EM0 decode returned ok=false")
	}
	if !reflect.DeepEqual(stereo0, wantStereo) {
		t.Fatalf("EM0 decoded = %v, want %v", stereo0, wantStereo)
	}

	// +6 dB RX gain approximately doubles amplitude (256 * 32 * 2 = 16384)
	em1Half := make([]byte, 4)
	binary.LittleEndian.PutUint16(em1Half[0:2], s16u(256))
	binary.LittleEndian.PutUint16(em1Half[2:4], s16u(-256))
	boosted, ok := DecodeRxToStereoS16(AudioHeader{Ver: 1, EncodeMode: EncodeModePCM16, FrameSamplesPerCh: 1}, em1Half, 6.020599913279624, nil)
	if !ok || boosted[0] != 16384 || boosted[1] != -16384 {
		t.Fatalf("+6dB RX gain decoded = %v (ok=%v), want [16384 -16384]", boosted, ok)
	}

	// Opus modes (2, 3) must return false
	for _, em := range []uint8{2, 3} {
		out, ok := DecodeRxToStereoS16(AudioHeader{Ver: 1, EncodeMode: em, FrameSamplesPerCh: 240}, []byte{1, 2, 3, 4}, 0, scratch)
		if ok || len(out) != 0 {
			t.Fatalf("EncodeMode %d should return len 0, false; got len %d, %v", em, len(out), ok)
		}
	}
}

func TestDecodeRxStrictStereoIsolation(t *testing.T) {
	// Left-only EM1 and Right-only EM1
	em1 := make([]byte, 8)
	binary.LittleEndian.PutUint16(em1[0:2], s16u(400))
	binary.LittleEndian.PutUint16(em1[2:4], 0)
	binary.LittleEndian.PutUint16(em1[4:6], 0)
	binary.LittleEndian.PutUint16(em1[6:8], s16u(-300))

	out1, ok := DecodeRxToStereoS16(AudioHeader{Ver: 1, EncodeMode: EncodeModePCM16, FrameSamplesPerCh: 2}, em1, 0, nil)
	if !ok {
		t.Fatalf("EM1 isolation decode failed")
	}
	want1 := []int16{400 * 32, 0, 0, -300 * 32}
	if !reflect.DeepEqual(out1, want1) {
		t.Fatalf("EM1 stereo isolation = %v, want %v", out1, want1)
	}

	// Left-only EM0 and Right-only EM0
	em0 := make([]byte, 16)
	binary.LittleEndian.PutUint32(em0[0:4], s32u(400<<8))
	binary.LittleEndian.PutUint32(em0[4:8], 0)
	binary.LittleEndian.PutUint32(em0[8:12], 0)
	binary.LittleEndian.PutUint32(em0[12:16], s32u(-300<<8))

	out0, ok := DecodeRxToStereoS16(AudioHeader{Ver: 1, EncodeMode: EncodeModePCM24, FrameSamplesPerCh: 2}, em0, 0, nil)
	if !ok {
		t.Fatalf("EM0 isolation decode failed")
	}
	if !reflect.DeepEqual(out0, want1) {
		t.Fatalf("EM0 stereo isolation = %v, want %v", out0, want1)
	}
}

func TestEncodeTxLeftRightBothParityAndPeak(t *testing.T) {
	leftOnly := []int16{16384, 0, -8192, 0}
	rightOnly := []int16{0, 16384, 0, -8192}
	both := []int16{16384, 16384, -8192, -8192}

	outL, rmsL, peakL := EncodeTxStereoS16ToEM1(leftOnly, 0, nil)
	outR, rmsR, peakR := EncodeTxStereoS16ToEM1(rightOnly, 0, nil)
	outB, rmsB, peakB := EncodeTxStereoS16ToEM1(both, 0, nil)

	if !bytes.Equal(outL, outR) || !bytes.Equal(outL, outB) {
		t.Fatalf("Left/Right/Both parity mismatch: L=%x R=%x B=%x", outL, outR, outB)
	}
	if math.Abs(rmsL-rmsR) > 1e-12 || math.Abs(rmsL-rmsB) > 1e-12 {
		t.Fatalf("RMS parity mismatch: L=%v R=%v B=%v", rmsL, rmsR, rmsB)
	}
	if peakL != 16384 || peakR != 16384 || peakB != 16384 {
		t.Fatalf("Peak parity mismatch: L=%d R=%d B=%d", peakL, peakR, peakB)
	}

	// Verify -32768 peak clamps safely to 32767 and empty input yields 0
	_, rmsMin, peakMin := EncodeTxStereoS16ToEM1([]int16{-32768, 0}, 0, nil)
	if peakMin != 32767 || rmsMin <= 0 {
		t.Fatalf("-32768 handling: rms=%v peak=%d", rmsMin, peakMin)
	}
	outEmpty, rmsEmpty, peakEmpty := EncodeTxStereoS16ToEM1(nil, 0, nil)
	if len(outEmpty) != 0 || rmsEmpty != 0 || peakEmpty != 0 {
		t.Fatalf("empty input: len=%d rms=%v peak=%d", len(outEmpty), rmsEmpty, peakEmpty)
	}
}
