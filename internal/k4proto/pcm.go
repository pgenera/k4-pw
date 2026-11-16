package k4proto

import (
	"encoding/binary"
	"math"
)

// DbToLinear converts a gain in decibels to a linear amplitude multiplier.
func DbToLinear(db float64) float64 {
	return math.Pow(10.0, db/20.0)
}

// SoftClip applies C1-continuous limiting above |x| > 0.9, preserving linearity in [-0.9, 0.9].
func SoftClip(x float64) float64 {
	if math.IsNaN(x) {
		return 0
	}
	ax := math.Abs(x)
	if ax <= 0.9 {
		return x
	}
	return math.Copysign(0.9+0.1*math.Tanh((ax-0.9)/0.1), x)
}

func quantizeS16(v float64) int16 {
	s := math.Round(v * 32768.0)
	if s > 32767 {
		return 32767
	}
	if s < -32768 {
		return -32768
	}
	return int16(s)
}

// DecodeRxToStereoS16 decodes K4 EM0 (24-bit in S32LE) or EM1 (S16LE) interleaved stereo PCM
// with K4's 32x gain boost, optional rxGainDb, and soft limiting into dst (Left=Main, Right=Sub).
func DecodeRxToStereoS16(hdr AudioHeader, data []byte, rxGainDb float64, dst []int16) ([]int16, bool) {
	if hdr.EncodeMode != EncodeModePCM24 && hdr.EncodeMode != EncodeModePCM16 {
		return dst[:0], false
	}

	bytesPerFrame := 4
	if hdr.EncodeMode == EncodeModePCM24 {
		bytesPerFrame = 8
	}
	frameCount := len(data) / bytesPerFrame
	if hdr.FrameSamplesPerCh > 0 && int(hdr.FrameSamplesPerCh) < frameCount {
		frameCount = int(hdr.FrameSamplesPerCh)
	}
	if frameCount == 0 {
		return dst[:0], true
	}

	need := frameCount * 2
	out := dst[:0]
	if cap(out) < need {
		out = make([]int16, need)
	} else {
		out = out[:need]
	}

	gain := K4GainBoost * DbToLinear(rxGainDb)
	if hdr.EncodeMode == EncodeModePCM16 {
		scale := gain / 32768.0
		for i := 0; i < frameCount; i++ {
			off := i * 4
			l := int16(binary.LittleEndian.Uint16(data[off : off+2]))
			r := int16(binary.LittleEndian.Uint16(data[off+2 : off+4]))
			out[i*2] = quantizeS16(SoftClip(float64(l) * scale))
			out[i*2+1] = quantizeS16(SoftClip(float64(r) * scale))
		}
		return out, true
	}

	scale := gain / 8388608.0
	for i := 0; i < frameCount; i++ {
		off := i * 8
		l := int32(binary.LittleEndian.Uint32(data[off : off+4]))
		r := int32(binary.LittleEndian.Uint32(data[off+4 : off+8]))
		out[i*2] = quantizeS16(SoftClip(float64(l) * scale))
		out[i*2+1] = quantizeS16(SoftClip(float64(r) * scale))
	}
	return out, true
}

// EncodeTxStereoS16ToEM1 converts interleaved stereo S16 samples to K4 EM1 (S16LE) dual-mono bytes
// scaled to K4's 1/32 wire convention, returning post-gain RMS [0..1] and raw input peak [0..32767].
func EncodeTxStereoS16ToEM1(stereoS16 []int16, txGainDb float64, dst []byte) (out []byte, rms float64, peak int16) {
	frames := len(stereoS16) / 2
	if frames == 0 {
		return dst[:0], 0, 0
	}

	need := frames * 4
	out = dst[:0]
	if cap(out) < need {
		out = make([]byte, need)
	} else {
		out = out[:need]
	}

	gain := DbToLinear(txGainDb)
	wireScale := 32768.0 / K4GainBoost
	var sumSq float64

	for i := 0; i < frames; i++ {
		l := stereoS16[i*2]
		r := stereoS16[i*2+1]

		absL := int32(l)
		if absL < 0 {
			absL = -absL
		}
		absR := int32(r)
		if absR < 0 {
			absR = -absR
		}
		framePeak := max(absL, absR)
		if framePeak > 32767 {
			framePeak = 32767
		}
		if int16(framePeak) > peak {
			peak = int16(framePeak)
		}

		// Single-channel WSJT-X output (Left or Right) produces the same drive as dual-channel.
		var mono float64
		switch {
		case r == 0:
			mono = float64(l) / 32768.0
		case l == 0:
			mono = float64(r) / 32768.0
		default:
			mono = 0.5 * float64(int32(l)+int32(r)) / 32768.0
		}

		v := SoftClip(mono * gain)
		sumSq += v * v

		q := math.Round(v * wireScale)
		if q > 32767 {
			q = 32767
		} else if q < -32768 {
			q = -32768
		}
		s16 := uint16(int16(q))
		off := i * 4
		binary.LittleEndian.PutUint16(out[off:off+2], s16)
		binary.LittleEndian.PutUint16(out[off+2:off+4], s16)
	}

	return out, math.Sqrt(sumSq / float64(frames)), peak
}
