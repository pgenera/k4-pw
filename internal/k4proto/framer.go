package k4proto

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"strings"
)

const (
	MaxPayloadSize = 262144

	TypeCAT     uint8 = 0x00
	TypeAudio   uint8 = 0x01
	TypePAN     uint8 = 0x02
	TypeMiniPAN uint8 = 0x03

	EncodeModePCM24 uint8 = 0
	EncodeModePCM16 uint8 = 1

	SampleRate12k    int   = 12000
	SampleRateCode0  uint8 = 0
	FrameSamples20ms       = 240
	K4GainBoost            = 32.0
)

var (
	StartMarker = [4]byte{0xFE, 0xFD, 0xFC, 0xFB}
	EndMarker   = [4]byte{0xFB, 0xFC, 0xFD, 0xFE}
)

// AudioHeader describes the 7-byte header of a K4 TypeAudio (0x01) payload.
type AudioHeader struct {
	Ver               uint8
	Seq               uint8
	EncodeMode        uint8
	FrameSamplesPerCh uint16
	SampleRateCode    uint8
}

// ExpectedAuthHex returns the 96-character lowercase SHA-384 hex digest of password.
func ExpectedAuthHex(password string) string {
	sum := sha512.Sum384([]byte(password))
	return hex.EncodeToString(sum[:])
}

// EncodeFrame wraps a payload with K4 start marker, big-endian length, and end marker.
func EncodeFrame(payload []byte) []byte {
	out := make([]byte, 12+len(payload))
	copy(out[0:4], StartMarker[:])
	binary.BigEndian.PutUint32(out[4:8], uint32(len(payload)))
	copy(out[8:8+len(payload)], payload)
	copy(out[8+len(payload):], EndMarker[:])
	return out
}

// EncodeCAT encodes an ASCII CAT command string as a framed TypeCAT (0x00) packet.
func EncodeCAT(catText string) []byte {
	payload := make([]byte, 3+len(catText))
	payload[0] = TypeCAT
	copy(payload[3:], catText)
	return EncodeFrame(payload)
}

// ParseCATPayload extracts the ASCII CAT text from a TypeCAT (0x00) payload.
func ParseCATPayload(payload []byte) (string, bool) {
	if len(payload) < 3 || payload[0] != TypeCAT {
		return "", false
	}
	return string(payload[3:]), true
}

// SplitCATFrames splits semicolon-terminated CAT commands while preserving
// byte-counted TBtrrs; / TB$trrs; payloads containing literal semicolons.
func SplitCATFrames(ascii string) []string {
	var cmds []string
	i := 0
	n := len(ascii)
	for i < n {
		for i < n && (ascii[i] == ';' || ascii[i] == '\r' || ascii[i] == '\n' || ascii[i] == ' ' || ascii[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}
		if strings.HasPrefix(ascii[i:], "TB") {
			hdrLen := 2
			if i+2 < n && ascii[i+2] == '$' {
				hdrLen = 3
			}
			if i+hdrLen+3 <= n {
				tChar := ascii[i+hdrLen]
				r1 := ascii[i+hdrLen+1]
				r2 := ascii[i+hdrLen+2]
				if tChar >= '0' && tChar <= '9' && r1 >= '0' && r1 <= '9' && r2 >= '0' && r2 <= '9' {
					textLen := int(r1-'0')*10 + int(r2-'0')
					end := min(i+hdrLen+3+textLen, n)
					cmds = append(cmds, ascii[i:end])
					i = end
					if i < n && ascii[i] == ';' {
						i++
					}
					continue
				}
			}
		}
		semi := strings.IndexByte(ascii[i:], ';')
		if semi < 0 {
			tail := strings.TrimSpace(ascii[i:])
			if tail != "" {
				cmds = append(cmds, tail)
			}
			break
		}
		cmd := strings.TrimSpace(ascii[i : i+semi])
		if cmd != "" {
			cmds = append(cmds, cmd)
		}
		i += semi + 1
	}
	return cmds
}

// ParseAudioPayload parses a TypeAudio (0x01) payload into its 7-byte header and audio bytes.
func ParseAudioPayload(payload []byte) (AudioHeader, []byte, bool) {
	if len(payload) < 7 || payload[0] != TypeAudio {
		return AudioHeader{}, nil, false
	}
	hdr := AudioHeader{
		Ver:               payload[1],
		Seq:               payload[2],
		EncodeMode:        payload[3],
		FrameSamplesPerCh: binary.LittleEndian.Uint16(payload[4:6]),
		SampleRateCode:    payload[6],
	}
	return hdr, payload[7:], true
}

// EncodeAudioPacket encodes an audio payload with a 7-byte TypeAudio header as a framed K4 packet.
func EncodeAudioPacket(seq uint8, encodeMode uint8, frameSamplesPerCh uint16, audioBytes []byte) []byte {
	payload := make([]byte, 7+len(audioBytes))
	payload[0] = TypeAudio
	payload[1] = 0x01
	payload[2] = seq
	payload[3] = encodeMode
	binary.LittleEndian.PutUint16(payload[4:6], frameSamplesPerCh)
	payload[6] = SampleRateCode0
	copy(payload[7:], audioBytes)
	return EncodeFrame(payload)
}

// StreamFramer parses K4 binary stream frames across arbitrary chunk boundaries.
type StreamFramer struct {
	buf            []byte
	PacketsDecoded uint64
	BadLengths     uint64
	BadEndMarkers  uint64
}

// Push appends chunk to the internal buffer and returns all complete payloads extracted.
func (f *StreamFramer) Push(chunk []byte) [][]byte {
	if len(chunk) > 0 {
		f.buf = append(f.buf, chunk...)
	}
	var out [][]byte
	cursor := 0
	n := len(f.buf)

	for n-cursor >= 4 {
		idx := bytes.Index(f.buf[cursor:], StartMarker[:])
		if idx < 0 {
			keep := min(n-cursor, 3)
			cursor = n - keep
			break
		}
		cursor += idx
		avail := n - cursor
		if avail < 12 {
			break
		}
		payloadLen := binary.BigEndian.Uint32(f.buf[cursor+4 : cursor+8])
		if payloadLen == 0 || payloadLen > MaxPayloadSize {
			f.BadLengths++
			cursor += 4
			continue
		}
		totalLen := 12 + int(payloadLen)
		if avail < totalLen {
			break
		}
		endOffset := cursor + 8 + int(payloadLen)
		if !bytes.Equal(f.buf[endOffset:endOffset+4], EndMarker[:]) {
			f.BadEndMarkers++
			cursor += 4
			continue
		}
		payload := make([]byte, payloadLen)
		copy(payload, f.buf[cursor+8:endOffset])
		out = append(out, payload)
		f.PacketsDecoded++
		cursor += totalLen
	}

	if cursor > 0 {
		remaining := n - cursor
		if remaining > 0 {
			copy(f.buf[0:remaining], f.buf[cursor:n])
		}
		f.buf = f.buf[:remaining]
	}
	return out
}
