//go:build !(cgo && linux)

package audio

import "errors"

func openPulseSimpleRecord(device string, sampleRate int) (txCapture, error) {
	return nil, errors.New("pulse-simple record requires cgo on linux")
}
