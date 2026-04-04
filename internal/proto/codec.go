// Package proto provides length-prefixed msgpack framing over TCP.
//
// Frame layout:
//
//	┌──────────────────────┬────────────────────────────────────┐
//	│  4-byte length (BE)  │  [1-byte type] [msgpack payload]  │
//	└──────────────────────┴────────────────────────────────────┘
package proto

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

const maxFrameSize = 16 * 1024 * 1024 // 16 MB

// WriteFrame encodes payload via msgpack and writes it as a length-prefixed frame.
// The length prefix counts [1-byte type + msgpack payload], not itself.
func WriteFrame(w io.Writer, msgType MessageType, payload interface{}) error {
	body, err := msgpack.Marshal(payload)
	if err != nil {
		return fmt.Errorf("WriteFrame marshal: %w", err)
	}

	frame := make([]byte, 1+len(body))
	frame[0] = byte(msgType)
	copy(frame[1:], body)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(frame)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("WriteFrame write length: %w", err)
	}
	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("WriteFrame write body: %w", err)
	}

	return nil
}

// ReadFrame reads one frame. Returns the type byte and raw msgpack payload.
// Errors: io.EOF (clean close), io.ErrUnexpectedEOF (partial), "frame too large", "empty frame".
func ReadFrame(r io.Reader) (MessageType, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}

	frameLen := binary.BigEndian.Uint32(lenBuf[:])
	if frameLen > maxFrameSize {
		return 0, nil, fmt.Errorf("frame too large: %d bytes", frameLen)
	}
	if frameLen == 0 {
		return 0, nil, fmt.Errorf("empty frame")
	}

	frame := make([]byte, frameLen)
	if _, err := io.ReadFull(r, frame); err != nil {
		return 0, nil, err
	}

	msgType := MessageType(frame[0])
	return msgType, frame[1:], nil
}

// Decode unmarshals raw msgpack bytes into dst.
func Decode(raw []byte, dst interface{}) error {
	return msgpack.Unmarshal(raw, dst)
}
