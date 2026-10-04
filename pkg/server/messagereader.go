package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
)

const defaultMaxMessageSize = 4 << 20

var errMessageTooLarge = errors.New("incoming message exceeds MaxMessageSize")

// messageReader keeps JSON value framing, including historical multiline and
// adjacent values. It owns decoder read-ahead explicitly; each fresh decoder
// sees at most limit bytes plus one byte to distinguish exact size from oversize.
type messageReader struct {
	input   *bufio.Reader
	pending []byte
	limit   int
}

func newMessageReader(input io.Reader, limit int) *messageReader {
	if limit <= 0 {
		limit = defaultMaxMessageSize
	}
	return &messageReader{input: bufio.NewReader(input), limit: limit}
}

func (r *messageReader) Read(p []byte) (int, error) {
	// Bound read-ahead independently of the message limit. In particular, do
	// not retain a large decoder buffer after a large message completes.
	if len(p) > 4096 {
		p = p[:4096]
	}
	if len(r.pending) != 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	return r.input.Read(p)
}

func jsonSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

func (r *messageReader) read() ([]byte, error) {
	// Discard inter-value whitespace with fixed storage. Counting it against
	// the next value would make the budget depend on idle separators/read-ahead.
	for {
		if len(r.pending) != 0 {
			if !jsonSpace(r.pending[0]) {
				break
			}
			r.pending = r.pending[1:]
			continue
		}
		b, err := r.input.ReadByte()
		if err != nil {
			return nil, err
		}
		if !jsonSpace(b) {
			if err := r.input.UnreadByte(); err != nil {
				return nil, err
			}
			break
		}
	}

	budget := &io.LimitedReader{R: r, N: int64(r.limit)}
	probe := &io.LimitedReader{R: r, N: 1}
	// Separate the extra byte to avoid overflowing a maximum positive int.
	dec := json.NewDecoder(io.MultiReader(budget, probe))
	var raw json.RawMessage
	err := dec.Decode(&raw)
	if len(raw) > r.limit {
		return nil, errMessageTooLarge
	}
	if err != nil {
		// A syntax error within budget remains malformed, even if the decoder
		// happened to read ahead. An unfinished value needing byte limit+1
		// is oversize. No truncated value is ever unmarshaled or forwarded.
		var syntax *json.SyntaxError
		if probe.N == 0 && (!errors.As(err, &syntax) || syntax.Offset > int64(r.limit)) {
			return nil, errMessageTooLarge
		}
		return nil, err
	}

	// Buffered is valid only until the decoder is reused. Copy its small
	// read-ahead and drop the decoder; prepend it to any unconsumed pending
	// bytes, without nesting readers across successive messages.
	unread, err := io.ReadAll(dec.Buffered())
	if err != nil {
		return nil, err
	}
	r.pending = append(unread, r.pending...)
	return raw, nil
}
