package driver

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// JSONStream wraps a streaming JSON Lines response.
type JSONStream struct {
	resp   *http.Response
	dec    *json.Decoder
	closed bool
}

// ErrStopIteration signals that a JSON stream iteration should stop without
// treating it as an error.
var ErrStopIteration = errors.New("driver: stop iteration")

func newJSONStream(resp *http.Response) *JSONStream {
	return &JSONStream{
		resp: resp,
		dec:  json.NewDecoder(resp.Body),
	}
}

// Close stops the stream and releases the underlying HTTP resources.
func (s *JSONStream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	return s.resp.Body.Close()
}

// Next decodes the next JSON value from the stream into v. It returns io.EOF
// when no more items are available.
func (s *JSONStream) Next(v any) error {
	if s == nil {
		return errors.New("nil stream")
	}
	if s.closed {
		return io.EOF
	}
	if err := s.dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			s.Close()
			return io.EOF
		}
		s.Close()
		return err
	}
	return nil
}

// Iterate decodes each JSON value into a typed destination and invokes fn for
// every item until the stream ends, fn returns ErrStopIteration or an error is
// encountered. Iterate stops consuming the stream as soon as fn returns an
// error.
func Iterate[T any](s *JSONStream, fn func(*T) error) error {
	if s == nil {
		return errors.New("nil stream")
	}
	if fn == nil {
		return errors.New("nil iterator callback")
	}

	for {
		var item T
		if err := s.Next(&item); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if err := fn(&item); err != nil {
			if errors.Is(err, ErrStopIteration) {
				if cerr := s.Close(); cerr != nil && !errors.Is(cerr, io.EOF) {
					return cerr
				}
				return nil
			}
			s.Close()
			return err
		}
	}
}

// StatusCode returns the HTTP status code associated with the stream.
func (s *JSONStream) StatusCode() int {
	if s == nil || s.resp == nil {
		return 0
	}
	return s.resp.StatusCode
}
