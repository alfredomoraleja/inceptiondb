package persistence

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/golang/snappy"
)

type snappyDriver struct{}

type snappyStore struct {
	path   string
	writer Writer
}

type snappyWriter struct {
	file   *os.File
	buffer *bufio.Writer
}

type snappyReader struct {
	file   *os.File
	buffer []byte
	offset int
}

func init() {
	Register("snappy", &snappyDriver{})
}

func (d *snappyDriver) Open(path string) (Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	f.Close()
	return &snappyStore{path: path}, nil
}

func (d *snappyDriver) Remove(path string) error {
	return os.Remove(path)
}

func (s *snappyStore) Reader() (io.ReadCloser, error) {
	file, err := os.OpenFile(s.path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &snappyReader{file: file}, nil
}

func (s *snappyStore) Writer() (Writer, error) {
	if s.writer != nil {
		return s.writer, nil
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	w := &snappyWriter{
		file:   file,
		buffer: bufio.NewWriterSize(file, 16*1024*1024),
	}
	s.writer = w
	return w, nil
}

func (s *snappyStore) Close() error {
	if s.writer != nil {
		err := s.writer.Close()
		s.writer = nil
		return err
	}
	return nil
}

func (w *snappyWriter) Write(p []byte) (int, error) {
	encoded := snappy.Encode(nil, p)
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(encoded)))
	if _, err := w.buffer.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := w.buffer.Write(encoded); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *snappyWriter) Flush() error {
	return w.buffer.Flush()
}

func (w *snappyWriter) Close() error {
	if err := w.Flush(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

func (r *snappyReader) Read(p []byte) (int, error) {
	for {
		if r.offset < len(r.buffer) {
			n := copy(p, r.buffer[r.offset:])
			r.offset += n
			return n, nil
		}
		header := make([]byte, 4)
		_, err := io.ReadFull(r.file, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return 0, io.EOF
			}
			return 0, err
		}
		length := binary.LittleEndian.Uint32(header)
		if length == 0 {
			r.buffer = nil
			r.offset = 0
			continue
		}
		compressed := make([]byte, length)
		_, err = io.ReadFull(r.file, compressed)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return 0, io.EOF
			}
			return 0, err
		}
		decoded, err := snappy.Decode(nil, compressed)
		if err != nil {
			return 0, err
		}
		r.buffer = decoded
		r.offset = 0
	}
}

func (r *snappyReader) Close() error {
	return r.file.Close()
}
