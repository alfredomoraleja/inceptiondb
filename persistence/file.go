package persistence

import (
	"bufio"
	"io"
	"os"
)

type fileDriver struct{}

type fileStore struct {
	path   string
	writer Writer
}

type fileWriter struct {
	file   *os.File
	buffer *bufio.Writer
}

func init() {
	Register(DefaultDriverName, &fileDriver{})
}

func (d *fileDriver) Open(path string) (Store, error) {
	// Ensure file exists before opening readers or writers.
	f, err := os.OpenFile(path, os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	f.Close()
	return &fileStore{path: path}, nil
}

func (d *fileDriver) Remove(path string) error {
	return os.Remove(path)
}

func (s *fileStore) Reader() (io.ReadCloser, error) {
	return os.OpenFile(s.path, os.O_RDONLY, 0666)
}

func (s *fileStore) Writer() (Writer, error) {
	if s.writer != nil {
		return s.writer, nil
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	w := &fileWriter{
		file:   file,
		buffer: bufio.NewWriterSize(file, 16*1024*1024),
	}
	s.writer = w
	return w, nil
}

func (s *fileStore) Close() error {
	if s.writer != nil {
		err := s.writer.Close()
		s.writer = nil
		return err
	}
	return nil
}

func (w *fileWriter) Write(p []byte) (int, error) {
	return w.buffer.Write(p)
}

func (w *fileWriter) Flush() error {
	return w.buffer.Flush()
}

func (w *fileWriter) Close() error {
	if err := w.Flush(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}
