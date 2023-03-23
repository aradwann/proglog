package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// store - the file we store records in

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// we write the length of the record so that, when we read the record we know how many bytes to read
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// we write to the buffered writer instead of th directly to the file to reduce the number of system calls
	// and improve performance, if a user wrote a lot of small records, this would help
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	// return number of bytes written and the position where the store holds the record in its file
	// the segment will use this position when it creates an associated index entry for this record
	return uint64(w), pos, nil

}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// flush the writer buffer, in case we're about to try to read a record that
	// the buffer hasn't flushed to disk yet
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// find how many bytes we have to read to get the whole record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// fetch and return the record
	b := make([]byte, enc.Uint64(size))

	_, err := s.File.ReadAt(b, int64(pos+lenWidth))

	if err != nil {
		return nil, err
	}
	return b, nil
}

// reads len(p) bytes into p beginning at the off offset in the store's file.
// it implements io.ReaderAt on the store type
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, nil
	}
	return s.File.ReadAt(p, off)
}

// presists any buffered data before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
