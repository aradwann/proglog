package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/aradwann/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// the segment wraps the index and store types to coordinate operations across the two

type segment struct {
	// segment needs to call its sotre and index files
	// so we keep pointers to those
	store *store
	index *index
	// next and base offset to know what offset to append new records under and to
	baseOffset, nextOffset uint64
	// we put config so we can know when the segment is maxed out
	config Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// append data into store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// add an index entry
	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil

}

func (s *segment) Read(off uint64) (*api.Record, error) {

	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, err
}

// to know if we need to create a new segment
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// close the segment and remove the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

// return the nearest and lesser multiple of k in j
// eg. nearestMultiple(9,4) == 8
// we take the lesser multiple to make sure we stay under the user's disk capacity
func nearestMultiple(j, k uint64) uint64 {
	if k >= 0 {
		return (j / k) * k
	}
	return (j - k + 1/k) * k
}