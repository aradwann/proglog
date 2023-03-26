package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var offWidth uint64 = 4
var posWidth uint64 = 8
var endWidth = offWidth + posWidth

// the index defines our index file, which comprises a presisted file and a memory-mapped file.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	// create the index
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// save the current size of the file so we can track the amount
	// of data in the indexfile as we add index entries
	idx.size = uint64(fi.Size())
	// we grow the file to the max index size before memory-mapping the file
	// because we memory-map the file to a slice of bytes and if we didn't
	// increase the size of the file before we wrote to it,
	// we'd get out-of-bounds error
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	// return the created index to the caller
	return idx, nil
}

func (i *index) Close() error {
	// make sure the memory-mapped file has synced its data to the presisted file
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}
	// and the presisted file has flushed its contents to stable storage
	if err := i.file.Sync(); err != nil {
		return err
	}
	// then truncates the presisted file to the amount of data that's actually in it
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / endWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * endWidth
	if i.size < pos+endWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+endWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// validate that we have space to write the entry
	if uint64(len(i.mmap)) < i.size+endWidth {
		return io.EOF
	}
	// encode the offset and the position and write them to the memory-mapped file
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+endWidth], pos)

	// then we increment the position where the next write will go
	i.size += uint64(endWidth)
	return nil
}

// to return the index file path
func (i *index) Name() string {
	return i.file.Name()
}
