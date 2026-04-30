package web

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/skillian/square9/internal"
)

type cachedFile struct {
	r    io.Reader
	w    io.Writer
	buf  *bytes.Buffer
	file bufferFile
}

func newCachedFile() *cachedFile {
	cf := &cachedFile{buf: buffers.Get().(*bytes.Buffer)}
	cf.r, cf.w = cf.buf, cf.buf
	return cf
}

var buffers = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(
			make([]byte, 0, 2<<20 /* 2 MiB */),
		)
	},
}

func (cf *cachedFile) Read(data []byte) (n int, err error) {
	return cf.r.Read(data)
}

func (cf *cachedFile) WriteTo(w io.Writer) (n int64, err error) {
	if cf.r == cf.buf {
		n, err = cf.buf.WriteTo(w)
	} else {
		n, err = io.Copy(w, cf.r)
	}
	if err != nil {
		err = fmt.Errorf(
			"error writing cached file %v to %v: %w",
			cf, w, err,
		)
	}
	return
}

func (cf *cachedFile) Write(data []byte) (n int, err error) {
	if cf.buf != nil && cf.buf.Len()+len(data) > cf.buf.Cap() {
		f, err := os.CreateTemp("", "square9-cachedFile-")
		if err != nil {
			return 0, fmt.Errorf(
				"failed to \"promote\" cachedFile from "+
					"in-memory buffer to disk: %w",
				err,
			)
		}
		if _, err := io.Copy(f, cf.r); err != nil {
			return 0, internal.MultiError(
				fmt.Errorf(
					"failed to dump in-memory "+
						"buffer to file %v: %w",
					f.Name(), err,
				),
				internal.ErrorfIfNotNil(
					"error closing %v: %w",
					f.Name(), f.Close(),
				),
			)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, internal.MultiError(
				f.Close(),
				fmt.Errorf(
					"failed to rewind cached file "+
						"%v after copying "+
						"in-memory buffer to "+
						"it: %w",
					f.Name(), err,
				),
			)
		}
		cf.r, cf.w = &cf.file, &cf.file
		cf.buf.Reset()
		buffers.Put(cf.buf)
		cf.buf = nil
		cf.file.f = f
	}
	if n, err = cf.w.Write(data); err != nil {
		err = fmt.Errorf(
			"error while writing from "+
				"%[1]v (type: %[1]T): %w",
			cf.w, err,
		)
	}
	return
}

func (cf *cachedFile) Close() error {
	cf.r, cf.w = nil, nil
	if cf.buf != nil {
		cf.buf.Reset()
		buffers.Put(cf.buf)
		cf.buf = nil
	}
	if cf.file.f != nil {
		if err := cf.file.f.Close(); err != nil {
			return fmt.Errorf(
				"failed to close cached file %v: %w",
				cf.file.f.Name(), err,
			)
		}
		if err := os.Remove(cf.file.f.Name()); err != nil {
			return fmt.Errorf(
				"failed to delete temporary file %v: %w",
				cf.file.f.Name(), err,
			)
		}
		cf.file.f = nil
	}
	return nil
}

// Reset the cached file to clear out its contents so that new writes and reads
// start over.
func (cf *cachedFile) Reset() error {
	if cf.buf != nil {
		cf.r, cf.w = cf.buf, cf.buf
		cf.buf.Reset()
		return nil
	}
	if cf.file.f != nil {
		cf.r, cf.w = &cf.file, &cf.file
		if _, err := cf.file.f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf(
				"failed to seek to beginning of "+
					"cached file %v to truncate",
				cf.file.f.Name(),
			)
		}
		if err := cf.file.f.Truncate(0); err != nil {
			return fmt.Errorf(
				"failed to truncate cached file %v: %w",
				cf.file.f.Name(), err,
			)
		}
	}
	return nil
}

func (cf *cachedFile) NopCloser() interface {
	io.ReadWriteCloser
	io.WriterTo
} {
	return cachedFileNopCloser{cf}
}

func (cf *cachedFile) Stat() (fi fs.FileInfo, err error) {
	cfi := cachedFileInfo{cf: cf}
	if cf.file.f != nil {
		cfi.fi, err = cf.file.f.Stat()
		if err != nil {
			return
		}
	}
	return cfi, nil
}

type cachedFileInfo struct {
	cf *cachedFile
	fi fs.FileInfo
}

func (fi cachedFileInfo) Name() string {
	if fi.fi != nil {
		return fi.fi.Name()
	}
	return ""
}

func (fi cachedFileInfo) Size() int64 {
	if fi.fi != nil {
		return fi.fi.Size()
	}
	if fi.cf.buf != nil {
		return int64(fi.cf.buf.Len())
	}
	return 0

}

func (fi cachedFileInfo) Mode() fs.FileMode {
	if fi.fi != nil {
		return fi.fi.Mode()
	}
	return 0
}

func (fi cachedFileInfo) ModTime() time.Time {
	if fi.fi != nil {
		return fi.fi.ModTime()
	}
	return time.Now()
}

func (fi cachedFileInfo) IsDir() bool {
	if fi.fi != nil {
		return fi.fi.IsDir()
	}
	return false
}

func (fi cachedFileInfo) Sys() any {
	if fi.fi != nil {
		return fi.fi.Sys()
	}
	return fi.cf.buf
}

type cachedFileNopCloser struct {
	*cachedFile
}

func (cachedFileNopCloser) Close() error { return nil }

// bufferFile works like a *bytes.Buffer where writing is always to the end
// and reading is always from wherever the last read was.
type bufferFile struct {
	f *os.File
}

func (bf *bufferFile) Read(data []byte) (n int, err error) {
	return bf.f.Read(data)
}

func (bf *bufferFile) Write(data []byte) (n int, err error) {
	var readPos int64
	readPos, err = bf.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		err = fmt.Errorf(
			"failed to determine current buffer file "+
				"offset: %w", err,
		)
		return
	}
	if _, err = bf.f.Seek(0, os.SEEK_END); err != nil {
		err = fmt.Errorf(
			"failed to seek to end of buffer file for "+
				"new write: %w", err,
		)
		return
	}
	if n, err = bf.f.Write(data); err != nil {
		err = fmt.Errorf(
			"failed to write to end of buffer file: %w", err,
		)
		return
	}
	if _, err = bf.f.Seek(readPos, os.SEEK_SET); err != nil {
		err = fmt.Errorf(
			"failed to restore file offset to read "+
				"offset: %w", err,
		)
	}
	return
}

type readCounter struct {
	r io.Reader
	n int64
}

func (r *readCounter) Read(data []byte) (n int, err error) {
	n, err = r.r.Read(data)
	r.n += int64(n)
	return
}

type writeCounter struct {
	w io.Writer
	n int64
}

func (w *writeCounter) Write(data []byte) (n int, err error) {
	n, err = w.w.Write(data)
	w.n += int64(n)
	return
}

type writerToFunc func(w io.Writer) (int64, error)

func (wt writerToFunc) WriteTo(w io.Writer) (int64, error) {
	return wt(w)
}

func determineContentType(r io.Reader) (io.Reader, string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	n, err := io.Copy(buf, io.LimitReader(r, 512))
	if err != nil {
		return nil, "", fmt.Errorf(
			"failed to \"peek\" at %+v content: %w",
			r, err,
		)
	}
	logger.Verbose2("read %[1]d bytes from %#[2]v", n, r)
	data := buf.Bytes()
	contentType := http.DetectContentType(data)
	logger.Verbose2(
		"bytes:\n\t%s\n\tcontent type: %s",
		data, contentType,
	)
	return io.MultiReader(buf, r), contentType, nil
}
