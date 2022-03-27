package gscp

import (
	"context"
	"io"
	"os"

	"github.com/skillian/expr/errors"
)

// OpenFilenameRead calls os.Open but wraps the error with more
// context if opening fails.
func OpenFilenameRead(filename string) (io.ReadCloser, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to open file %v for reading",
			filename,
		)
	}
	return f, nil
}

// OpenFilenameCreate calls os.Create but first checks
func OpenFilenameCreate(filename string, overwrite bool) (wc io.WriteCloser, err error) {
	if overwrite {
		wc, err = os.Create(filename)
	} else {
		wc, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	}
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to open output file %v for writing",
			filename,
		)
	}
	return
}

type fileNopCloserWriterTo struct {
	*os.File
}

func (fwt fileNopCloserWriterTo) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, fwt.File)
}

func (fwt fileNopCloserWriterTo) Close() error { return nil }

type readerFromFunc func(r io.Reader) (int64, error)

func (rf readerFromFunc) ReadFrom(r io.Reader) (int64, error) {
	return rf(r)
}

type writerToFunc func(w io.Writer) (int64, error)

func (wt writerToFunc) WriteTo(w io.Writer) (int64, error) {
	return wt(w)
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(b []byte) (n int, err error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(b)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}
