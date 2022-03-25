package gscp

import (
	"context"
	"io"
	"os"

	"github.com/skillian/expr/errors"
)

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

func OpenFilenameCreate(filename string, overwrite bool) (io.WriteCloser, error) {
	if !overwrite {
		if _, err := os.Stat(filename); !os.IsNotExist(err) {
			return nil, errors.Errorf1From(
				err, "refusing to overwrite existing file %v",
				filename,
			)
		}
	}
	f, err := os.Create(filename)
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to open output file %v for writing",
			filename,
		)
	}
	return f, nil
}

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
