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
func OpenFilenameCreate(filename string, overwrite bool) (f *os.File, err error) {
	if overwrite {
		f, err = os.Create(filename)
	} else {
		f, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	}
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to open output file %v for writing",
			filename,
		)
	}
	return
}

func createDirIfNotExist(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0750); err != nil {
			return errors.Errorf1From(
				err, "failed to create directory: %v",
				path,
			)
		}
	} else if err != nil {
		return errors.Errorf1From(
			err, "failed to check if directory %v exists",
			path,
		)
	}
	return nil
}

type fileNopCloserWriterTo struct {
	*os.File
}

func (fwt fileNopCloserWriterTo) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, fwt.File)
}

func (fwt fileNopCloserWriterTo) Close() error { return nil }

func createReaderFromFromWriter(w io.Writer) io.ReaderFrom {
	if rf, ok := w.(io.ReaderFrom); ok {
		return rf
	}
	return readerFromFunc(func(r io.Reader) (int64, error) {
		n, err := io.Copy(w, r)
		if err != nil {
			err = errors.Errorf2From(
				err, "error while copying %[1]v "+
					"(type: %[1]T) to %[2]v "+
					"(type: %[2]T)",
				r, w,
			)
		}
		return n, err
	})
}

type readerFromFunc func(r io.Reader) (int64, error)

func (rf readerFromFunc) ReadFrom(r io.Reader) (int64, error) {
	return rf(r)
}

func createWriterToFromReader(r io.Reader) io.WriterTo {
	if wt, ok := r.(io.WriterTo); ok {
		return wt
	}
	if f, ok := r.(*os.File); ok {
		return fileNopCloserWriterTo{f}
	}
	return writerToFunc(func(w io.Writer) (int64, error) {
		n, err := io.Copy(w, r)
		if err != nil {
			err = errors.Errorf2From(
				err, "error while copying %[1]v "+
					"(type: %[1]T) to %[2]v "+
					"(type: %[2]T)",
				r, w,
			)
		}
		return n, err
	})
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
