package gscp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// OpenFilenameRead calls os.Open but wraps the error with more
// context if opening fails.
func OpenFilenameRead(filename string) (io.ReadCloser, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open file %v for reading: %w",
			filename, err,
		)
	}
	return f, nil
}

type File interface {
	Chdir() error
	Chmod(mode os.FileMode) error
	Chown(uid int, gid int) error
	Close() error
	Fd() uintptr
	Name() string
	Read(b []byte) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	ReadDir(n int) ([]os.DirEntry, error)
	ReadFrom(r io.Reader) (n int64, err error)
	Readdir(n int) ([]os.FileInfo, error)
	Readdirnames(n int) (names []string, err error)
	Seek(offset int64, whence int) (ret int64, err error)
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Stat() (os.FileInfo, error)
	Sync() error
	SyscallConn() (syscall.RawConn, error)
	Truncate(size int64) error
	Write(b []byte) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	WriteString(s string) (n int, err error)
	WriteTo(w io.Writer) (n int64, err error)
}

// LockedFile is a hack to deal with parallel websessions attempting
// to open, write, and close the same index file.
type LockedFile struct {
	m        sync.Mutex
	Position int64
	Size     int64
	rc       int
	f        *os.File
}

var lockedFiles = struct {
	mutex sync.Mutex
	files map[string]*LockedFile
}{files: make(map[string]*LockedFile)}

func existingLockedFileOf(filename string) *LockedFile {
	lockedFiles.mutex.Lock()
	if lf, ok := lockedFiles.files[filename]; ok {
		lf.m.Lock()
		if lf.rc > 0 {
			lf.rc++
			lf.m.Unlock()
			lockedFiles.mutex.Unlock()
			return lf
		}
		lf.m.Unlock()
	}
	lockedFiles.mutex.Unlock()
	return nil
}

func lockedFileOf(f *os.File) (*LockedFile, error) {
	lockedFiles.mutex.Lock()
	lf, ok := lockedFiles.files[f.Name()]
	if ok {
		lf.m.Lock()
		if lf.rc > 0 {
			lf.rc++
			lf.m.Unlock()
			lockedFiles.mutex.Unlock()
			if err := f.Close(); err != nil {
				logger.Warn2(
					"closing discarded file %s: %v",
					f.Name(), err,
				)
			}
			return lf, nil
		}
		lf.m.Unlock()
	}
	lf = &LockedFile{f: f, rc: 1}
	lf.m.Lock()
	defer lf.m.Unlock()
	lockedFiles.files[f.Name()] = lf
	lockedFiles.mutex.Unlock()
	fi, err := lf.f.Stat()
	if err != nil {
		return nil, fmt.Errorf(
			"checking initial file size of %v: %w",
			f.Name(), err,
		)
	}
	lf.Size = fi.Size()
	lf.Position, err = f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to determine current position of %v: %w",
			f.Name(), err,
		)
	}
	return lf, nil
}

type LockedFileTellFlag uint8

const (
	// LockedFileTellOmitSize indicates that the file size does
	// not need to be re-queried after an operation on a LockedFile.
	LockedFileTellOmitSize = 1 << iota

	// LockedFileTellOmitPosition indicates that the file seek
	// position does not need to be re-queried after an operation
	// on a LockedFile.
	LockedFileTellOmitPosition

	// LockedFileTellOmit indicates that the LockedFile's cached
	// file seek position and size do not need to be updated.
	LockedFileTellOmit = LockedFileTellOmitSize | LockedFileTellOmitPosition
)

// WithLock locks the LockedFile and executes fn on the underlying
// os.File.  If the function does not update the file seek position or
// size, fn can return bypassTell = true to not update the LockedFile's
// cached info.
func (f *LockedFile) WithLock(fn func(*os.File) (tellFlags LockedFileTellFlag, err error)) error {
	f.m.Lock()
	defer f.m.Unlock()
	tellFlags, err := fn(f.f)
	if tellFlags&LockedFileTellOmitSize == 0 {
		fi, err2 := f.f.Stat()
		if err2 != nil {
			return errors.Join(err, err2)
		}
		f.Size = fi.Size()
	}
	if tellFlags&LockedFileTellOmitPosition == 0 {
		n, err2 := f.f.Seek(0, io.SeekCurrent)
		if err2 != nil {
			return errors.Join(err, err2)
		}
		f.Position = n
	}
	return err
}

func (f *LockedFile) Chdir() error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Chdir()
}
func (f *LockedFile) Chmod(mode os.FileMode) error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Chmod(mode)
}
func (f *LockedFile) Chown(uid int, gid int) error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Chown(uid, gid)
}
func (f *LockedFile) Close() error {
	f.m.Lock()
	defer f.m.Unlock()
	f.rc--
	if f.rc < 0 {
		logger.Error("%v reference count = %d", f.f, f.rc)
	} else if f.rc == 0 {
		err := f.f.Close()
		if err == nil && f.Size == 0 {
			err = os.Remove(f.f.Name())
		}
	}
	return nil
}
func (f *LockedFile) Fd() uintptr {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Fd()
}
func (f *LockedFile) Name() string {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Name()
}
func (f *LockedFile) Read(b []byte) (n int, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.Read(b)
	f.Position += int64(n)
	return
}
func (f *LockedFile) ReadAt(b []byte, off int64) (n int, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.ReadAt(b, off)
	return
}
func (f *LockedFile) ReadDir(n int) ([]os.DirEntry, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.ReadDir(n)
}
func (f *LockedFile) ReadFrom(r io.Reader) (n int64, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.ReadFrom(r)
	f.Position += n
	f.Size = max(f.Size, f.Position)
	return
}
func (f *LockedFile) Readdir(n int) ([]os.FileInfo, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Readdir(n)
}
func (f *LockedFile) Readdirnames(n int) (names []string, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Readdirnames(n)
}
func (f *LockedFile) Seek(offset int64, whence int) (ret int64, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	ret, err = f.f.Seek(offset, whence)
	if err == nil {
		f.Position = ret
	}
	return
}
func (f *LockedFile) SetDeadline(t time.Time) error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.SetDeadline(t)
}
func (f *LockedFile) SetReadDeadline(t time.Time) error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.SetReadDeadline(t)
}
func (f *LockedFile) SetWriteDeadline(t time.Time) error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.SetWriteDeadline(t)
}
func (f *LockedFile) Stat() (os.FileInfo, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Stat()
}
func (f *LockedFile) Sync() error {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.Sync()
}
func (f *LockedFile) SyscallConn() (syscall.RawConn, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.f.SyscallConn()
}
func (f *LockedFile) Truncate(size int64) (err error) {
	f.m.Lock()
	defer f.m.Unlock()
	err = f.f.Truncate(size)
	if err == nil {
		f.Size = size
		f.Position = min(f.Position, f.Size)
	}
	return
}
func (f *LockedFile) Write(b []byte) (n int, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.Write(b)
	f.Position += int64(n)
	f.Size = max(f.Size, f.Position)
	return
}
func (f *LockedFile) WriteAt(b []byte, off int64) (n int, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.WriteAt(b, off)
	f.Size = max(f.Size, f.Position+int64(n))
	return
}
func (f *LockedFile) WriteString(s string) (n int, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.WriteString(s)
	f.Position += int64(n)
	f.Size = max(f.Size, f.Position)
	return
}
func (f *LockedFile) WriteTo(w io.Writer) (n int64, err error) {
	f.m.Lock()
	defer f.m.Unlock()
	n, err = f.f.WriteTo(w)
	f.Position += n
	return
}

// OpenLockedFileAppend opens or reuses an existing currently open
// LockedFile for appending.  This is usually used for appending to
// index files.
func OpenLockedFileAppend(filename string) (*LockedFile, error) {
	filename, err := filepath.Abs(filename)
	if err != nil {
		return nil, fmt.Errorf("getting absolute path to %s: %w", filename, err)
	}
	if lf := existingLockedFileOf(filename); lf != nil {
		return lf, nil
	}
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf(
			"opening file %s for appending: %w",
			filename, err,
		)
	}
	return lockedFileOf(f)
}

// CreateLockedFile creates a LockedFile.  If the file isn't currently
// open, this will create the file.  If the file already exists, this
// function will fail unless overwrite is true.
func CreateLockedFile(filename string, overwrite bool) (*LockedFile, error) {
	filename, err := filepath.Abs(filename)
	if err != nil {
		return nil, fmt.Errorf("getting absolute path to %s: %w", filename, err)
	}
	if overwrite {
		if lf := existingLockedFileOf(filename); lf != nil {
			if err = lf.Truncate(0); err != nil {
				return nil, fmt.Errorf(
					"truncating locked file %v before overwrite: %w",
					filename, err,
				)
			}
			return lf, nil
		}
	}
	var f *os.File
	if overwrite {
		f, err = os.Create(filename)
	} else {
		f, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
		if os.IsExist(err) {
			// someone opened it before we did:
			lf := existingLockedFileOf(filename)
			if lf != nil {
				return lf, nil
			}
			// but didn't open it as a locked file, or
			// it's already closed
		}
	}
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create output file %v for writing: %w",
			filename, err,
		)
	}
	return lockedFileOf(f)
}

func createDirIfNotExist(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0750); err != nil {
			return fmt.Errorf(
				"failed to create directory: %v: %w",
				path, err,
			)
		}
	} else if err != nil {
		return fmt.Errorf(
			"failed to check if directory %v exists: %w",
			path, err,
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
			err = fmt.Errorf(
				"error while copying %[1]v "+
					"(type: %[1]T) to %[2]v "+
					"(type: %[2]T): %w",
				r, w, err,
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
			err = fmt.Errorf(
				"error while copying %[1]v "+
					"(type: %[1]T) to %[2]v "+
					"(type: %[2]T): %w",
				r, w, err,
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

const badWindowsFilenameChars = "<>:\"/\\|?*"

func cleanFilename(name string) string {
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune(badWindowsFilenameChars, r) {
			return '_'
		}
		return r
	}, name)
}
