package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/skillian/errors"
)

const (
	tokenQueryKey    = "Token"
	secureIDQueryKey = "SecureId"
)

// request is a Square 9-specific REST request
type request struct {
	err         error
	method      HTTPMethod
	secureID    string
	path        []string
	values      [][]string
	reqBody     io.WriterTo
	resBody     io.ReaderFrom
	contentType string
	base        *url.URL
}

// RequestOption configures a Square 9 request.
type RequestOption func(r *request) error

// ContentType sets the content type of the request
func ContentType(t string) RequestOption {
	return func(r *request) error {
		r.contentType = t
		return nil
	}
}

// Method defines the HTTPMethod to use for a Request.
func Method(m HTTPMethod) RequestOption {
	return func(r *request) error {
		if r.method != "" {
			return errRedefined("Method")
		}
		r.method = m
		return nil
	}
}

// Path defines the Request path
func Path(segments ...interface{}) RequestOption {
	return func(r *request) error {
		for _, v := range segments {
			switch v := v.(type) {
			case string:
				r.path = append(r.path, url.PathEscape(v))
			case interface{ PathSegments() []string }:
				for _, v2 := range v.PathSegments() {
					r.path = append(
						r.path, url.PathEscape(v2))
				}
			default:
				s := fmt.Sprint(v)
				r.path = append(r.path, url.PathEscape(s))
			}
		}
		return nil
	}
}

// RequestBody specifies the Body to use for the request
func RequestBody(wt io.WriterTo) RequestOption {
	return func(r *request) error {
		if r.reqBody != nil {
			return errRedefined("RequestBody")
		}
		r.reqBody = wt
		return nil
	}
}

// ResponseBody specifies the Body to use for the response
func ResponseBody(rf io.ReaderFrom) RequestOption {
	return func(r *request) error {
		if r.resBody != nil {
			return errRedefined("ResponseBody")
		}
		r.resBody = rf
		return nil
	}
}

// SecureID configures the SecureID to use with this request.  It usually comes
// from a "Hash" or "SecureId" field of a previously-requested object.
func SecureID(hash string) RequestOption {
	return func(r *request) error {
		// TODO: Maybe this shouldn't produce an error?
		if r.secureID != "" {
			return errRedefined("SecureID")
		}
		r.secureID = hash
		return nil
	}
}

// Value adds a Value (either part of the Query string or POST values,
// depending on the Method) to the request.
func Value(k string, vs ...interface{}) RequestOption {
	return func(r *request) error {
		strs := make([]string, 1, len(vs)+1)
		strs[0] = k
		for _, v := range vs {
			strs = append(strs, fmt.Sprint(v))
		}
		r.values = append(r.values, strs)
		return nil
	}
}

func (r *request) init(options []RequestOption) error {
	for _, opt := range options {
		if err := opt(r); err != nil {
			return err
		}
	}
	if r.method == "" {
		r.method = GET
	}
	return nil
}

// urlAndQuery returns a URL and the query values parsed out of it.  The values
// have to be Encoded and assigned to the URL's (Raw)Query field before the
// String() function is called on the URL.
func (r *request) urlAndQuery() (u *url.URL, q url.Values) {
	rel := *r.base
	rel.Path = rel.EscapedPath() + strings.Join(r.path, "/")
	u = r.base.ResolveReference(&rel)
	q = u.Query()
	for _, vs := range r.values {
		var k string
		k, vs = vs[0], vs[1:]
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	return u, q
}

func (r *request) getReadCloser(s *Session) (rc io.ReadCloser, Err error) {
	if r.reqBody == nil {
		return http.NoBody, nil
	}
	if rc, ok := r.reqBody.(io.ReadCloser); ok {
		return rc, nil
	}
	if r, ok := r.reqBody.(io.Reader); ok {
		return io.NopCloser(r), nil
	}
	cf := newCachedFile()
	if _, err := r.reqBody.WriteTo(cf); err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to copy request body into cached file",
		)
	}
	return cf, nil
}

// Body is the body of a request or response.
type Body interface {
	// WriteTo the *http.Request's body before sending to the server
	WriteTo(w io.Writer) (n int64, err error)

	// ReadFrom the *http.Response back from the server.
	ReadFrom(r io.Reader) (n int64, err error)
}

// JSONBodyFrom returns an io.ReadCloser created from JSON marshalled
// from v.
func JSONFrom(v interface{}) io.WriterTo {
	data, err := json.Marshal(v)
	if err != nil {
		return errWriterTo{err}
	}
	return bytes.NewReader(data)
}

// JSONBodyTo returns a WriteCloser that when closed will unmarshal
// the content into the target.
func JSONTo(v interface{}) io.ReaderFrom {
	b := jsonResponseBodies.Get().(*jsonResponseBody)
	b.V = v
	return b
}

type jsonResponseBody struct {
	V interface{}
	bytes.Buffer
}

var jsonResponseBodies = sync.Pool{
	New: func() interface{} {
		return &jsonResponseBody{}
	},
}

func (b *jsonResponseBody) Close() error {
	err := json.Unmarshal(b.Buffer.Bytes(), b.V)
	b.Buffer.Reset()
	b.V = nil
	jsonResponseBodies.Put(b)
	return err
}

type errWriterTo struct {
	err error
}

func (ewt errWriterTo) WriteTo(w io.Writer) (int64, error) {
	return 0, ewt.err
}

func (ewt errWriterTo) Close() error { return nil }

// ReaderBody implements io.WriterTo by copying to the reader.
type ReaderBody struct {
	io.Reader
}

// WriteTo the writer by copying from the reader.
func (b ReaderBody) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := b.Reader.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, b)
}

// HTTPMethod is a type-safe HTTP method
type HTTPMethod string

const (
	// DELETE HTTP method
	DELETE HTTPMethod = http.MethodDelete

	// GET HTTP method
	GET HTTPMethod = http.MethodGet

	// POST HTTP method
	POST HTTPMethod = http.MethodPost
)

func httpStatusOK(code int) bool {
	return 200 <= code && code < 300
}

type nopReaderFromCloser struct {
	io.ReaderFrom
}

func (c nopReaderFromCloser) Close() error { return nil }

type limitWriter struct {
	w io.Writer
	n int64
}

func (w *limitWriter) Write(p []byte) (int, error) {
	limit := len(p)
	if int64(limit) > w.n {
		limit = int(w.n)
	}
	n, err := w.w.Write(p[:limit])
	w.n -= int64(n)
	if err != nil {
		return n, err
	}
	if limit < len(p) {
		err = (*limitedWrite)(nil)
	}
	return n, err
}

type limitedWrite struct{}

func (*limitedWrite) Error() string { return "limited write" }

type tempFile struct {
	*os.File
}

func (f tempFile) Close() error {
	if err := f.File.Close(); err != nil {
		return errors.ErrorfWithCause(
			err, "error attempting to close %v", f.File,
		)
	}
	if err := os.Remove(f.File.Name()); err != nil {
		return errors.ErrorfWithCause(
			err, "failed to remove temporary file: %v",
			f.File.Name(),
		)
	}
	return nil
}

// copyIntoMultipartForm copies an io.WriterTo into a multipart/form-data form
// which is then written into w.
func copyIntoMultipartForm(wt io.WriterTo, w io.Writer) (contentType string, err error) {
	form := multipart.NewWriter(w)
	h := make(textproto.MIMEHeader)
	length, err := getContentLength(wt)
	if err != nil {
		return "", err
	}
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	contentType = "application/octet-stream"
	if r, ok := wt.(io.Reader); ok {
		r, contentType, err = determineContentType(r)
		if err != nil {
			return "", err
		}
		wt = writerToFunc(func(w io.Writer) (int64, error) {
			return io.Copy(w, r)
		})
	}
	h.Set("Content-Type", contentType)
	ext := ""
	exts, err := mime.ExtensionsByType(contentType)
	if err != nil {
		return "", errors.ErrorfWithCause(
			err, "failed to get extension for content type %v",
			contentType,
		)
	}
	if len(exts) > 0 {
		ext = exts[0]
	}
	h.Set(
		"Content-Disposition",
		fmt.Sprintf(
			`form-data; name="File"; filename="File%s"`,
			ext,
		),
	)
	w, err = form.CreatePart(h)
	if err != nil {
		return "", errors.ErrorfWithCause(
			err, "failed to create form file",
		)
	}
	if _, err := wt.WriteTo(w); err != nil {
		return "", errors.ErrorfWithCause(
			err, "failed to write import file into "+
				"multipart message",
		)
	}
	if err := form.Close(); err != nil {
		return "", errors.ErrorfWithCause(
			err, "failed to close multipart message",
		)
	}
	return form.FormDataContentType(), nil
}

func getContentLength(wt io.WriterTo) (int64, error) {
	if sr, ok := wt.(interface{ Stat() (fs.FileInfo, error) }); ok {
		st, err := sr.Stat()
		if err != nil {
			return 0, errors.ErrorfWithCause(
				err, "failed to stat request body %+v to "+
					"get content length",
				wt,
			)
		}
		return st.Size(), nil
	}
	if lr, ok := wt.(interface{ Len() int }); ok {
		return int64(lr.Len()), nil
	}
	return 0, nil
}
