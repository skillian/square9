package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/skillian/errors"
)

const (
	tokenQueryKey    = "Token"
	secureIDQueryKey = "SecureId"
)

// request is a Square 9-specific REST request
type request struct {
	err      error
	method   HTTPMethod
	secureID string
	path     []string
	values   [][]string
	reqBody  io.WriterTo
	resBody  io.ReaderFrom
	base     *url.URL
}

// RequestOption configures a Square 9 request.
type RequestOption func(r *request) error

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

// Request a resource from Square 9
func Request(ctx context.Context, s *Session, options ...RequestOption) (Err error) {
	var r request
	r.base = s.url
	for _, o := range options {
		if Err = o(&r); Err != nil {
			return
		}
	}
	if r.method == "" {
		r.method = GET
	}

	u, q := r.URLAndQuery()
	if r.secureID != "" {
		q.Set(secureIDQueryKey, r.secureID)
	}
	if s.token != "" {
		q.Set(tokenQueryKey, s.token)
	}
	u.RawQuery = q.Encode()
	url := u.String()
	logger.Verbose2("creating HTTP %v request for URL: %q...", r.method, url)
	reqBody, err := r.reqReadCloser(s)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "error retrieving request body from request %v",
			r,
		)
	}
	defer errors.WrapDeferred(&Err, reqBody.Close)
	req, err := http.NewRequestWithContext(
		ctx, string(r.method), url, reqBody,
	)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to create new HTTP request for "+
				"%q", url)
	}
	if s.basicAuth[0] != "" {
		req.SetBasicAuth(s.basicAuth[0], s.basicAuth[1])
	}
	logger.Verbose1("executing request %[1]p: %[1]#v ...", req)
	res, err := s.h.Do(req)
	logger.Verbose3("executed request: %[1]p: %[1]#v -> (%[2]v, %[3]v)", req, res, err)
	defer errors.WrapDeferred(&err, res.Body.Close)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.Errorf(
			"non-success response code: %d - %s",
			res.StatusCode, res.Status)
	}
	if r.resBody != nil {
		if _, err = r.resBody.ReadFrom(res.Body); err != nil {
			return errors.ErrorfWithCause(
				err, "error while handling response body")
		}
	}
	return
}

// URLAndQuery returns a URL and the query values parsed out of it.  The values
// have to be Encoded and assigned to the URL's (Raw)Query field before the
// String() function is called on the URL.
func (r *request) URLAndQuery() (u *url.URL, q url.Values) {
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

func (r *request) reqReadCloser(s *Session) (io.ReadCloser, error) {
	if r.reqBody == nil {
		// TODO: Is this the right thing to do?
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if rc, ok := r.reqBody.(io.ReadCloser); ok {
		return rc, nil
	}
	if r, ok := r.reqBody.(io.Reader); ok {
		return io.NopCloser(r), nil
	}
	if s.buf == nil {
		s.buf = bytes.NewBuffer(make([]byte, 0, 2<<20 /* 2 MiB */))
	}
	s.buf.Reset()
	// only use up to 2MiB in memory for temporary files.  If the
	// file's bigger than that, switch over to a file.
	var w io.Writer = &limitWriter{s.buf, int64(s.buf.Cap())}
	_, err := r.reqBody.WriteTo(w)
	if err == nil {
		return io.NopCloser(s.buf), nil
	}
	if _, ok := err.(*limitedWrite); ok {
		f, err := os.CreateTemp("", "")
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "error attempting to create "+
					"temporary file",
			)
		}
		if sr, ok := r.reqBody.(io.Seeker); ok {
			if _, err := sr.Seek(0, io.SeekStart); err != nil {
				return nil, errors.ErrorfWithCause(
					err, "failed to rewind "+
						"request body",
				)
			}
		}
		if _, err := r.reqBody.WriteTo(f); err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to write request body "+
					"to temporary file",
			)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to rewind temporary "+
					"file after loading from "+
					"request body",
			)
		}
		return tempFile{f}, nil
	}
	return nil, errors.ErrorfWithCause(
		err, "failed to read request body",
	)
}

// Body is the body of a request or response.
type Body interface {
	// WriteTo the *http.Request's body before sending to the server
	WriteTo(w io.Writer) (n int64, err error)

	// ReadFrom the *http.Response back from the server.
	ReadFrom(r io.Reader) (n int64, err error)
}

// JSONBody implements the Body interface by (un)marshaling requests and
// responses to/from the server.
type JSONBody struct {
	// V is a value that is either marshaled into or from.
	V interface{}
}

// WriteTo implements the WriterTo interface.
func (b JSONBody) WriteTo(w io.Writer) (int64, error) {
	bs, err := json.Marshal(b.V)
	if err != nil {
		return 0, err
	}
	i, err := w.Write(bs)
	return int64(i), err
}

// ReadFrom implements the ReaderFrom interface.
func (b JSONBody) ReadFrom(r io.Reader) (int64, error) {
	bs, err := ioutil.ReadAll(r)
	length := int64(len(bs))
	if err != nil {
		return length, err
	}
	if err = json.Unmarshal(bs, b.V); err != nil {
		return length, errors.ErrorfWithCause(
			err, "failed to unmarshal %q as JSON", bs)
	}
	return length, err
}

// ReaderBody implements io.WriterTo by copying to the reader.
type ReaderBody struct {
	io.Reader
}

// WriteTo the writer by copying from the reader.
func (b ReaderBody) WriteTo(w io.Writer) (int64, error) {
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
	return code >= 200 && code < 300
}

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
