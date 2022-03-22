package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/skillian/errors"
)

// Client interacts with the Square 9 REST API.
type Client interface {
	// Close the client and release the license(s) it's consuming.
	Close() error

	// Session gets a session from the client and passes it into function f
	// as a parameter.  Neither the session nor any intermediate API
	// objects may be stored outside of f's scope (e.g. via closure).
	//
	// Many API objects are bound to a single session.  Calling Session
	// does not guarantee (or even try) to give you the same session, so
	// you should do all of your work that you want to do with the session
	// while you have it and only return the final result.
	Session(f func(*Session) error) error
}

// Session is an implementation of the Client interface for a single user.
type Session struct {
	h   *http.Client
	url *url.URL

	basicAuth [2]string
	token     string

	lic *License
	dbs

	buf *bytes.Buffer
}

// SessionOption configures a session
type SessionOption func(s *Session) error

// BasicAuth configures a session to use basic authentication
func BasicAuth(username, password string) SessionOption {
	return func(s *Session) error {
		if s.basicAuth[0] != "" {
			return errRedefined("BasicAuth")
		}
		s.basicAuth = [2]string{username, password}
		return nil
	}
}

// HTTPClient configures the HTTP client that a Session should use.
func HTTPClient(h *http.Client) SessionOption {
	return func(s *Session) error {
		if s.h != nil {
			return errRedefined("HTTPClient")
		}
		s.h = h
		return nil
	}
}

// APIURL specifies the base URL of the Square 9 REST API.
func APIURL(u *url.URL) SessionOption {
	return func(s *Session) error {
		if s.url != nil {
			return errRedefined("APIURL")
		}
		s.url = u
		return nil
	}
}

// APIURLString configures the API URL with a string.
func APIURLString(u string) SessionOption {
	return func(s *Session) error {
		ur, err := url.Parse(u)
		if err != nil {
			return errors.ErrorfWithCause(
				err, "failed to parse %q as URL", s)
		}
		return APIURL(ur)(s)
	}
}

// NewSession creates a new Square 9 session.
func NewSession(ctx context.Context, options ...SessionOption) (*Session, error) {
	s := &Session{}
	for _, opt := range options {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	if s.h == nil {
		s.h = http.DefaultClient
	}
	if s.url == nil {
		return nil, errRequired("APIURL")
	}
	if !strings.HasSuffix(s.url.Path, "/") {
		u := new(url.URL)
		*u = *s.url
		u.Path = u.EscapedPath() + "/"
		s.url = u
	}
	if err := Request(ctx, s, Path("licenses"), ResponseBody(JSONBody{&s.lic})); err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to get license")
	}
	s.token = s.lic.Token.String()
	return s, nil
}

var _ Client = (*Session)(nil)

// Databases gets the databases available via the session.
func (s *Session) Databases(ctx context.Context, f IDAndNamerFilter) (ds []Database, err error) {
	if s.dbs.elems == nil {
		var model Databases
		err = Request(ctx, s, Path("dbs", ""), ResponseBody(JSONBody{&model}))
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to get database list")
		}
		s.dbs.elems = make([]db, len(model.Databases))
		for i, d := range model.Databases {
			s.dbs.elems[i] = db{Database: d}
		}
		s.dbs.lookup = lookupOf(s.dbs.elems)
	}
	ds = ds[:0]
	for _, i := range s.dbs.lookup.lookup(f, nil) {
		ds = append(ds, s.dbs.elems[i].Database)
	}
	return
}

// Archives gets the archives in the database matching the filter.
func (s *Session) Archives(ctx context.Context, d *Database, f IDAndNamerFilter) (ars []Archive, err error) {
	// Make sure we get this session's Database:
	ds, err := s.Databases(ctx, d.ID())
	if err != nil {
		return nil, err
	}
	if err = expectOne(len(ds), d); err != nil {
		return nil, err
	}
	d = &ds[0]
	db, err := s.dbs.db(d.ID())
	if err != nil {
		return nil, err
	}
	if db.archs.elems == nil {
		var model Archives
		err = Request(
			ctx, s, Path(d, "archives"), ResponseBody(JSONBody{&model}))
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to get archives from database %v", d)
		}
		db.archs.elems = make([]arch, len(model.Archives))
		for i, a := range model.Archives {
			db.archs.elems[i] = arch{Archive: a}
		}
		db.archs.lookup = lookupOf(db.archs.elems)
	}
	for _, i := range db.archs.lookup.lookup(f, nil) {
		ars = append(ars, db.archs.elems[i].Archive)
	}
	return
}

// Document retrieves document data associated with the given document.  This
// document must have been returned from a previous call to Search with the
// same session or else it will fail.
func (s *Session) Document(ctx context.Context, d *Database, a *Archive, doc *Document, o DocumentOption, rf io.ReaderFrom) error {
	err := Request(ctx, s,
		Path(d, a, "documents", doc.DocumentID, o),
		SecureID(doc.Hash),
		ResponseBody(rf))
	if err != nil {
		return errors.ErrorfWithCause(err, "failed to get document %v", doc)
	}
	return nil
}

// Import one or more files to an archive with the given fields.
func (s *Session) Import(ctx context.Context, d *Database, a *Archive, fs []ImportField, wts ...io.WriterTo) error {
	id := ImportDocument{
		Fields: fs,
		Files:  make([]ImportFile, len(wts)),
	}
	for i, wt := range wts {
		err := Request(ctx, s, Method(POST), Path("files"), RequestBody(wt), ResponseBody(JSONBody{&id.Files[i]}))
		if err != nil {
			return errors.ErrorfWithCause(err, "error uploading %v", wt)
		}
	}
	return Request(ctx, s, Path(d, a), RequestBody(JSONBody{id}))
}

// Fields gets all the fields in an Archive matching the filter.
// The filter can be nil to get all fields.
func (s *Session) Fields(ctx context.Context, d *Database, a *Archive, f IDAndNamerFilter) (fds []FieldDef, err error) {
	ars, err := s.Archives(ctx, d, a.ID())
	if err != nil {
		return nil, err
	}
	if err = expectOne(len(ars), a); err != nil {
		return nil, err
	}
	a = &ars[0]
	ar, err := s.dbs.arch(d.ID(), a.ID())
	if err != nil {
		return nil, err
	}
	if ar.fields.elems == nil {
		err = Request(
			ctx, s, Path(d, a), Value("type", "fields"),
			ResponseBody(JSONBody{&ar.fields.elems}))
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to get fields from archive %v", a)
		}
		ar.fields.lookup = lookupOf(ar.fields.elems)
	}
	for _, i := range ar.searches.lookup.lookup(f, nil) {
		fds = append(fds, ar.fields.elems[i])
	}
	return
}

// Searches gets the searches associated with an archive
func (s *Session) Searches(ctx context.Context, d *Database, a *Archive, f IDAndNamerFilter) (srs []Search, err error) {
	ars, err := s.Archives(ctx, d, a.ID())
	if err != nil {
		return nil, err
	}
	if err = expectOne(len(ars), a); err != nil {
		return nil, err
	}
	a = &ars[0]
	ar, err := s.dbs.arch(d.ID(), a.ID())
	if err != nil {
		return nil, err
	}
	if ar.searches.elems == nil {
		err = Request(
			ctx, s, Path(d, a, "searches"), ResponseBody(JSONBody{&ar.searches.elems}))
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to get searches from archive %v", a)
		}
		ar.searches.lookup = lookupOf(ar.searches.elems)
		for i := range ar.searches.elems {
			src := &ar.searches.elems[i]
			src.lookup = lookupOf(src.Detail)
		}
	}
	for _, i := range ar.searches.lookup.lookup(f, nil) {
		srs = append(srs, ar.searches.elems[i].Search)
	}
	return
}

// Search executes a search
func (s *Session) Search(ctx context.Context, d *Database, a *Archive, sr *Search, fs []SearchCriterion, options ...RequestOption) (rs Results, err error) {
	srs, err := s.Searches(ctx, d, a, sr.ID())
	if err != nil {
		return
	}
	if err = expectOne(len(srs), sr); err != nil {
		return
	}
	sr = &srs[0]
	src, err := s.dbs.search(d.ID(), a.ID(), sr.ID())
	if err != nil {
		return
	}
	reqOpts := []RequestOption{
		Path(d, sr, "archive", a.ArchiveID, "documents"),
		SecureID(sr.Hash),
		ResponseBody(JSONBody{&rs}),
		nil,
	}[:3]
	if len(fs) > 0 {
		crit := make(map[int]string)
		for _, f := range fs {
			for _, i := range src.lookup.lookup(f.Prompt, nil) {
				p := src.Search.Detail[i]
				crit[p.PromptID] = fmt.Sprint(f.Value)
			}
		}
		bs, err := json.Marshal(crit)
		if err != nil {
			return Results{}, errors.ErrorfWithCause(
				err, "failed to marshal search criteria into "+
					"JSON")
		}
		reqOpts = append(reqOpts, Value("SearchCriteria", string(bs)))
	}
	reqOpts = append(reqOpts, options...)
	err = Request(ctx, s, reqOpts...)
	return
}

// Close the session, releasing the license.
func (s *Session) Close() error {
	return Request(context.Background(), s, Method(DELETE), Path("licenses", s.token))
}

// Session implements the client interface by just passing in itself.
func (s *Session) Session(f func(*Session) error) error {
	return f(s)
}
