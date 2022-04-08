package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/bits"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/skillian/errors"
	"github.com/skillian/logging"
	"github.com/skillian/workers"
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
	Session(context.Context, func(context.Context, *Session) error) error
}

// Session is an implementation of the Client interface for a single user.
type Session struct {
	h   *http.Client
	url *url.URL

	basicAuth [2]string
	token     string

	lic *License
	dbs

	bufs sync.Pool
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
	s.bufs.New = func() any {
		b := &sessionBuffer{s: s}
		// only use up to 2MiB in memory for temporary files.
		// If the file's bigger than that, switch over to a
		// file.
		b.Grow(2 << 20 /* 2MiB */)
		return b
	}
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
	if err := s.request(
		ctx, Path("licenses"),
		ResponseBody(JSONTo(&s.lic)),
	); err != nil {
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
		err = s.request(ctx, Path("dbs", ""), ResponseBody(JSONTo(&model)))
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
		if err = s.initArchs(ctx, db); err != nil {
			return nil, err
		}
	}
	for _, i := range db.archs.lookup.lookup(f, nil) {
		ars = append(ars, db.archs.elems[i].Archive)
	}
	return
}

// DeleteDocument deletes a document.
func (s *Session) DeleteDocument(ctx context.Context, d *Database, a *Archive, doc *Document) error {
	err := s.request(
		ctx, Path(d, a, doc, "delete"), SecureID(doc.Hash),
	)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to delete document %v", doc,
		)
	}
	return nil
}

// Document retrieves document data associated with the given document.  This
// document must have been returned from a previous call to Search with the
// same session or else it will fail.
func (s *Session) Document(ctx context.Context, d *Database, a *Archive, doc *Document, o DocumentOption, rf io.ReaderFrom) error {
	err := s.request(
		ctx, Path(d, a, "documents", doc.DocumentID, o),
		SecureID(doc.Hash),
		ResponseBody(nopReaderFromCloser{rf}),
	)
	if err != nil {
		return errors.ErrorfWithCause(err, "failed to get document %v", doc)
	}
	return nil
}

// Import one or more files to an archive with the given fields.
func (s *Session) Import(ctx context.Context, d *Database, a *Archive, ifs []ImportField, wts ...io.WriterTo) (Err error) {
	id := ImportDocument{
		Fields: make([]ImportField, len(ifs)),
		Files:  make([]ImportFile, len(wts)),
	}
	cf := newCachedFile()
	defer errors.WrapDeferred(&Err, cf.Close)
	for _, wt := range wts {
		cf.Reset()
		contentType, err := copyIntoMultipartForm(wt, cf)
		if err != nil {
			return err
		}
		if err = s.request(
			ctx, Method(POST),
			Path("files"),
			RequestBody(cf.NopCloser()),
			ContentType(contentType),
			ResponseBody(JSONTo(&id)),
		); err != nil {
			return errors.ErrorfWithCause(err, "error uploading %v", wt)
		}
	}
	if err := s.setImportDocumentFields(ctx, d, a, ifs, &id); err != nil {
		return err
	}
	return s.request(
		ctx, Method(POST), Path(d, a), RequestBody(JSONFrom(id)),
		ContentType("application/json"),
	)
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
		err = s.request(
			ctx, Path(d, a), Value("type", "fields"),
			ResponseBody(JSONTo(&ar.fields.elems)))
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to get fields from archive %v", a)
		}
		ar.fields.lookup = lookupOf(ar.fields.elems)
		if logger.EffectiveLevel() <= logging.VerboseLevel {
			logger.Verbose3(
				"archive %s (ID: %d) fields: %v",
				a.ArchiveName, a.ArchiveID,
				spew.Sdump(ar.fields),
			)
		}
	}
	fieldIndexes := ar.fields.lookup.lookup(f, make([]int, 0, len(ar.fields.elems)))
	sort.Ints(fieldIndexes)
	for _, i := range fieldIndexes {
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
		err = s.request(
			ctx, Path(d, a, "searches"),
			ResponseBody(JSONTo(&ar.searches.elems)),
		)
		if err != nil {
			return nil, errors.ErrorfWithCause(
				err, "failed to get searches from archive %v", a)
		}
		ar.searches.lookup = lookupOf(ar.searches.elems)
		for i := range ar.searches.elems {
			src := &ar.searches.elems[i]
			src.lookup = lookupOf(src.Detail)
		}
		if logger.EffectiveLevel() <= logging.DebugLevel {
			logger.Debug(
				"searches: %v", spew.Sdump(ar.searches),
			)
		}
	}
	for _, i := range ar.searches.lookup.lookup(f, nil) {
		srs = append(srs, ar.searches.elems[i].Search)
	}
	return
}

// Search executes a search
func (s *Session) Search(ctx context.Context, d *Database, a *Archive, sr *Search, fs []SearchCriterion, rs *Results, options ...RequestOption) error {
	return s.executeSearch(ctx, d, a, sr, fs, JSONTo(rs), options...)
}

func (s *Session) executeSearch(ctx context.Context, d *Database, a *Archive, sr *Search, fs []SearchCriterion, r io.ReaderFrom, options ...RequestOption) error {
	srs, err := s.Searches(ctx, d, a, sr.ID())
	if err != nil {
		return err
	}
	if err = expectOne(len(srs), sr); err != nil {
		return err
	}
	sr = &srs[0]
	ar, err := s.dbs.arch(d.ID(), a.ID())
	if err != nil {
		return err
	}
	src, err := s.dbs.search(d.ID(), a.ID(), sr.ID())
	if err != nil {
		return err
	}
	reqOpts := []RequestOption{
		Path(d, sr, "archive", a.ArchiveID, "documents"),
		SecureID(sr.Hash),
		ResponseBody(r),
		nil,
	}[:3]
	if len(fs) > 0 {
		crit, err := s.initSearchCriteria(ctx, ar, src, fs)
		if err != nil {
			return errors.ErrorfWithCause(
				err, "failed to initialize search criteria",
			)
		}
		bs, err := json.Marshal(crit)
		if err != nil {
			return errors.ErrorfWithCause(
				err, "failed to marshal search criteria into "+
					"JSON")
		}
		reqOpts = append(reqOpts, Value("SearchCriteria", string(bs)))
	}
	reqOpts = append(reqOpts, options...)
	return s.request(ctx, reqOpts...)
}

// Close the session, releasing the license.
func (s *Session) Close() error {
	return s.request(
		context.Background(),
		Method(GET),
		Path("licenses", s.token),
	)
}

// Session implements the client interface by just passing in itself.
func (s *Session) Session(ctx context.Context, f func(context.Context, *Session) error) error {
	return f(ctx, s)
}

// request a resource from Square 9
func (s *Session) request(ctx context.Context, options ...RequestOption) (Err error) {
	var r request
	r.base = s.url
	if err := r.init(options); err != nil {
		return err
	}
	if cl, ok := r.resBody.(io.Closer); ok {
		defer errors.WrapDeferred(&Err, cl.Close)
	}
	if cl, ok := r.reqBody.(io.Closer); ok {
		defer errors.WrapDeferred(&Err, cl.Close)
	}

	u, q := r.urlAndQuery()
	if r.secureID != "" {
		q.Set(secureIDQueryKey, r.secureID)
	}
	if s.token != "" {
		q.Set(tokenQueryKey, s.token)
	}
	u.RawQuery = q.Encode()
	url := u.String()

	reqBody, err := r.getReadCloser(s)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "error retrieving request body from request %v",
			r,
		)
	}
	if !equals(reqBody, r.reqBody) {
		defer errors.WrapDeferred(&Err, reqBody.Close)
	}
	logger.Verbose2(
		"request.reqBody: %#[1]v (type: %[1]T), "+
			"reqBody: %#[2]v (type: %[2]T)",
		r.reqBody, reqBody,
	)

	if r.contentType == "" {
		buf := s.bufs.Get().(*sessionBuffer)
		defer errors.WrapDeferred(&Err, buf.Close)
		if _, err := io.Copy(buf, io.LimitReader(reqBody, 512)); err != nil {
			return errors.ErrorfWithCause(
				err, "failed to \"peek\" at request "+
					"body to determine content type",
			)
		}
		logger.Verbose2(
			"check content type of %#[1]v (type: %[1]T) "+
				"of bytes: %[2]s...",
			reqBody,
			buf.Bytes(),
		)
		r.contentType = http.DetectContentType(buf.Bytes())
		logger.Verbose("got content type: %s.", r.contentType)
		reqBody = io.NopCloser(io.MultiReader(buf, reqBody))
	}

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
	if r.contentType != "" {
		req.Header.Set("Content-Type", r.contentType)
	}
	length, err := getContentLength(r.reqBody)
	if err != nil {
		return err
	}
	req.ContentLength = length

	if logger.EffectiveLevel() <= logging.VerboseLevel {
		data, err := httputil.DumpRequestOut(req, true)
		if err != nil {
			logger.LogErr(err)
		} else if len(data) < 1024 {
			logger.Verbose2("executing request %p.  data:\n%s", req, data)
		} else {
			logger.Verbose3(
				"executing request %p.  data (truncated):\n%s\n...\n%s",
				req, data[:512], data[len(data)-512:],
			)
		}
	}
	res, err := s.h.Do(req)
	logger.Verbose3("executed request: %[1]p: (%[2]v, %[3]v)", req, res, err)
	if res != nil && res.Body != nil {
		defer errors.WrapDeferred(&err, res.Body.Close)
	}
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

func (s *Session) initArchs(ctx context.Context, db *db) error {
	var model Archives
	err := s.request(
		ctx, Path(&db.Database, "archives"),
		ResponseBody(JSONTo(&model)),
	)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to get archives from database %v",
			db.Database.DatabaseName,
		)
	}
	pendingArchives := make(chan *Archive, 1024)
	archiveWg := sync.WaitGroup{}
	archiveWg.Add(len(model.Archives))
	const arbitraryConcurrencyLimit = 4
	archivesModels := workers.Work(ctx, pendingArchives, func(ctx context.Context, _ int, a *Archive) (models Archives, err error) {
		var model Archives
		defer archiveWg.Done()
		if err = s.request(
			ctx, Path(&db.Database, a),
			ResponseBody(JSONTo(&model)),
		); err != nil {
			err = errors.ErrorfWithCause(
				err, "failed to get child archives of "+
					"%v (ID: %v)",
				a.ArchiveName, a.ArchiveID,
			)
			return
		}
		for i := range model.Archives {
			ar := &model.Archives[i]
			ar.ArchiveName = strings.Join([]string{
				a.ArchiveName, ar.ArchiveName,
			}, "/")
			archiveWg.Add(1)
			pendingArchives <- ar
		}
		return model, nil
	}, workers.WorkerCount(arbitraryConcurrencyLimit))
	go func() {
		archiveWg.Wait()
		close(pendingArchives)
	}()
	archElems := make([]Archive, 0, 1<<bits.Len(uint(len(model.Archives)*len(model.Archives))))
	for i, a := range model.Archives {
		ar := &model.Archives[i]
		archElems = append(archElems, a)
		pendingArchives <- ar
	}
	err = nil
	for res := range archivesModels {
		if res.Err != nil {
			err = errors.ErrorfWithCauseAndContext(
				res.Err, err, "error encountered "+
					"while attempting to retrieve "+
					"subarchives",
			)
		}
		for _, a := range res.Res.Archives {
			archElems = append(archElems, a)
		}
	}
	db.archs = makeArchs(archElems)
	if logger.EffectiveLevel() <= logging.VerboseLevel {
		logger.Verbose("all archives: %v", spew.Sdump(db.archs))
	}
	return err
}

// setImportDocumentFields initializes an ImportDocument's fields from a set
// of fields.  The field's "name" key actually holds a string representation
// of the field ID, not the field's name.
func (s *Session) setImportDocumentFields(ctx context.Context, d *Database, a *Archive, ifs []ImportField, id *ImportDocument) error {
	flds, err := s.Fields(ctx, d, a, nil)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to get fields for archive %v", a,
		)
	}
	if len(id.Fields) < len(ifs) {
		if cap(id.Fields) < len(ifs) {
			id.Fields = make([]ImportField, len(ifs))
		}
	}
	id.Fields = id.Fields[:len(ifs)]
	for i, f := range ifs {
		fieldFound := false
		for _, fld := range flds {
			if f.Name != fld.FieldName {
				continue
			}
			fieldFound = true
			id.Fields[i] = ImportField{
				Name:  strconv.FormatInt(int64(fld.FieldID), 10),
				Value: f.Value,
			}
			break
		}
		if !fieldFound {
			return errors.Errorf(
				"archive %v has no field %q", a.ArchiveName,
				f.Name,
			)
		}
	}
	return nil
}

func (s *Session) initSearchCriteria(ctx context.Context, ar *arch, src *search, fs []SearchCriterion) (criteria map[int64]string, err error) {
	criteria = make(map[int64]string)
	for _, f := range fs {
		indexes := src.lookup.lookup(f.Prompt, nil)
		if len(indexes) > 0 {
			for _, i := range indexes {
				p := src.Search.Detail[i]
				criteria[p.PromptID] = fmt.Sprint(f.Value)
			}
			continue
		}
		indexes = ar.fields.lookup.lookup(f.Prompt, indexes[:0])
		if len(indexes) == 0 {
			return nil, errors.Errorf(
				"failed to find prompt or field with name/id: %v",
				f.Prompt,
			)
		}
		for _, i := range indexes {
			fld := &ar.fields.elems[i]
			var pr *Prompt
			for j := range src.Detail {
				p := &src.Detail[j]
				if fld.FieldID != p.FieldID {
					continue
				}
				if pr != nil {
					return nil, errors.Errorf(
						"ambiguous use of field %v (ID: %d) as search %v prompt value.  Please use the prompt ID or name instead of the field ID or name.",
						fld.FieldName, fld.FieldID,
						src.SearchName,
					)
				}
				pr = p
			}
			if pr == nil {
				return nil, errors.Errorf(
					"No prompt found in search %v with field %v (ID: %v)",
					src.SearchName,
					fld.FieldName,
					fld.FieldID,
				)
			}
			criteria[pr.PromptID] = fmt.Sprint(f.Value)
		}
	}
	return criteria, nil
}

type sessionBuffer struct {
	s *Session
	bytes.Buffer
}

func (b *sessionBuffer) Close() error {
	b.Buffer.Reset()
	b.s.bufs.Put(b)
	return nil
}
