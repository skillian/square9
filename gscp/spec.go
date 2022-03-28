package gscp

import (
	"context"
	"encoding/csv"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/davecgh/go-spew/spew"
	"github.com/skillian/expr/errors"
	"github.com/skillian/logging"
	"github.com/skillian/square9/web"
	"github.com/skillian/workers"
)

var logger = logging.GetLogger("square9")

const defaultAPIPath = "square9api/api"

// Spec is a specification for a gscp source or destination.  Either
// of which could be a local file or a gscp "pseudo-URI."
type Spec struct {
	// Kind of the specification
	Kind SpecKind

	// Username is the GlobalSearch username
	Username string

	// Password for the GlobalSearch username
	Password string

	// Hostname that the GlobalSearch API server is running on
	Hostname string

	// APIPath is the GlobalSearch "square9api" API path.
	APIPath string

	// Database is the name of the GlobalSearch database.
	Database string

	// ArchivePath is a the name of the archive (or, in the case
	// of "subarchives," a slash-separated path to the subarchive).
	//
	// For Local Specs, ArchivePath holds the local path.
	ArchivePath string

	// Search is an optional search specification.  It is only
	// populated in source specifications.
	Search string

	// Fields holds a collection of GlobalSearch fields for
	// destination specifications or search prompts for source
	// specifications.
	Fields map[string]string
}

// SpecKind defines the kind of item a Spec refers to.
type SpecKind uint8

const (
	// LocalSpec is set when the specification refers to a local
	// path.  When it is set, the Spec's ArchivePath holds the
	// local path.
	LocalSpec SpecKind = 1 << iota

	// IndexSpec is set when the specification refers to an index
	// of other specifications.
	IndexSpec

	// UnsecureSpec allows connecting to the non-local spec
	// with HTTP instead of HTTPS.
	UnsecureSpec
)

const (
	// FieldNameListKey can be specified in Index Spec's Fields to
	// define the field sequence in the index file.  Field names
	// should be separated by FieldNameListSep.
	FieldNameListKey = "fieldlist"

	// FieldNameListSep is the separator between field names in
	// the field list
	FieldNameListSep = ","

	// ArchiveNameKey can be specified in Index Spec's Fields to
	// allow the columns' field names to be looked up from the
	// archive fields.
	ArchiveNameKey = "archive"
)

func (k SpecKind) HasAll(flags SpecKind) bool {
	return k&flags == flags
}

// ParseSpec parses a gscp source or destination specification into a Spec
// structure.
func ParseSpec(specString string) (*Spec, error) {
	s := specString
	isLocalSpec := func(s string) bool {
		i := strings.IndexByte(s, ':')
		if i == -1 {
			// gscp specs must contain a colon before the
			// database.
			return true
		}
		if i == 1 && runtime.GOOS == "windows" {
			// Windows drives are a letter and a colon.
			return true
		}
		return false
	}
	sp := &Spec{}
	if isLocalSpec(s) {
		sp.Kind = LocalSpec
		i := strings.IndexByte(s, '?')
		if i == -1 {
			sp.ArchivePath = s
			sp.Fields = map[string]string{}
			return sp, nil
		} else {
			sp.ArchivePath = s[:i]
			s = s[i+1:]
		}
		fs, err := parseQueryIntoFields(s)
		if err != nil {
			return nil, errors.Errorf1From(
				err, "failed to parse %q as a local specification",
				s,
			)
		}
		sp.Fields = fs
		return sp, nil
	}
	sp.APIPath = defaultAPIPath
	s = strings.TrimPrefix(s, "gscp://")
	i := strings.IndexByte(s, '@')
	if i != -1 {
		sp.Username = s[:i]
		s = s[i+1:]
		if i = strings.IndexByte(sp.Username, ':'); i != -1 {
			v, err := url.PathUnescape(sp.Username[:i])
			if err != nil {
				return nil, errors.Errorf1From(
					err, "failed to unescape username: %v",
					sp.Username[:i],
				)
			}
			v2, err := url.PathUnescape(sp.Username[i+1:])
			if err != nil {
				return nil, errors.Errorf0From(
					err, "failed to unescape password",
				)
			}
			sp.Username, sp.Password = v, v2
		}
	}
	i = strings.IndexByte(s, '/')
	j := strings.IndexByte(s, ':')
	if j == -1 {
		return nil, errors.Errorf1From(
			errNoDatabase, "cannot find database in spec: %q",
			specString,
		)
	}
	if i == -1 {
		sp.Hostname, sp.Database = s[:j], s[j+1:]
		return sp, nil
	}
	if i < j {
		sp.Hostname, sp.APIPath = s[:i], s[i+1:j]
		s = s[j+1:]
		i = strings.IndexByte(s, '/')
		if i == -1 {
			sp.Database = s
			s = ""
		} else {
			sp.Database = s[:i]
			s = s[i+1:]
		}
	} else {
		sp.Hostname, sp.Database = s[:j], s[j+1:i]
		s = s[i+1:]
	}
	if i = strings.IndexByte(s, '?'); i == -1 {
		sp.ArchivePath = s
		return sp, nil
	}
	sp.ArchivePath = s[:i]
	s = s[i+1:]
	fs, err := parseQueryIntoFields(s)
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to parse %s as a specification",
			specString,
		)
	}
	sp.Fields = fs
	return sp, nil
}

func parseQueryIntoFields(s string) (map[string]string, error) {
	// TODO: Maybe use url.ParseQuery instead?
	fs := make(map[string]string, strings.Count(s, "&"))
	for len(s) > 0 {
		i := strings.IndexByte(s, '&')
		if i == -1 {
			i = len(s)
		}
		j := strings.IndexByte(s[:i], '=')
		k, err := url.QueryUnescape(s[:j])
		if err != nil {
			return nil, errors.Errorf1From(
				err, "failed to unescape key: %v", s[:j],
			)
		}
		v, err := url.QueryUnescape(s[j+1 : i])
		if err != nil {
			return nil, errors.Errorf1From(
				err, "failed to unescape value: %v", s[j+1:i],
			)
		}
		fs[k] = v
		if i == len(s) {
			break
		}
		s = s[i+1:]
	}
	return fs, nil
}

// IsLocal returns true if the specification is a local file specification
func (sp *Spec) IsLocal() bool { return sp.Kind.HasAll(LocalSpec) }

// Eq checks if two Specs are equal.
func (sp *Spec) Eq(b *Spec) bool {
	scalars := func(sp *Spec) [7]string {
		return [...]string{
			sp.Username, sp.Password, sp.Hostname,
			sp.APIPath, sp.Database, sp.ArchivePath,
			sp.Search,
		}
	}
	if scalars(sp) != scalars(b) {
		return false
	}
	if len(sp.Fields) != len(b.Fields) {
		return false
	}
	for k, v := range sp.Fields {
		v2, ok := b.Fields[k]
		if !ok {
			return false
		}
		if v != v2 {
			return false
		}
	}
	return true
}

// Copy copies the spec to a new spec and applies zero or more changes
func (sp *Spec) Copy(changes ...func(*Spec)) *Spec {
	sp2 := &Spec{}
	*sp2 = *sp
	sp2.Fields = make(map[string]string, len(sp.Fields))
	for k, v := range sp.Fields {
		sp2.Fields[k] = v
	}
	for _, change := range changes {
		change(sp2)
	}
	return sp2
}

func (sp *Spec) String() string {
	return sp.createString(false)
}

func (sp *Spec) StringWithPassword() string {
	return sp.createString(true)
}

func (sp *Spec) createString(includePassword bool) string {
	if sp.IsLocal() {
		return sp.ArchivePath
	}
	sb := strings.Builder{}
	sb.WriteString("gscp://")
	sb.WriteString(sp.Username)
	if includePassword {
		sb.WriteByte(':')
		sb.WriteString(sp.Password)
	}
	sb.WriteByte('@')
	sb.WriteString(sp.Hostname)
	if sp.APIPath != defaultAPIPath {
		sb.WriteByte('/')
		sb.WriteString(sp.APIPath)
	}
	sb.WriteByte(':')
	sb.WriteString(sp.Database)
	sb.WriteByte('/')
	sb.WriteString(sp.ArchivePath)
	if sp.Search != "" {
		sb.WriteByte('/')
		sb.WriteString(sp.Search)
	}
	b := byte('?')
	for k, v := range sp.Fields {
		sb.WriteByte(b)
		b = '&'
		sb.WriteString(url.QueryEscape(k))
		sb.WriteByte('=')
		sb.WriteString(url.QueryEscape(v))
	}
	return sb.String()
}

type Config struct {
	IndexOnly           bool
	AllowOverwrite      bool
	Unsecure            bool
	WebSessionPoolLimit int
}

func CopyFromSourceToDestSpec(ctx context.Context, source, dest *Spec, config Config) (Err error) {
	if logger.EffectiveLevel() <= logging.VerboseLevel {
		type specNoString Spec
		logger.Verbose4(
			"copying source %v to %v\nsource: %v\ndest: %v",
			source, dest,
			spew.Sdump((*specNoString)(source)),
			spew.Sdump((*specNoString)(dest)),
		)
	}
	ctx, created := getOrCreateWebClientMapContext(ctx, false)
	localSource, localDest := source.IsLocal(), dest.IsLocal()
	var sourceClient, destClient web.Client
	if !localSource {
		sourceClient, _ = getWebClientForSpec(ctx, source)
	}
	if !localDest {
		destClient, _ = getWebClientForSpec(ctx, dest)
	}
	if created {
		if sourceClient != nil {
			defer errors.Catch(&Err, sourceClient.Close)
		}
		if destClient != nil && destClient != sourceClient {
			defer errors.Catch(&Err, destClient.Close)
		}
	}
	switch {
	case localSource && localDest:
		return localCopy(ctx, source, dest, config)
	case localSource && !localDest:
		return localToRemote(ctx, source, dest, config)
	case !localSource && localDest:
		return remoteToLocal(ctx, source, dest, config)
	}
	return errors.Errorf0(
		"copying from a remote source to a remote destination " +
			"is not yet supported",
	)
	// return remoteToRemote(ctx, source, dest, config)
}

// localCopy copies a local file to another local file.  There's really no
// reason to use this program to do that, but I figured it'd be a missing
// edge case if it was just omitted!
func localCopy(ctx context.Context, source, dest *Spec, config Config) (Err error) {
	sourceFile, err := OpenFilenameRead(source.ArchivePath)
	if err != nil {
		return err
	}
	defer errors.Catch(&Err, sourceFile.Close)
	destFile, err := OpenFilenameCreate(dest.ArchivePath, config.AllowOverwrite)
	if err != nil {
		return err
	}
	_, err = io.Copy(contextWriter{
		ctx: ctx,
		w:   destFile,
	}, contextReader{
		ctx: ctx,
		r:   sourceFile,
	})
	return err
}

func localToRemote(ctx context.Context, source, dest *Spec, config Config) error {
	if !source.Kind.HasAll(IndexSpec) {
		return singleLocalToRemote(ctx, source, dest, config)
	}
	return localCSVToRemote(ctx, source, dest, config)
}

func singleLocalToRemote(ctx context.Context, source, dest *Spec, config Config) (Err error) {
	sourceFile, err := OpenFilenameRead(source.ArchivePath)
	if err != nil {
		return err
	}
	defer errors.Catch(&Err, sourceFile.Close)
	return ReadIntoSpecFrom(ctx, sourceFile, dest, config)
}

func localCSVToRemote(ctx context.Context, source, dest *Spec, config Config) error {
	return localCSVToDest(
		ctx, source, dest, func(ctx context.Context, fieldNames, fieldValues []string, config Config) (*Spec, error) {
			return dest.Copy(func(s *Spec) {
				for i, fieldName := range fieldNames {
					if _, ok := s.Fields[fieldName]; !ok {
						s.Fields[fieldName] = fieldValues[i]
					}
				}
			}), nil
		}, config,
	)
}

func localCSVToDest(
	ctx context.Context, source, dest *Spec,
	destFactory func(ctx context.Context, fieldNames, fieldValues []string, config Config) (*Spec, error),
	config Config,
) error {
	fieldNames, err := getFieldNamesForSourceSpec(ctx, source, dest)
	if err != nil {
		return err
	}
	if logger.EffectiveLevel() <= logging.VerboseLevel {
		names := strings.Join(fieldNames, ", ")
		logger.Verbose1("field names: %s", names)
	}
	sourceFile, err := OpenFilenameRead(source.ArchivePath)
	if err != nil {
		return err
	}
	type req struct {
		rowSpec     *Spec
		rowDestSpec *Spec
	}
	limit := config.WebSessionPoolLimit
	if limit < 1 {
		limit = 1
	}
	reqs := make(chan req, limit*3/2)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := workers.Work(ctx, reqs, func(ctx context.Context, req req) (struct{}, error) {
		logger.Info2("copying %s to %s", req.rowSpec, req.rowDestSpec)
		err := CopyFromSourceToDestSpec(ctx, req.rowSpec, req.rowDestSpec, config)
		return struct{}{}, err
	}, workers.WorkerCount(limit))
	resultsDone := make(chan error)
	go func() {
		defer close(resultsDone)
		for result := range results {
			if result.Err != nil {
				cancel()
				resultsDone <- result.Err
			}
		}
	}()
	reader := csv.NewReader(sourceFile)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Errorf1From(
				err, "failed to read row from source "+
					"CSV: %v",
				source.ArchivePath,
			)
		}
		if len(row) != len(fieldNames)+1 {
			return errors.Errorf2(
				"row has %d columns but expected "+
					"exactly %d (which includes "+
					"the filename at the end)",
				len(row), len(fieldNames)+1,
			)
		}
		rowSpec, err := ParseSpec(row[len(row)-1])
		if err != nil {
			return errors.Errorf1From(
				err, "failed to parse row filename "+
					"%v as spec",
				row[len(row)-1],
			)
		}
		rowDestSpec, err := destFactory(ctx, fieldNames, row, config)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			break
		case reqs <- req{rowSpec, rowDestSpec}:
		}
	}
	close(reqs)
	errs := make([]error, 0, config.WebSessionPoolLimit)
	for err := range resultsDone {
		errs = append(errs, err)
	}
	return errors.Aggregate(errs...)
}

func remoteToLocal(ctx context.Context, source, dest *Spec, config Config) error {
	if !dest.Kind.HasAll(IndexSpec) {
		return singleRemoteToLocal(ctx, source, dest, config)
	}
	if !config.IndexOnly {
		return errors.Errorf0(
			"exporting files and documents is not yet " +
				"supported.  Please use GlobalSearch " +
				"Extensions in the mean time",
		)
	}
	return remoteSearchToLocalIndex(ctx, source, dest, config)
}

func singleRemoteToLocal(ctx context.Context, source, dest *Spec, config Config) (Err error) {
	destFile, err := OpenFilenameCreate(dest.ArchivePath, config.AllowOverwrite)
	if err != nil {
		return err
	}
	defer errors.Catch(&Err, destFile.Close)
	return WriteSpecTo(ctx, source, destFile)
}

func remoteSearchToLocalIndex(ctx context.Context, source, dest *Spec, config Config) (Err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client, err := getWebClientForSpec(ctx, source)
	if err != nil {
		return err
	}
	f, err := OpenFilenameCreate(dest.ArchivePath, config.AllowOverwrite)
	if err != nil {
		return err
	}
	defer errors.Catch(&Err, f.Close)
	csvWriter := csv.NewWriter(f)
	defer errors.Catch(&Err, func() error {
		csvWriter.Flush()
		return csvWriter.Error()
	})
	var exportDir string
	if !config.IndexOnly {
		exportDir = dest.ArchivePath[:len(dest.ArchivePath)-len(filepath.Ext(dest.ArchivePath))]
		if _, err := os.Stat(exportDir); err != nil {
			if os.IsNotExist(err) {
				if err = os.MkdirAll(exportDir, 0750); err != nil {
					return errors.Errorf1From(
						err, "failed to create export directory %v",
						exportDir,
					)
				}
			} else {
				return errors.Errorf1From(
					err, "failed to check if export directory %v exists",
					exportDir,
				)
			}
		}
	}
	return client.Session(ctx, func(ctx context.Context, s *web.Session) error {
		dbar, err := getDBArch(ctx, s, source)
		if err != nil {
			return err
		}
		flds, err := s.Fields(ctx, dbar.db, dbar.arch, nil)
		if err != nil {
			return err
		}
		srs, err := s.Searches(ctx, dbar.db, dbar.arch, web.Name(source.Search))
		if err != nil {
			return err
		}
		crit := make([]web.SearchCriterion, 0, len(source.Fields))
		logger.Verbose("search prompt values: %#v", source.Fields)
		for k, v := range source.Fields {
			crit = append(crit, web.SearchCriterion{
				Prompt: web.Name(k),
				Value:  v,
			})
		}
		fieldVals := make([]string, len(flds))
		return iterateSearchResultsPages(
			ctx, s, dbar.db, dbar.arch, &srs[0], crit,
			func(ctx context.Context, doc *web.Document) (Err error) {
				if !config.IndexOnly {
					outputFilename := filepath.Join(
						exportDir,
						strconv.FormatInt(doc.DocumentID, 10)+doc.FileType,
					)
					f, err := OpenFilenameCreate(outputFilename, config.AllowOverwrite)
					if err != nil {
						return err
					}
					defer errors.Catch(&Err, f.Close)
					if err := s.Document(ctx, dbar.db, dbar.arch, doc, web.FileOption, f); err != nil {
						return err
					}
				}
				for i, fld := range flds {
					foundField := false
					for _, docVal := range doc.Fields {
						if docVal.FieldID != fld.FieldID {
							continue
						}
						foundField = true
						fieldVals[i] = docVal.Value
						break
					}
					if !foundField {
						fieldVals[i] = ""
					}
				}
				return csvWriter.Write(fieldVals)
			},
		)
	})
}

func iterateSearchResultsPages(
	ctx context.Context, s *web.Session, d *web.Database,
	a *web.Archive, search *web.Search, criteria []web.SearchCriterion,
	forEach func(context.Context, *web.Document) error,
	options ...web.RequestOption,
) error {
	const recordsPerPage = 1024
	options = append(
		options,
		web.Value("RecordsPerPage", recordsPerPage),
		nil, // replaced with page=i in the loop below
	)
	for i := 1; ; i++ {
		options = append(options[:len(options)-1], web.Value("page", i))
		res, err := s.Search(
			ctx, d, a, search, criteria,
			options...,
		)
		if err != nil {
			return err
		}
		for i := range res.Docs {
			if err := forEach(ctx, &res.Docs[i]); err != nil {
				return err
			}
		}
		if len(res.Docs) < recordsPerPage {
			return nil
		}
	}
}

var (
	errNoDatabase = errors.New("no database found")
)

type webClientKey struct {
	Username string
	Hostname string
	APIPath  string
}

type webClientMap struct {
	mu   sync.Mutex
	m    map[webClientKey]web.Client
	prev *webClientMap
}

func webClientMapFromContext(ctx context.Context) (wcm *webClientMap, ok bool) {
	wcm, ok = ctx.Value((*webClientMap)(nil)).(*webClientMap)
	return
}

func CreateWebClientMapContext(ctx context.Context) context.Context {
	ctx, _ = getOrCreateWebClientMapContext(ctx, true)
	return ctx
}

func getOrCreateWebClientMapContext(ctx context.Context, mustCreate bool) (out context.Context, created bool) {
	prev, ok := webClientMapFromContext(ctx)
	if ok && !mustCreate {
		return ctx, false
	}
	return context.WithValue(
		ctx,
		(*webClientMap)(nil),
		&webClientMap{
			m:    make(map[webClientKey]web.Client, 2),
			prev: prev,
		},
	), true
}

// getWebClientForSpec retrieves a web.Client from the context
// for the given Spec.
func getWebClientForSpec(ctx context.Context, sp *Spec) (web.Client, error) {
	wcm, ok := webClientMapFromContext(ctx)
	if !ok {
		return nil, errors.Errorf0(
			"web client map not found in context",
		)
	}
	return wcm.getOrCreate(ctx, sp), nil
}

func (m *webClientMap) add(sp *Spec, wc web.Client) {
	k := createWebClientKeyFromSpec(sp)
	m.mu.Lock()
	m.m[k] = wc
	m.mu.Unlock()
}

func (m *webClientMap) getOrCreate(ctx context.Context, sp *Spec) web.Client {
	k := createWebClientKeyFromSpec(sp)
	var root *webClientMap
	for m := m; m != nil; m = m.prev {
		root = m
		m.mu.Lock()
		v, loaded := m.m[k]
		m.mu.Unlock()
		if loaded {
			return v
		}
	}
	pool := createWebSessionsFromSpec(ctx, sp)
	root.mu.Lock()
	v, loaded := root.m[k]
	if loaded {
		root.mu.Unlock()
		return v.(web.Client)
	}
	root.m[k] = pool
	root.mu.Unlock()
	return pool
}

func createWebClientKeyFromSpec(sp *Spec) webClientKey {
	return webClientKey{
		Username: sp.Username,
		Hostname: sp.Hostname,
		APIPath:  sp.APIPath,
	}
}

type WebSessionPoolLimit struct{}

func createWebSessionsFromSpec(ctx context.Context, sp *Spec) *web.SessionPool {
	scheme := "https"
	if sp.Kind.HasAll(UnsecureSpec) {
		scheme = scheme[:4]
	}
	limit := 1
	if v, ok := ctx.Value((*WebSessionPoolLimit)(nil)).(int); ok {
		limit = v
	}
	return web.NewSessionPool(limit, func(ctx context.Context) (*web.Session, error) {
		return web.NewSession(
			ctx,
			web.APIURL(&url.URL{
				Scheme: scheme,
				Host:   sp.Hostname,
				Path:   sp.APIPath,
			}),
			web.BasicAuth(
				sp.Username,
				sp.Password,
			),
		)
	})
}

func ReadIntoSpecFrom(ctx context.Context, r io.Reader, sp *Spec, config Config) error {
	if sp.IsLocal() {
		return readIntoLocalFile(ctx, r, sp.ArchivePath, config)
	}
	client, err := getWebClientForSpec(ctx, sp)
	if err != nil {
		return err
	}
	return client.Session(ctx, func(ctx context.Context, s *web.Session) error {
		return readIntoDocument(ctx, s, r, sp)
	})
}

func readIntoLocalFile(ctx context.Context, r io.Reader, filename string, config Config) (Err error) {
	f, err := OpenFilenameCreate(filename, config.AllowOverwrite)
	defer errors.Catch(&Err, f.Close)
	_, err = io.Copy(f, r)
	return err
}

func readIntoDocument(ctx context.Context, s *web.Session, r io.Reader, sp *Spec) error {
	dbar, err := getDBArch(ctx, s, sp)
	if err != nil {
		return err
	}
	flds := make([]web.ImportField, 0, len(sp.Fields))
	for k, v := range sp.Fields {
		flds = append(flds, web.ImportField{
			Name:  k,
			Value: v,
		})
	}
	wt, ok := r.(io.WriterTo)
	if !ok {
		if f, ok := r.(*os.File); ok {
			wt = fileNopCloserWriterTo{f}
		} else {
			wt = writerToFunc(func(w io.Writer) (int64, error) {
				return io.Copy(w, r)
			})
		}
	}
	return s.Import(ctx, dbar.db, dbar.arch, flds, wt)
}

func WriteSpecTo(ctx context.Context, sp *Spec, w io.Writer) error {
	if sp.IsLocal() {
		return writeLocalFileTo(ctx, sp.ArchivePath, w)
	}
	client, err := getWebClientForSpec(ctx, sp)
	if err != nil {
		return err
	}
	return client.Session(ctx, func(ctx context.Context, s *web.Session) error {
		return writeDocumentTo(ctx, s, sp, w)
	})
}

func writeLocalFileTo(ctx context.Context, filename string, w io.Writer) (Err error) {
	var r io.Reader
	if filename == "" || filename == "-" {
		r = contextReader{ctx: ctx, r: os.Stdin}
	} else {
		rc, err := OpenFilenameRead(filename)
		if err != nil {
			return err
		}
		defer errors.Catch(&Err, rc.Close)
		r = contextReader{ctx: ctx, r: rc}
	}
	_, err := io.Copy(w, r)
	return err
}

func writeDocumentTo(ctx context.Context, s *web.Session, sp *Spec, w io.Writer) error {
	dbar, err := getDBArch(ctx, s, sp)
	srs, err := s.Searches(ctx, dbar.db, dbar.arch, web.Name(sp.Search))
	if err != nil {
		return err
	}
	if len(srs) == 0 {
		return errors.Errorf1(
			"failed to find any searches with name %q",
			sp.Search,
		)
	}
	criteria := make([]web.SearchCriterion, 0, len(sp.Fields))
	for k, v := range sp.Fields {
		criteria = append(criteria, web.SearchCriterion{
			Prompt: web.Name(k),
			Value:  v,
		})
	}
	res, err := s.Search(ctx, dbar.db, dbar.arch, &srs[0], criteria)
	if err != nil {
		return err
	}
	if len(res.Docs) == 0 {
		return errors.Errorf0("no documents found")
	}
	rf, ok := w.(io.ReaderFrom)
	if !ok {
		rf = readerFromFunc(func(r io.Reader) (int64, error) {
			return io.Copy(w, r)
		})
	}
	return s.Document(
		ctx, dbar.db, dbar.arch, &res.Docs[0], web.FileOption,
		rf,
	)
}

type dbArch struct {
	db   *web.Database
	arch *web.Archive
}

func getDBArch(ctx context.Context, s *web.Session, sp *Spec) (dbar dbArch, err error) {
	dbs, err := s.Databases(ctx, web.Name(sp.Database))
	if err != nil {
		return dbar, err
	}
	if len(dbs) == 0 {
		return dbar, errors.Errorf1(
			"failed to get any database with name %q",
			sp.Database,
		)
	}
	ars, err := s.Archives(ctx, &dbs[0], web.Name(sp.ArchivePath))
	if err != nil {
		return dbar, err
	}
	if len(ars) == 0 {
		return dbar, errors.Errorf1(
			"failed to get any archives with name %q",
			sp.ArchivePath,
		)
	}
	return dbArch{db: &dbs[0], arch: &ars[0]}, nil
}

// getFieldNamesForSourceSpec figures out the ordered list of field
// names for the source specification.  If the source specification
// has a fieldlist parameter, that field list is used.  Otherwise,
// we use the field list of the source or destination GlobalSearch
// archive.
func getFieldNamesForSourceSpec(ctx context.Context, source, dest *Spec) ([]string, error) {
	if fieldList, ok := source.Fields[FieldNameListKey]; ok {
		return strings.Split(fieldList, FieldNameListSep), nil
	}
	clientSpec := source
	if source.IsLocal() {
		clientSpec = dest
	}
	archiveName, ok := source.Fields[ArchiveNameKey]
	if !ok {
		archiveName = clientSpec.ArchivePath
	}
	wc, err := getWebClientForSpec(ctx, clientSpec)
	if err != nil {
		return nil, err
	}
	var fieldNames []string
	if err = wc.Session(ctx, func(ctx context.Context, s *web.Session) error {
		fieldSpec := new(Spec)
		*fieldSpec = *clientSpec
		fieldSpec.ArchivePath = archiveName
		dbar, err := getDBArch(ctx, s, fieldSpec)
		if err != nil {
			return err
		}
		fs, err := s.Fields(ctx, dbar.db, dbar.arch, nil)
		if err != nil {
			return err
		}
		fieldNames = make([]string, len(fs))
		for i, f := range fs {
			fieldNames[i] = f.FieldName
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return fieldNames, nil
}
