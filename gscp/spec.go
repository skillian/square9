package gscp

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/skillian/interactivity"
	"github.com/skillian/logging"
	"github.com/skillian/square9/internal"
	"github.com/skillian/square9/web"
)

var logger = logging.GetLogger("square9")

const defaultAPIPath = "square9api/api"

type SpecFields map[string]string

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
	Fields SpecFields
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
			return nil, fmt.Errorf(
				"failed to parse %q as a local "+
					"specification: %w",
				s, err,
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
				return nil, fmt.Errorf(
					"failed to unescape username: %v: %w",
					sp.Username[:i], err,
				)
			}
			v2, err := url.PathUnescape(sp.Username[i+1:])
			if err != nil {
				return nil, fmt.Errorf(
					"failed to unescape password: %w",
					err,
				)
			}
			sp.Username, sp.Password = v, v2
		}
	}
	i = strings.IndexByte(s, '/')
	j := strings.IndexByte(s, ':')
	if j == -1 {
		return nil, fmt.Errorf(
			"cannot find database in spec: %q: %w",
			specString, errNoDatabase,
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
		return nil, fmt.Errorf(
			"failed to parse %s as a specification: %w",
			specString, err,
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
			return nil, fmt.Errorf(
				"failed to unescape key: %v: %w",
				s[:j], err,
			)
		}
		v, err := url.QueryUnescape(s[j+1 : i])
		if err != nil {
			return nil, fmt.Errorf(
				"failed to unescape value: %v: %w",
				s[j+1:i], err,
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

type SpecComponent struct {
	Field any
	Get   func(*Spec) any
	Set   func(*Spec, any)
}

// Components returns an iterator over the components of the Spec.
func (sp *Spec) Components() iter.Seq[SpecComponent] {
	return func(yield func(SpecComponent) bool) {
		if !yield(SpecComponent{
			&sp.Username,
			func(sp *Spec) any { return sp.Username },
			func(sp *Spec, v any) { sp.Username = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.Password,
			func(sp *Spec) any { return sp.Password },
			func(sp *Spec, v any) { sp.Password = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.Hostname,
			func(sp *Spec) any { return sp.Hostname },
			func(sp *Spec, v any) { sp.Hostname = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.APIPath,
			func(sp *Spec) any { return sp.APIPath },
			func(sp *Spec, v any) { sp.APIPath = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.Database,
			func(sp *Spec) any { return sp.Database },
			func(sp *Spec, v any) { sp.Database = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.ArchivePath,
			func(sp *Spec) any { return sp.ArchivePath },
			func(sp *Spec, v any) { sp.ArchivePath = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.Search,
			func(sp *Spec) any { return sp.Search },
			func(sp *Spec, v any) { sp.Search = v.(string) },
		}) {
			return
		}
		if !yield(SpecComponent{
			&sp.Fields,
			func(sp *Spec) any { return sp.Fields },
			func(sp *Spec, v any) { sp.Fields = v.(SpecFields) },
		}) {
			return
		}
	}
}

// IsLocal returns true if the specification is a local file specification
func (sp *Spec) IsLocal() bool { return sp.Kind.HasAll(LocalSpec) }

// Eq checks if two Specs are equal.
func (sp *Spec) Eq(b *Spec) bool {
	for component := range sp.Components() {
		left := component.Get(sp)
		right := component.Get(b)
		if leftFields, ok := left.(SpecFields); ok {
			rightFields := right.(SpecFields)
			if len(leftFields) != len(rightFields) {
				return false
			}
			for k, v := range leftFields {
				w, ok := rightFields[k]
				if !ok || v != w {
					return false
				}
			}
		} else if left != right {
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
	maps.Copy(sp2.Fields, sp.Fields)
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
	// IndexOnly will not export documents when the destination is
	// an index file; it will only export the CSV.
	IndexOnly bool

	// AppendIndex is for searches inside of index files
	AppendIndex bool

	// AllowOverwrite allows the destination to be overwritten
	// if it already exists.
	AllowOverwrite bool

	// Unsecure will use HTTP instead of HTTPS.  Its purpose is
	// just for temporary development environments that don't have
	// SSL configured.  Do not use this for test or production
	// systems.
	Unsecure bool

	// WebSessionPoolLimit defines the number of sessions to limit
	// the pool to.
	WebSessionPoolLimit int
}

var errRemoteToRemoteNotSupported = errors.New(
	"copying from a remote source to a remote destination " +
		"is not yet supported",
)

func CopyFromSourceToDestSpec(ctx context.Context, source, dest *Spec, config *Config) (Err error) {
	logger.Info2("copying source %v to %v...", source, dest)
	ctx, created := getOrCreateWebClientMapContext(ctx, false)
	localSource, localDest := source.IsLocal(), dest.IsLocal()
	var sourceClient, destClient web.Client
	if !localSource {
		if sourceClient, Err = getWebClientForSpec(ctx, source); Err != nil {
			Err = fmt.Errorf("getting source client: %w", Err)
		}
	}
	if Err == nil && !localDest {
		if destClient, Err = getWebClientForSpec(ctx, dest); Err != nil {
			Err = fmt.Errorf("getting destination client: %w", Err)
		}
	}
	if created {
		if sourceClient != nil {
			defer internal.Catch(&Err, sourceClient.Close)
		}
		if destClient != nil && destClient != sourceClient {
			defer internal.Catch(&Err, destClient.Close)
		}
		if Err != nil {
			return
		}
	}
	switch {
	case localSource && source.Kind.HasAll(IndexSpec):
		if dest.Kind.HasAll(IndexSpec) {
			config.AppendIndex = true
		}
		return localCSVToDest2(ctx, source, dest, config)
	case localSource && localDest:
		return localCopy(ctx, source, dest, config)
	case localSource && !localDest:
		return localToRemote(ctx, source, dest, config)
	case !localSource && localDest:
		return remoteToLocal(ctx, source, dest, config)
	}
	return errRemoteToRemoteNotSupported
}

// localCopy copies a local file to another local file.  There's really no
// reason to use this program to do that, but I figured it'd be a missing
// edge case if it was just omitted!
func localCopy(ctx context.Context, source, dest *Spec, config *Config) (Err error) {
	sourceFile, err := OpenFilenameRead(source.ArchivePath)
	if err != nil {
		return err
	}
	defer internal.Catch(&Err, sourceFile.Close)
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

func localToRemote(ctx context.Context, source, dest *Spec, config *Config) error {
	if !source.Kind.HasAll(IndexSpec) {
		return singleLocalToRemote(ctx, source, dest, config)
	}
	return localCSVToDest2(ctx, source, dest, config)
}

func singleLocalToRemote(ctx context.Context, source, dest *Spec, config *Config) (Err error) {
	sourceFile, err := OpenFilenameRead(source.ArchivePath)
	if err != nil {
		return err
	}
	defer internal.Catch(&Err, sourceFile.Close)
	return ReadIntoSpecFrom(ctx, sourceFile, dest, config)
}

func remoteToLocal(ctx context.Context, source, dest *Spec, config *Config) error {
	if !dest.Kind.HasAll(IndexSpec) {
		return singleRemoteToLocal(ctx, source, dest, config)
	}
	return remoteSearchToLocalIndex(ctx, source, dest, config)
}

func singleRemoteToLocal(ctx context.Context, source, dest *Spec, config *Config) (Err error) {
	destFile, err := OpenFilenameCreate(dest.ArchivePath, config.AllowOverwrite)
	if err != nil {
		return err
	}
	defer internal.Catch(&Err, destFile.Close)
	return WriteSpecTo(ctx, source, destFile)
}

func remoteSearchToLocalIndex(ctx context.Context, source, dest *Spec, config *Config) (Err error) {
	if source.Search == "" {
		// TODO: Find out if this code ever gets triggered
		// and remove it if not.
		logger.Debug1("source.Search was not set in %s", source)
		if pivot := strings.LastIndexByte(source.ArchivePath, '/'); pivot != -1 {
			source.ArchivePath, source.Search = source.ArchivePath[:pivot], source.ArchivePath[pivot+1:]
		} else {
			// Must be a global search
			source.ArchivePath, source.Search = "", source.ArchivePath
		}
	}
	client, err := getWebClientForSpec(ctx, source)
	if err != nil {
		return err
	}
	{
		archiveDir := filepath.Dir(dest.ArchivePath)
		if err := createDirIfNotExist(archiveDir); err != nil {
			return fmt.Errorf(
				"creating output index file directory "+
					"%s: %w",
				archiveDir, err,
			)
		}
	}
	f, err := func() (*os.File, error) {
		if config.AppendIndex {
			return OpenFilenameAppend(dest.ArchivePath)
		}
		return OpenFilenameCreate(dest.ArchivePath, config.AllowOverwrite)
	}()
	if err != nil {
		return err
	}
	defer internal.Catch(&Err, f.Close)
	csvWriter := csv.NewWriter(f)
	defer internal.Catch(&Err, func() error {
		csvWriter.Flush()
		return csvWriter.Error()
	})
	var exportDir string
	if !config.IndexOnly {
		exportDir = dest.ArchivePath[:len(dest.ArchivePath)-len(filepath.Ext(dest.ArchivePath))]
		if err := createDirIfNotExist(exportDir); err != nil {
			return fmt.Errorf(
				"failed to create export directory "+
					"%v: %w",
				exportDir, err,
			)
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
		logger.Verbose1("search prompt values: %#v", source.Fields)
		for k, v := range source.Fields {
			crit = append(crit, web.SearchCriterion{
				Prompt: web.Name(k),
				Value:  v,
			})
		}
		fieldCount := len(flds) + 1 // document ID
		if !config.IndexOnly {
			fieldCount++ // for filename
		}
		fieldVals := make([]string, fieldCount)
		fieldVals[0] = "Id"
		for i, fld := range flds {
			fieldVals[i+1] = fld.FieldName
		}
		if !config.IndexOnly {
			fieldVals[len(fieldVals)-1] = "Filename"
		}
		if err := csvWriter.Write(fieldVals); err != nil {
			return fmt.Errorf("writing CSV header row: %w", err)
		}
		outputFilename := &fieldVals[len(fieldVals)-1]
		storeDocument := func(
			ctx context.Context, doc *web.Document,
			outputFilename string, config *Config,
		) (Err error) {
			f, err := OpenFilenameCreate(outputFilename, config.AllowOverwrite)
			if err != nil {
				return err
			}
			defer internal.Catch(&Err, f.Close)
			return s.Document(ctx, dbar.db, dbar.arch, doc, web.FileOption, f)
		}
		return iterateSearchResultsPages(
			ctx, s, dbar.db, dbar.arch, &srs[0], crit,
			func(ctx context.Context, doc *web.Document) (Err error) {
				docIDStr := strconv.FormatInt(doc.DocumentID, 10)
				if !config.IndexOnly {
					*outputFilename = filepath.Join(
						exportDir,
						docIDStr+doc.FileType,
					)
					if err := storeDocument(ctx, doc, *outputFilename, config); err != nil {
						return err
					}
				}
				fieldVals[0] = docIDStr
				for i, fld := range flds {
					foundField := false
					for _, docVal := range doc.Fields {
						if docVal.FieldID != fld.FieldID {
							continue
						}
						foundField = true
						fieldVals[i+1] = docVal.Value
						break
					}
					if !foundField {
						fieldVals[i+1] = ""
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
	var res web.Results
	for i := 1; ; i++ {
		res.Fields = res.Fields[:0]
		res.Docs = res.Docs[:0]
		options[len(options)-1] = web.Value("page", i)
		err := s.Search(
			ctx, d, a, search, criteria, &res,
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

func webClientMapFromContext(ctx context.Context) (*webClientMap, error) {
	wcm, ok := ctx.Value((*webClientMap)(nil)).(*webClientMap)
	if !ok {
		return nil, errClientMapNotFound
	}
	return wcm, nil
}

func CreateWebClientMapContext(ctx context.Context) context.Context {
	ctx, _ = getOrCreateWebClientMapContext(ctx, true)
	return ctx
}

func getOrCreateWebClientMapContext(ctx context.Context, mustCreate bool) (out context.Context, created bool) {
	prev, err := webClientMapFromContext(ctx)
	if err == nil && !mustCreate {
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

var errClientMapNotFound = errors.New("web client map not found in context")

// getWebClientForSpec retrieves a web.Client from the context
// for the given Spec.
func getWebClientForSpec(ctx context.Context, sp *Spec) (web.Client, error) {
	wcm, err := webClientMapFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return wcm.getOrCreate(ctx, sp), nil
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
		return v
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
	return web.NewSessionPool(limit, func(ctx context.Context) (s *web.Session, err error) {
		if sp.Password == "" {
			if asker, ok := ctx.Value((*interactivity.Asker)(nil)).(interactivity.Asker); ok {
				sp.Password, err = interactivity.Ask(
					ctx, asker,
					fmt.Sprintf(
						"Password for %s username %s: ",
						sp.String(), sp.Username,
					),
					interactivity.IsSecret(true),
				)
				if err != nil {
					return nil, fmt.Errorf(
						"failed to get %s %v password: %w",
						sp, sp, err,
					)
				}
			}
		}
		if sp.Password == "" {
			return nil, fmt.Errorf(
				"unable to obtain password for %s",
				sp,
			)
		}
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

func ReadIntoSpecFrom(ctx context.Context, r io.Reader, sp *Spec, config *Config) error {
	if sp.IsLocal() {
		return readIntoLocalFile(r, sp.ArchivePath, config)
	}
	client, err := getWebClientForSpec(ctx, sp)
	if err != nil {
		return err
	}
	return client.Session(ctx, func(ctx context.Context, s *web.Session) error {
		return readIntoDocument(ctx, s, r, sp, config)
	})
}

func readIntoLocalFile(r io.Reader, filename string, config *Config) (Err error) {
	f, err := OpenFilenameCreate(filename, config.AllowOverwrite)
	if err != nil {
		return err
	}
	defer internal.Catch(&Err, f.Close)
	_, err = io.Copy(f, r)
	return err
}

func readIntoDocument(ctx context.Context, s *web.Session, r io.Reader, sp *Spec, config *Config) error {
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
	if config.AllowOverwrite {
		if !sp.Kind.HasAll(IndexSpec) || sp.Search == "" {
			return fmt.Errorf(
				"remote destination specification %v "+
					"must be to an index and must "+
					"have a Search when used in "+
					"conjunction with overwrite",
				sp,
			)
		}
		if err = deleteExistingDocuments(ctx, s, sp, dbar); err != nil {
			return err
		}
	}
	wt := createWriterToFromReader(r)
	if err := s.Import(ctx, dbar.db, dbar.arch, flds, wt); err != nil {
		return fmt.Errorf("failed to import %v: %w", sp, err)
	}
	return nil
}

var errSearchRequired = errors.New("search is required")

// deleteExistingDocuments deletes any documents matching the sp
// specification.  sp must have its Search field filled in and that
// search's prompts are filled in with sp's Fields and if only one
// document is returned that matches, it is deleted.
func deleteExistingDocuments(ctx context.Context, s *web.Session, sp *Spec, dbar dbArch) error {
	if sp.Search == "" {
		return fmt.Errorf(
			"%w: cannot delete documents without a search",
			errSearchRequired,
		)
	}
	srs, err := s.Searches(ctx, dbar.db, dbar.arch, web.Name(sp.Search))
	if err != nil {
		return err
	}
	flds, err := s.Fields(ctx, dbar.db, dbar.arch, nil)
	if err != nil {
		return err
	}
	crit := make([]web.SearchCriterion, 0, len(srs[0].Detail))
	for _, fld := range flds {
		fv, ok := sp.Fields[fld.FieldName]
		if !ok {
			continue
		}
		for _, pr := range srs[0].Detail {
			if pr.FieldID != fld.FieldID {
				continue
			}
			if pr.Operator != web.Equals {
				continue
			}
			crit = append(crit, web.SearchCriterion{
				Prompt: pr.ID(),
				Value:  fv,
			})
			break
		}
	}
	var res web.Results
	if err := s.Search(ctx, dbar.db, dbar.arch, &srs[0], crit, &res); err != nil {
		return err
	}
	if len(res.Docs) > 1 {
		return fmt.Errorf(
			"found %d documents when attempting to "+
				"replace %v.  Nothing was replaced",
			len(res.Docs), sp,
		)
	}
	if len(res.Docs) == 1 {
		logger.Info1("deleting existing document %v...", &res.Docs[0])
		return s.DeleteDocument(ctx, dbar.db, dbar.arch, &res.Docs[0])
	}
	return nil
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
		defer internal.Catch(&Err, rc.Close)
		r = contextReader{ctx: ctx, r: rc}
	}
	_, err := io.Copy(w, r)
	return err
}

var errNoDocumentsFound = errors.New("no documents found")

func writeDocumentTo(ctx context.Context, s *web.Session, sp *Spec, w io.Writer) error {
	dbar, err := getDBArch(ctx, s, sp)
	if err != nil {
		return err
	}
	srs, err := s.Searches(ctx, dbar.db, dbar.arch, web.Name(sp.Search))
	if err != nil {
		return err
	}
	if len(srs) == 0 {
		return fmt.Errorf(
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
	var res web.Results
	err = s.Search(ctx, dbar.db, dbar.arch, &srs[0], criteria, &res)
	if err != nil {
		return err
	}
	if len(res.Docs) == 0 {
		return errNoDocumentsFound
	}
	rf := createReaderFromFromWriter(w)
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
		return dbar, fmt.Errorf(
			"failed to get any database with name %q",
			sp.Database,
		)
	}
	ars, err := s.Archives(ctx, &dbs[0], web.Name(sp.ArchivePath))
	if err != nil {
		return dbar, err
	}
	if len(ars) == 0 {
		return dbar, fmt.Errorf(
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
