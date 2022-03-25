package web

import (
	"strconv"
	"time"

	"github.com/google/uuid"
)

// License is returned when requesting a license from Square 9
type License struct {
	Username     string    `json:"Username"`
	Domain       string    `json:"Domain"`
	AuthServer   string    `json:"AuthServer"`
	Type         int       `json:"Type"`
	IPAddress    string    `json:"IPAddress"`
	DateCreated  time.Time `json:"DateCreated"`
	DateAccessed time.Time `json:"DateAccessed"`
	Token        uuid.UUID `json:"Token"`
	Reg          int       `json:"Reg"`
}

// Databases is a useless outer wrapper around the response that comes from
// getting a list of databases from Square 9
type Databases struct {
	Databases []Database `json:"Databases"`
}

// Database defines a Square 9 database
type Database struct {
	DatabaseID   int    `json:"Id"`
	DatabaseName string `json:"Name"`
}

// PathSegments is used by Request to know how to insert this object into a URL.
func (d *Database) PathSegments() []string {
	return []string{
		"dbs",
		strconv.Itoa(d.DatabaseID),
	}
}

// ID gets an ID of the resource.
func (d *Database) ID() ID { return ID(d.DatabaseID) }

// Name gets the name of the resource.
func (d *Database) Name() Name { return Name(d.DatabaseName) }

// Archive in Square 9
type Archive struct {
	ArchiveID   int    `json:"Id"`
	ArchiveName string `json:"Name"`
	Parent      int    `json:"Parent"`
	Permissions int    `json:"Permissions"`
	Properties  int    `json:"Properties"`
}

// ID gets an ID of the resource.
func (a *Archive) ID() ID { return ID(a.ArchiveID) }

// Name gets the name of the resource.
func (a *Archive) Name() Name { return Name(a.ArchiveName) }

// PathSegments is used by Request to know how to insert this object into a URL.
func (a *Archive) PathSegments() []string {
	return []string{
		"archives",
		strconv.Itoa(a.ArchiveID),
	}
}

// Archives is a useless outer wrapper around the response that comes from
// getting a list of archives from Square 9
type Archives struct {
	Archives []Archive `json:"Archives"`
}

// Search in a Database.
type Search struct {
	Archives   []Archive
	SearchID   int    `json:"Id"`
	SearchName string `json:"Name"`
	Parent     int
	Hash       string
	Detail     []Prompt
	Props      int
	Fuzzy      int
	Grouping   string
	Settings   int
}

// ID gets the ID of the search
func (s *Search) ID() ID { return ID(s.SearchID) }

// Name gets the name of the search
func (s *Search) Name() Name { return Name(s.SearchName) }

// PathSegments is used by Request to know how to insert this object into a URL.
func (s *Search) PathSegments() []string {
	return []string{
		"searches",
		strconv.Itoa(s.SearchID),
	}
}

// Prompt is a single prompt in a Search.
type Prompt struct {
	PromptID int `json:"ID"`
	FieldID  int `json:"FID"`
	ListID   int
	Parent   int
	Operator int
	Prompt   string
	Value    string `json:"VAL"`
	Prop     int
}

// ID gets the ID of the prompt
func (p *Prompt) ID() ID { return ID(p.PromptID) }

// Name gets the string prompt used in the search
func (p *Prompt) Name() Name { return Name(p.Prompt) }

// Results are returned by searches
type Results struct {
	Fields []FieldDef
	Docs   []Document
	Count  int
}

// FieldDef is the definition of an Archive field.
type FieldDef struct {
	FieldID   int    `json:"ID"`
	FieldName string `json:"Name"`
	List      int
	Type      FieldType
	Mask      string
	Size      int
	RegEx     string
	Prop      FieldProps
	Parent    int
	ListF1    int
	ListF2    int
}

// ID implements IDAndNamer
func (f *FieldDef) ID() ID { return ID(f.FieldID) }

// Name implements IDAndNamer
func (f *FieldDef) Name() Name { return Name(f.FieldName) }

//go:generate stringer -type FieldType -linecomment

// FieldType indicates the data type of the field.
type FieldType int

const (
	// InvalidFieldType indicates an invalid field type
	InvalidFieldType FieldType = 0 // Invalid

	// CharacterField indicates that the field stores character data
	CharacterField FieldType = 1 // Character

	// IntegerField indicates that the field stores integer data
	IntegerField FieldType = 2 // Integer

	// DateTimeField indicates that the field stores date or date/time
	// data
	DateTimeField FieldType = 3 // DateTime

	// DecimalField indicates that the field stores decimal data
	DecimalField FieldType = 4 // Decimal
)

//go:generate stringer -type FieldProps -linecomment

// FieldProps is a bit mask describing the properties of fields.
type FieldProps uint

const (
	// Required indicates that the field is required
	Required FieldProps = 2

	// SystemFieldDateEntered indicates that the field is a system field
	// storing the date that the document was stored.
	SystemFieldDateEntered FieldProps = 4 // System Field: Date Entered

	// SystemFieldIndexedBy indicates that the field stores the username
	// of the person who indexed the document
	SystemFieldIndexedBy FieldProps = 8 // System Field: Indexed By

	// SystemFieldPageCount stores the page count of the document
	SystemFieldPageCount FieldProps = 16 // System Field: Page Count

	// MultiValueField indicates that the field accepts more than one
	// value
	MultiValueField FieldProps = 32 // Multi-Value Field

	// FieldContainsDropdownList indicates that the field has a static
	// drop-down list
	FieldContainsDropdownList FieldProps = 64 // Field contains drop-down list

	// FieldContainsDynamicList indicates that the field has a dynamic
	// drop-down list.
	FieldContainsDynamicList FieldProps = 128 // Field contains dynamic list

	// SystemFieldLastModifiedBy indicates that the field holds the name
	// of the user that last modified the document.
	SystemFieldLastModifiedBy FieldProps = 256 // System Field: Last Modified By

	// TableField indicates that the field is a member of a table.
	TableField FieldProps = 512 // Table Field

	// SystemFieldFileType holds the file type of the document
	SystemFieldFileType FieldProps = 1024 // System Field: File Type

	// SystemFieldReadOnly indicates that the field is read-only.
	SystemFieldReadOnly FieldProps = 2048 // System Field: Read-only
)

// FieldVal is a field value within a Document.
type FieldVal struct {
	FieldID    int           `json:"ID"`
	Value      string        `json:"VAL"`
	MultiValue []interface{} `json:"MVAL"`
}

// Document as returned by search results
type Document struct {
	DocumentID      int `json:"Id"`
	Hash            string
	TID             int
	Fields          []FieldVal
	Version         int
	RootVersionID   int
	Username        string `json:"User_Name"`
	Hits            int
	Permissions     uint
	RevisionOptions int
	FileType        string
}

// DocumentOption is included in the call to Client.Document to specify the
// requested format of the document.
type DocumentOption string

const (
	// FileOption Returns the requested file
	FileOption DocumentOption = "File"

	// EmailOption Returns a ready-to-email version of the document
	EmailOption DocumentOption = "Email"

	// PrintOption Returns a ready-to-print version of the document
	PrintOption DocumentOption = "Print"

	// ThumbOption Returns a thumbnail of the document
	ThumbOption DocumentOption = "Thumb"

	// ZoneOption ---data missing---
	ZoneOption DocumentOption = "Zone"
)

// File is a temporary file created by posting an upload to the "WebPortalCache"
type File struct {
	Name      string      `json:"name"`
	IsEmail   bool        `json:"isEmail"`
	EmailData interface{} `json:"oEmailData"`
	Test      interface{} `json:"test"`
}

// ImportDocument is a completely different layout from the Update layout to
// import a document that has already been uploaded to the cache.
type ImportDocument struct {
	Fields []ImportField `json:"fields"`
	Files  []ImportFile  `json:"files"`
}

// ImportField is a completely different layout from FieldVal that's used when
// importing a new document.
type ImportField struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ImportFile is used in ImportDocument to specify one or more files uploaded
// to the WebPortalCache.
type ImportFile struct {
	Name string `json:"name"`
}
