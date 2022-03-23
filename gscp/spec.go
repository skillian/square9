package gscp

import (
	"runtime"
	"strings"

	"github.com/skillian/expr/errors"
)

type Spec struct {
	Local       string
	Username    string
	Password    string
	Hostname    string
	APIPath     string
	Database    string
	ArchivePath []string
	Search      string
	Fields      []FieldSpec
}

// Eq checks if two Specs are equal.
func (sp *Spec) Eq(b *Spec) bool {
	if ([...]string{
		sp.Local, sp.Username, sp.Password, sp.Hostname, sp.APIPath,
		sp.Database, sp.Search,
	}) != ([...]string{
		b.Local, b.Username, b.Password, b.Hostname, b.APIPath,
		b.Database, b.Search,
	}) {
		return false
	}
	if len(sp.ArchivePath) != len(b.ArchivePath) {
		return false
	}
	if len(sp.Fields) != len(b.Fields) {
		return false
	}
	for i, ap := range sp.ArchivePath {
		if ap != b.ArchivePath[i] {
			return false
		}
	}
	for i, f := range sp.Fields {
		if f != b.Fields[i] {
			return false
		}
	}
	return true
}

var (
	errNoDatabase = errors.New("no database found")
)

func ParseSpec(specString string) (*Spec, error) {
	s := specString
	if i := strings.IndexByte(s, ':'); i == -1 || ((i == 1 || strings.HasPrefix(s, "\\")) && runtime.GOOS == "windows") {
		return &Spec{Local: s}, nil
	}
	sp := &Spec{
		APIPath: "square9api",
	}
	s = strings.TrimPrefix(s, "gscp://")
	i := strings.IndexByte(s, '@')
	if i != -1 {
		sp.Username = s[:i]
		s = s[i+1:]
		if i = strings.IndexByte(sp.Username, ':'); i != -1 {
			sp.Username, sp.Password = sp.Username[:i], sp.Username[i+1:]
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
	if len(s) > 0 {
		sp.ArchivePath = strings.Split(s, "/")
	}
	return sp, nil
}

type FieldSpec struct {
	Field string
	Value string
}
