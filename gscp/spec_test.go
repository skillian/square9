package gscp_test

import (
	"strings"
	"testing"

	"github.com/skillian/square9/gscp"
)

type parseSpecTest struct {
	spec   string
	expect gscp.Spec
	err    string
}

var parseSpecTests = []parseSpecTest{
	{
		spec: "hostname:database",
		expect: gscp.Spec{
			Hostname: "hostname",
			APIPath:  "square9api",
			Database: "database",
		},
	},
	{
		spec: "gscp://hostname:database",
		expect: gscp.Spec{
			Hostname: "hostname",
			APIPath:  "square9api",
			Database: "database",
		},
	},
	{
		spec: "username@hostname:database",
		expect: gscp.Spec{
			Username: "username",
			Hostname: "hostname",
			APIPath:  "square9api",
			Database: "database",
		},
	},
	{
		spec: "username:password@hostname:database",
		expect: gscp.Spec{
			Username: "username",
			Password: "password",
			Hostname: "hostname",
			APIPath:  "square9api",
			Database: "database",
		},
	},
	{
		spec: "username@hostname/s9api:database",
		expect: gscp.Spec{
			Username: "username",
			Hostname: "hostname",
			APIPath:  "s9api",
			Database: "database",
		},
	},
	{
		spec: "username@hostname:database/archive",
		expect: gscp.Spec{
			Username:    "username",
			Hostname:    "hostname",
			APIPath:     "square9api",
			Database:    "database",
			ArchivePath: []string{"archive"},
		},
	},
	{
		spec: "username:password@hostname:database/archive/subarchive",
		expect: gscp.Spec{
			Username:    "username",
			Password:    "password",
			Hostname:    "hostname",
			APIPath:     "square9api",
			Database:    "database",
			ArchivePath: []string{"archive", "subarchive"},
		},
	},
	{
		spec: "username:password@hostname/sq9:database/archive/subarchive",
		expect: gscp.Spec{
			Username:    "username",
			Password:    "password",
			Hostname:    "hostname",
			APIPath:     "sq9",
			Database:    "database",
			ArchivePath: []string{"archive", "subarchive"},
		},
	},
	{
		spec: "C:\\Users\\Sean\\Downloads",
		expect: gscp.Spec{
			Local: "C:\\Users\\Sean\\Downloads",
		},
	},
	{
		spec: "\\\\server\\share",
		expect: gscp.Spec{
			Local: "\\\\server\\share",
		},
	},
	// negative cases:
	{
		spec: "gscp://hostname/archive",
		err:  "cannot find database in spec",
	},
}

func TestParseSpec(t *testing.T) {
	for _, tc := range parseSpecTests {
		t.Run(tc.spec, func(t *testing.T) {
			sp, err := gscp.ParseSpec(tc.spec)
			if err != nil {
				if tc.err == "" {
					t.Fatal(err)
				}
				if strings.Contains(err.Error(), tc.err) {
					return
				}
			}
			if !sp.Eq(&tc.expect) {
				t.Fatalf(
					"expected:\n\t%#v\n\t%#v",
					tc.expect, *sp,
				)
			}
		})
	}
}
