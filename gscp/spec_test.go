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
			APIPath:  "square9api/api",
			Database: "database",
		},
	},
	{
		spec: "gscp://hostname:database",
		expect: gscp.Spec{
			Hostname: "hostname",
			APIPath:  "square9api/api",
			Database: "database",
		},
	},
	{
		spec: "username@hostname:database",
		expect: gscp.Spec{
			Username: "username",
			Hostname: "hostname",
			APIPath:  "square9api/api",
			Database: "database",
		},
	},
	{
		spec: "username:password@hostname:database",
		expect: gscp.Spec{
			Username: "username",
			Password: "password",
			Hostname: "hostname",
			APIPath:  "square9api/api",
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
			APIPath:     "square9api/api",
			Database:    "database",
			ArchivePath: "archive",
		},
	},
	{
		spec: "username:password@hostname:database/archive/subarchive",
		expect: gscp.Spec{
			Username:    "username",
			Password:    "password",
			Hostname:    "hostname",
			APIPath:     "square9api/api",
			Database:    "database",
			ArchivePath: "archive/subarchive",
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
			ArchivePath: "archive/subarchive",
		},
	},
	{
		spec: "username:password@hostname/sq9:database/archive/subarchive?hello=world&myName=sean",
		expect: gscp.Spec{
			Username:    "username",
			Password:    "password",
			Hostname:    "hostname",
			APIPath:     "sq9",
			Database:    "database",
			ArchivePath: "archive/subarchive",
			Fields: map[string]string{
				"hello":  "world",
				"myName": "sean",
			},
		},
	},
	{
		spec: "username:password@hostname/square9api/api:database/archive/subarchive?hello=world&myName=sean",
		expect: gscp.Spec{
			Username:    "username",
			Password:    "password",
			Hostname:    "hostname",
			APIPath:     "square9api/api",
			Database:    "database",
			ArchivePath: "archive/subarchive",
			Fields: map[string]string{
				"hello":  "world",
				"myName": "sean",
			},
		},
	},
	{
		spec: "C:\\Users\\Sean\\Downloads",
		expect: gscp.Spec{
			ArchivePath: "C:\\Users\\Sean\\Downloads",
		},
	},
	{
		spec: "\\\\server\\share",
		expect: gscp.Spec{
			Kind:        gscp.LocalSpec,
			ArchivePath: "\\\\server\\share",
		},
	},
	{
		spec: "\\\\server\\share?hello=world",
		expect: gscp.Spec{
			Kind:        gscp.LocalSpec,
			ArchivePath: "\\\\server\\share",
			Fields: map[string]string{
				"hello": "world",
			},
		},
	},
	{
		spec: "gscp://ssAdministrator:globalsearch@192.168.0.242:BrewHaven/Accounts Payable/Purchase Orders/Browse Purchase Orders?PO+Number=08312008",
		expect: gscp.Spec{
			Username:    "ssAdministrator",
			Password:    "globalsearch",
			Hostname:    "192.168.0.242",
			APIPath:     "square9api/api",
			Database:    "BrewHaven",
			ArchivePath: "Accounts Payable/Purchase Orders/Browse Purchase Orders",
			Fields: map[string]string{
				"PO Number": "08312008",
			},
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
			checkErr := func(err error) {
				if err != nil {
					if tc.err == "" {
						t.Fatal(err)
					}
					if strings.Contains(err.Error(), tc.err) {
						t.SkipNow()
					}
				}
			}
			sp, err := gscp.ParseSpec(tc.spec)
			checkErr(err)
			if !sp.Eq(&tc.expect) {
				t.Fatalf(
					"expected:\n\t%#v\n\t%#v",
					tc.expect, *sp,
				)
			}
		})
	}
}
