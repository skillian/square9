package web_test

import (
	"context"
	"testing"

	"github.com/skillian/logging"
	"github.com/skillian/square9/gscp"
)

type mainTest struct {
	fromIndex      bool
	source         string
	toIndex        bool
	dest           string
	allowOverwrite bool
	unsecure       bool
}

var mainTests = []mainTest{
	// {
	// 	//source:   `C:\Users\sean\Downloads\Missing Invoice Process and Report Guide - Paperless Solutions Inc.pdf`,
	// 	source:   `C:\Users\Sean\Downloads\1 (14).pdf`,
	// 	dest:     "ssAdministrator:globalsearch@192.168.0.242:BrewHaven/Accounts Payable/Purchase Orders?Vendor Name=TEST&PO Number=123456",
	// 	unsecure: true,
	// },
	{
		//source:   `C:\Users\sean\Downloads\Missing Invoice Process and Report Guide - Paperless Solutions Inc.pdf`,
		fromIndex: true,
		source:    `C:\Users\Sean\Downloads\testimport\export_Purchase Orders.csv`,
		dest:      "ssAdministrator:globalsearch@192.168.0.242:BrewHaven/Accounts Payable/Purchase Orders",
		unsecure:  true,
	},
}

func TestMain(t *testing.T) {
	defer logging.TestingHandler(
		logging.GetLogger("square9"), t,
		logging.HandlerLevel(logging.VerboseLevel),
		logging.HandlerFormatter(logging.GoFormatter{}),
	)()
	for _, tc := range mainTests {
		t.Run(tc.source+"_to_"+tc.dest, func(t *testing.T) {
			if err := gscp.Main(
				context.Background(),
				tc.fromIndex,
				tc.source,
				tc.toIndex,
				tc.dest,
				tc.allowOverwrite,
				tc.unsecure,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}
