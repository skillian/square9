package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime/pprof"

	"github.com/skillian/argparse"
	"github.com/skillian/expr/errors"
	"github.com/skillian/logging"
	"github.com/skillian/square9/gscp"
)

var (
	homePath = func() string {
		me, err := user.Current()
		if err != nil {
			panic(err)
		}
		return me.HomeDir
	}()

	defaultConfigFilename = filepath.Join(
		homePath,
		".config",
		"s9.json",
	)

	logger = logging.GetLogger("gscp")
)

func main() {
	parser := argparse.MustNewArgumentParser(
		argparse.Description(
			"Interact with Square 9's APIs",
		),
		argparse.Epilog(
			`A specification can be one of the following:
	1. A filesystem path (on the local computer or a network location)
	2. A GlobalSearch API URL to a document
	3. A gscp pseudo-URI.

A gscp pseudo-URI is meant to resemble a remote host specification like in scp
and has the following format:

  [gscp://][username@]hostname[/api-path]:database[/archive[/sub-archive ...]]

where:
	username:	The GlobalSearch user name.  The password must be
			specified within the configuration file or
			given interactively.
	hostname:	The hostname of the GlobalSearch API server.
	api-path:	The path on the hostname to the GlobalSearch API.  This
			only needs to be specified if it is not /square9api.
	database:	The name of the GlobalSearch database.
	archive:	The name of the GlobalSearch archive.
	sub-archive:	Optional sub archive(s)

A gscp pseudo-URI can have query parameters starting with a question mark ("?")
and separated by ampersands ("&").  When a gscp pseudo-URI is a source, then
the parameters represent prompt or field values to a search.  When a gscp
pseudo-URI is a target, then the parameters are field values.
`,
		),
	)
	var fromIndex bool
	parser.MustAddArgument(
		argparse.OptionStrings("--from-index"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"source specification is an index; not an individual "+
				"file",
		),
	).MustBind(&fromIndex)
	var toIndex bool
	parser.MustAddArgument(
		argparse.OptionStrings("--to-index"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"destination specification is an index; not an "+
				"individual file",
		),
	).MustBind(&toIndex)
	var allowOverwrite bool
	parser.MustAddArgument(
		argparse.OptionStrings("--overwrite"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"allow existing destination files to be overwritten",
		),
	).MustBind(&allowOverwrite)
	var sourceSpec string
	parser.MustAddArgument(
		argparse.Dest("source"),
		argparse.MetaVar("SOURCE"),
		argparse.Help("source specification"),
	).MustBind(&sourceSpec)
	var destSpec string
	parser.MustAddArgument(
		argparse.Dest("destination"),
		argparse.MetaVar("DEST"),
		argparse.Help("destination specification"),
	).MustBind(&destSpec)
	parser.MustParseArgs()
	handleErr := func(err error) {
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(-1)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	{
		sigs := make(chan os.Signal)
		signal.Notify(sigs, os.Kill)
		go func() {
			<-sigs
			signal.Stop(sigs)
			w := os.Stderr
			if err := pprof.Lookup("goroutine").WriteTo(w, 2); err != nil {
				logger.LogErr(
					errors.Errorf1From(
						err, "failed to write "+
							"goroutine profiles "+
							"to %v",
						w,
					),
				)
			}
			os.Exit(1)
		}()
	}
	if err := Main(ctx, specInfo{
		str:   sourceSpec,
		index: fromIndex,
	}, specInfo{
		str:   destSpec,
		index: toIndex,
	}, allowOverwrite); err != nil {
		handleErr(err)
	}
}

func Main(ctx context.Context, sourceInfo, destInfo specInfo, allowOverwrite bool) error {
	source, err := parseSpecInfo(sourceInfo)
	if err != nil {
		return err
	}
	dest, err := parseSpecInfo(destInfo)
	if err != nil {
		return err
	}
	return gscp.CopyFromSourceToDestSpec(ctx, source, dest, allowOverwrite)
}

type specInfo struct {
	str   string
	index bool
}

func parseSpecInfo(si specInfo) (*gscp.Spec, error) {
	sp, err := gscp.ParseSpec(si.str)
	if err != nil {
		return nil, err
	}
	if si.index {
		sp.Kind |= gscp.IndexSpec
	}
	return sp, nil
}
