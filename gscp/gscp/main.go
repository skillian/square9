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

A gscp pseudo-URI can have query parameters 
`,
		),
	)
	var sourceSpec string
	parser.MustAddArgument(
		argparse.Dest("source"),
		argparse.Help("source specification"),
	).MustBind(&sourceSpec)
	var destSpec string
	parser.MustAddArgument(
		argparse.Dest("destination"),
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
	if err := Main(ctx, sourceSpec, destSpec); err != nil {
		handleErr(err)
	}
}

func Main(ctx context.Context, sourceSpec, destSpec string) error {
	source, err := gscp.ParseSpec(sourceSpec)
	if err != nil {
		return errors.Errorf1From(
			err, "failed to parse %q as a specification",
			sourceSpec,
		)
	}
	dest, err := gscp.ParseSpec(destSpec)
	if err != nil {
		return errors.Errorf1From(
			err, "failed to parse %q as a specification",
			destSpec,
		)
	}
}
