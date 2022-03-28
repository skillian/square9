package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/skillian/argparse"
	"github.com/skillian/expr/errors"
	"github.com/skillian/logging"
	"github.com/skillian/square9/gscp"
)

var (
	logger = logging.GetLogger("square9")
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
	var config gscp.MainConfig
	parser.MustAddArgument(
		argparse.OptionStrings("--web-session-pool-limit"),
		argparse.ActionFunc(argparse.Store),
		argparse.Type(argparse.Int),
		argparse.Help(
			"specify a session limit for the web session "+
				"pool.  By default, only one session "+
				"is used at a time, but specifying "+
				"this option allows multiple requests "+
				"to execute concurrently",
		),
	).MustBind(&config.Config.WebSessionPoolLimit)
	var consoleLogLevel string
	parser.MustAddArgument(
		argparse.OptionStrings("--log-console"),
		argparse.ActionFunc(argparse.Store),
		argparse.Help(
			"enable logging for the console at the "+
				"given level",
		),
	).MustBind(&consoleLogLevel)
	parser.MustAddArgument(
		argparse.OptionStrings("--from-index"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"source specification is an index; not an individual "+
				"file",
		),
	).MustBind(&config.FromIndex)
	parser.MustAddArgument(
		argparse.OptionStrings("--to-index"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"destination specification is an index; not an "+
				"individual file",
		),
	).MustBind(&config.ToIndex)
	parser.MustAddArgument(
		argparse.OptionStrings("--index-only"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"can only be used with --to-index.  Outputs "+
				"the index without exporting the "+
				"actual documents.",
		),
	).MustBind(&config.Config.IndexOnly)
	parser.MustAddArgument(
		argparse.OptionStrings("--overwrite"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"allow existing destination files to be overwritten",
		),
	).MustBind(&config.Config.AllowOverwrite)
	parser.MustAddArgument(
		argparse.OptionStrings("--unsecure"),
		argparse.ActionFunc(argparse.StoreTrue),
		argparse.Help(
			"use HTTP instead of HTTPS.  This is almost "+
				"certainly a very bad idea.",
		),
	).MustBind(&config.Config.Unsecure)
	parser.MustAddArgument(
		argparse.Dest("source"),
		argparse.MetaVar("SOURCE"),
		argparse.Nargs(1),
		argparse.Help("source specification"),
	).MustBind(&config.Source)
	parser.MustAddArgument(
		argparse.Dest("destination"),
		argparse.MetaVar("DEST"),
		argparse.Nargs(1),
		argparse.Help("destination specification"),
	).MustBind(&config.Dest)
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
		signal.Notify(sigs, os.Interrupt)
		go func() {
			<-sigs
			signal.Stop(sigs)
			cancel()
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
		}()
	}
	if consoleLogLevel != "" {
		lvl, ok := logging.ParseLevel(consoleLogLevel)
		if !ok {
			handleErr(errors.Errorf(
				"invalid logging level: %q",
				consoleLogLevel,
			))
		}
		handler := &logging.ConsoleHandler{}
		handler.SetFormatter(logging.DefaultFormatter{})
		handler.SetLevel(lvl)
		logger.AddHandler(handler)
		if lvl < logger.Level() {
			logger.SetLevel(lvl)
		}
	}
	if config.Config.WebSessionPoolLimit != 0 {
		// TODO: Get rid of this.
		ctx = context.WithValue(ctx, (*gscp.WebSessionPoolLimit)(nil), config.Config.WebSessionPoolLimit)
	}
	if err := gscp.Main(ctx, config); err != nil {
		handleErr(err)
	}
}
