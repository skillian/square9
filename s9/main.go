package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/skillian/argparse"
	"github.com/skillian/errors"
	"github.com/skillian/logging"
	"github.com/skillian/square9/web"
)

var (
	home = func() string {
		me, err := user.Current()
		if err != nil {
			panic(err)
		}
		return me.HomeDir
	}()

	defaultConfigFilename = filepath.Join(home, ".config", "s9.config")

	parser = argparse.MustNewArgumentParser(
		argparse.Description("Square 9 command line interface"),
	)

	argConfig = parser.MustAddArgument(
		argparse.OptionStrings("-c", "--config"),
		argparse.ActionFunc(argparse.Store),
		argparse.Default(defaultConfigFilename),
		argparse.Help("Specify a custom configuration file.  Command "+
			"line arguments override the configuration (default: "+
			"%q)", defaultConfigFilename),
	)

	argConfigKey = parser.MustAddArgument(
		argparse.OptionStrings("-k", "--config-key"),
		argparse.ActionFunc(argparse.Store),
		argparse.Default(""),
		argparse.Help("Specific configuration to load within the "+
			"configuration file."),
	)

	argDatabase = parser.MustAddArgument(
		argparse.OptionStrings("-d", "--database"),
		argparse.MetaVar("NAME_OR_ID"),
		argparse.ActionFunc(argparse.Store),
		argparse.Default(""),
		argparse.Help("The database name or ID to connect to"),
	)

	argArchive = parser.MustAddArgument(
		argparse.OptionStrings("-a", "--archive"),
		argparse.MetaVar("NAME_OR_ID"),
		argparse.ActionFunc(argparse.Store),
		argparse.Default(""),
		argparse.Help("The Archive name or ID.  If the archive is "+
			"nested, use a forward slash (\"/\") to separate the "+
			"archive names (e.g. Parent/Child)"),
	)

	argCommand = parser.MustAddArgument(
		argparse.OptionStrings("-x", "--exec"),
		argparse.MetaVar("COMMAND"),
		argparse.Help("Optional command to execute"),
		argparse.ActionFunc(argparse.Store),
		argparse.Choices(
			argparse.Choice{Key: "copy", Value: nil},
			argparse.Choice{Key: "repl", Value: cmdFunc(cmdRepl)},
		),
	)

	argLogLevel = parser.MustAddArgument(
		argparse.OptionStrings("--log-level"),
		argparse.ActionFunc(argparse.Store),
		argparse.Choices(
			argparse.Choice{Key: "verbose", Value: logging.VerboseLevel},
			argparse.Choice{Key: "debug", Value: logging.DebugLevel},
			argparse.Choice{Key: "info", Value: logging.InfoLevel},
			argparse.Choice{Key: "warn", Value: logging.WarnLevel},
			argparse.Choice{Key: "error", Value: logging.ErrorLevel},
		),
	)

	argItems = parser.MustAddArgument(
		argparse.Dest("items"),
		argparse.MetaVar("ITEM"),
		argparse.ActionFunc(argparse.Store),
		argparse.Nargs(argparse.OneOrMore),
		argparse.Help("Local files or remote documents to act on"),
	)

	args = parser.MustParseArgs()

	config      = args.MustGet(argConfig).(string)
	configKey   = args.MustGet(argConfigKey).(string)
	database    = args.MustGet(argDatabase).(string)
	archive     = args.MustGet(argArchive).(string)
	command     = args.MustGet(argCommand).(cmdFunc)
	logLevel, _ = func() interface{} {
		v, _ := args.Get(argLogLevel)
		return v
	}().(logging.Level)
	items, _ = args.GetStrings(argItems)
)

type cmdFunc func(context.Context, *Config, web.Client) error

func main() {
	logger.SetLevel(logLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 2)
	go func() {
		<-sigs
		if err := pprof.Lookup("goroutine").WriteTo(os.Stderr, 2); err != nil {
			logger.LogErr(err)
		}
		cancel()
		signal.Stop(sigs)
		for range sigs {
		}
		close(sigs)
	}()
	signal.Notify(sigs, os.Interrupt)
	if err := main2(ctx); err != nil {
		panic(err)
	}
}

func main2(ctx context.Context) (err error) {
	cfg, err := LoadConfigFile(config, configKey)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to load configuration")
	}
	if database != "" {
		cfg.Database = database
	}
	if archive != "" {
		cfg.Archive = archive
	}
	logger.Debug3(
		"getting client for URL: %q, database %q, archive %q",
		cfg.URL.URL, cfg.Database, cfg.Archive)
	var c web.Client = web.NewSessionPool(1, func() (*web.Session, error) {
		return web.NewSession(
			ctx,
			web.BasicAuth(cfg.Username, cfg.Password),
			web.APIURL(cfg.URL.URL))
	})
	defer errors.WrapDeferred(&err, c.Close)
	return command(ctx, cfg, c)
}

func cmdRepl(ctx context.Context, cfg *Config, c web.Client) error {
	var sc scanner.Scanner
	sc.Init(os.Stdin)
	var fields []string
	var db *web.Database
	var ar *web.Archive
	return c.Session(func(s *web.Session) (err error) {
		getDB := func(f web.IDAndNamerFilter) ([]web.Database, error) {
			ds, err := s.Databases(ctx, f)
			if err != nil {
				return nil, err
			}
			if len(ds) == 0 {
				return nil, errors.Errorf("Database %#v not found", f)
			}
			return ds, nil
		}
		getAr := func(f web.IDAndNamerFilter) ([]web.Archive, error) {
			ars, err := s.Archives(ctx, db, f)
			if err != nil {
				return nil, err
			}
			if len(ars) == 0 {
				return nil, errors.Errorf("Archive %#v not found", f)
			}
			return ars, nil
		}
		getSr := func(f web.IDAndNamerFilter) ([]web.Search, error) {
			srs, err := s.Searches(ctx, db, ar, f)
			if err != nil {
				return nil, err
			}
			if len(srs) == 0 {
				return nil, errors.Errorf("Search %#v not found", f)
			}
			return srs, nil
		}
		getNameOrID := func(s string) web.IDAndNamerFilter {
			id, err := strconv.Atoi(s)
			if err != nil {
				return web.Name(s)
			}
			return web.ID(id)
		}
		scanOnto := func(fs []string) ([]string, error) {
			for t := sc.Scan(); t != ';'; t = sc.Scan() {
				txt := sc.TokenText()
				if t == scanner.String {
					unq, err := strconv.Unquote(txt)
					if err != nil {
						return fs, errors.ErrorfWithCause(
							err, "invalid string: %q",
							txt)
					}
					txt = unq
				}
				fs = append(fs, txt)
			}
			return fs, nil
		}
		fmt.Println(
			"Starting the interactive Read-Evaluate-Print-Loop " +
				"(\"repl\").\n" +
				"**NOTE**:\n" +
				"\tAll commands must end with a semicolon!\n" +
				"\tHopefully this requirement will go away in " +
				"the future!\n" +
				"Type \"help;\" for a list of commands.")
	replLoop:
		for {
			fmt.Printf("s9> ")
			if fields, err = scanOnto(fields[:0]); err != nil {
				return err
			}
			if len(fields) == 0 {
				continue replLoop
			}
			switch {
			case strings.EqualFold(fields[0], "help"):
				fmt.Println(
					"commands:\n\n" +
						"list (databases OR archives OR searches)\n" +
						"\tList the databases, or archives or searches within a database.\n" +
						"\tBe sure to set the database before attempting to list the archives.\n\n" +
						"set (database OR archive) (name OR id)\n" +
						"\tSet the database or archive to use for other operations\n\n" +
						"import (files...)\n" +
						"\tImport one or more files to an archive.  You will be prompted for the fields.\n\n" +
						"quit\n" +
						"\tQuit the REPL.\n\n" +
						"run (search name OR id)\n" +
						"\tRun a search.  You will be prompted for values for the search prompts.\n\n" +
						"settings\n" +
						"\tShow the REPL's current settings like the current database or archive.")
				continue replLoop
			case strings.EqualFold(fields[0], "import"):
				if len(fields) < 2 {
					logger.Error("import expects 1 or more files to import")
					continue replLoop
				}
				files := make([]*os.File, 0, len(fields)-1)
				wts := make([]io.WriterTo, cap(files))
				cleanupFiles := func(err error) error {
					for _, f := range files {
						if err2 := f.Close(); err2 != nil {
							err = errors.ErrorfWithCauseAndContext(
								err2, err, "failed to close %v", f.Name())
						}
					}
					return err
				}
				for i, n := range fields[1:] {
					f, err := os.Open(n)
					if err != nil {
						logger.LogErr(cleanupFiles(errors.ErrorfWithCause(
							err, "failed to open %v for reading", n)))
						continue replLoop
					}
					files = append(files, f)
					wts[i] = web.ReaderBody{Reader: f}
				}
				fs, err := s.Fields(ctx, db, ar, nil)
				if err != nil {
					logger.LogErr(cleanupFiles(err))
					continue replLoop
				}
				indexFields := make([]web.ImportField, 0, len(fs))
				for _, f := range fs {
					fmt.Printf("%s: ", f.FieldName)
					if fields, err = scanOnto(fields[:0]); err != nil {
						return cleanupFiles(err)
					}
					if len(fields) > 0 {
						indexFields = append(indexFields, web.ImportField{
							Name:  strconv.Itoa(f.FieldID),
							Value: strings.Join(fields, " "),
						})
					}
				}
				if err = cleanupFiles(s.Import(ctx, db, ar, indexFields, wts...)); err != nil {
					logger.LogErr(err)
				}
				continue
			case strings.EqualFold(fields[0], "list"):
				if len(fields) != 2 {
					logger.Error("list what? \"databases\", \"archives\", \"fields\", \"searches\"?")
					continue
				}
				switch {
				case strings.EqualFold(fields[1], "databases"):
					dbs, err := getDB(nil)
					if err != nil {
						logger.LogErr(err)
						continue
					}
					for _, d := range dbs {
						fmt.Println(d)
					}
					continue
				case strings.EqualFold(fields[1], "archives"):
					ars, err := getAr(nil)
					if err != nil {
						logger.LogErr(err)
						continue
					}
					for _, a := range ars {
						fmt.Println(a)
					}
					continue
				case strings.EqualFold(fields[1], "searches"):
					srs, err := getSr(nil)
					if err != nil {
						logger.LogErr(err)
						continue
					}
					for _, s := range srs {
						fmt.Println(s)
					}
					continue
				default:
					logger.LogErr(errors.Errorf(
						"cannot list %q", fields[1]))
					continue
				}
			case strings.EqualFold(fields[0], "quit"):
				return nil
			case strings.EqualFold(fields[0], "run"):
				if len(fields) != 2 {
					logger.Error0("run what search?")
				}
				srs, err := getSr(getNameOrID(fields[1]))
				if err != nil {
					logger.LogErr(err)
					continue
				}
				fs := make([]web.SearchCriterion, 0, len(srs[0].Detail))
				for _, p := range srs[0].Detail {
					fmt.Printf(p.Prompt + " ")
					if fields, err = scanOnto(fields[:0]); err != nil {
						return err
					}
					if len(fields) == 0 {
						continue
					}
					fs = append(fs, web.SearchCriterion{
						Prompt: p.ID(),
						Value:  strings.Join(fields, " "),
					})
				}
				rs, err := s.Search(ctx, db, ar, &srs[0], fs, web.Value("ResultsPerPage", 10))
				if err != nil {
					logger.LogErr(err)
					continue
				}
				for _, d := range rs.Docs {
					fmt.Println(d)
				}
				continue
			case strings.EqualFold(fields[0], "set"):
				if len(fields) != 3 {
					logger.Error0(
						"syntax: set (database OR archive) (name OR ID)")
					continue
				}
				switch {
				case strings.EqualFold(fields[1], "database"):
					ds, err := getDB(getNameOrID(fields[2]))
					if err != nil {
						logger.LogErr(err)
						continue
					}
					db = new(web.Database)
					*db = ds[0]
					continue
				case strings.EqualFold(fields[1], "archive"):
					as, err := getAr(getNameOrID(fields[2]))
					if err != nil {
						logger.LogErr(err)
						continue
					}
					ar = new(web.Archive)
					*ar = as[0]
					continue
				default:
					logger.Error1("unknown setting: %q", fields[1])
					continue
				}
			case strings.EqualFold(fields[0], "settings"):
				fmt.Printf("db:\t%#v\nar:\t%#v\n", db, ar)
				continue
			default:
				logger.Error1("unknown command: %q", fields[0])
			}
		}
	})
}
