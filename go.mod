module github.com/skillian/square9

go 1.18

replace github.com/skillian/argparse => ../argparse
replace github.com/skillian/expr => ../expr
replace github.com/skillian/interactivity => ../interactivity
replace github.com/skillian/logging => ../logging
replace github.com/skillian/workers => ../workers

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/google/uuid v1.3.0
	github.com/skillian/argparse v0.0.0-20220107121831-4b430f804d62
	github.com/skillian/errors v0.0.0-20190910214200-f19f31b303bd
	github.com/skillian/expr v0.0.0-20220215084646-3e36fbdc1ffd
	github.com/skillian/interactivity v0.0.0-20210425124543-4b3b9b919a80
	github.com/skillian/logging v0.0.0-20210425124543-4b3b9b919a80
	github.com/skillian/workers v0.0.0-20220324165209-b3b5e8db799e
)

require (
	github.com/skillian/textwrap v0.0.0-20190707153458-15c7ee8d44ed // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
)
