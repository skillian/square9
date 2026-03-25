package web

import (
	"fmt"

	"github.com/skillian/logging"
)

var (
	logger = logging.GetLogger(
		"square9", logging.LoggerLevel(logging.VerboseLevel))
)

func errRequired(what string) error {
	return fmt.Errorf(what + " is required and cannot be nil/zero")
}

func errRedefined(what string) error {
	return fmt.Errorf("redefinition of " + what)
}
