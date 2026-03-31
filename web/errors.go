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
	return fmt.Errorf("%s is required and cannot be nil/zero", what)
}

func errRedefined(what string) error {
	return fmt.Errorf("redefinition of %s", what)
}
