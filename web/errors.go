package web

import (
	"github.com/skillian/errors"
	"github.com/skillian/logging"
)

var (
	logger = logging.GetLogger(
		"square9", logging.LoggerLevel(logging.VerboseLevel))
)

func errRequired(what string) error {
	return errors.Errorf(what + " is required and cannot be nil/zero")
}

func errRedefined(what string) error {
	return errors.Errorf("redefinition of " + what)
}
