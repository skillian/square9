package main

import "github.com/skillian/logging"

var (
	logger = logging.GetLogger(
		"square9",
		logging.LoggerHandler(
			new(logging.ConsoleHandler),
			logging.HandlerLevel(logging.VerboseLevel),
			logging.HandlerFormatter(logging.DefaultFormatter{})))
)
