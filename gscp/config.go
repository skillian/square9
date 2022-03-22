package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/skillian/errors"
)

// Config defines the model of the configuration file that's loaded from JSON.
type Config struct {
	URL      URL    `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Archive  string `json:"archive"`
}

// LoadConfigFile loads a configuration from a file by its filename.
func LoadConfigFile(filename, key string) (c *Config, err error) {
	var r io.ReadCloser
	r, err = os.Open(filename)
	if err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to open configuration file %q for reading",
			filename)
	}
	defer errors.WrapDeferred(&err, r.Close)
	return LoadConfig(r, key)
}

// LoadConfig loads a mapping of keys to configurations as JSON from an
// io.Reader and selects the configuration associated with the given key.
func LoadConfig(r io.Reader, key string) (*Config, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to read %v", r)
	}
	if key != "" {
		return loadMultiConfig(data, key)
	}
	return loadSingleConfig(data)
}

func loadMultiConfig(data []byte, key string) (cfg *Config, err error) {
	var m map[string]json.RawMessage
	if err = json.Unmarshal(data, &m); err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to unmarshal configuration",
		)
	}
	cfgVal, ok := m[key]
	if !ok {
		return nil, errors.Errorf(
			"no configuration with key: %q", key,
		)
	}
	return loadSingleConfig(([]byte)(cfgVal))
}

func loadSingleConfig(data []byte) (cfg *Config, err error) {
	if err = json.Unmarshal(data, &cfg); err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to unmarshal configuration",
		)
	}
	return
}

// URL wraps a *url.URL just so we can unmarshal JSON into it.
type URL struct {
	*url.URL
}

// UnmarshalText implements encoding.TextUnmarshaler
func (u *URL) UnmarshalText(bs []byte) error {
	s := string(bs)
	logger.Debug1("quoted unmarshaled URL text: %q", s)
	ur, err := url.Parse(s)
	if err != nil {
		return err
	}
	u.URL = ur
	return nil
}
