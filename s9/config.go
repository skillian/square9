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
	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to read %v", r)
	}
	var m map[string]*Config
	if err = json.Unmarshal(bs, &m); err != nil {
		return nil, errors.ErrorfWithCause(
			err, "failed to unmarshal configuration")
	}
	if len(m) == 0 {
		return nil, errors.Errorf("no configurations in %v", r)
	}
	if key == "" {
		for key = range m {
			break
		}
		if len(m) > 1 {
			logger.Warn1("no specific configuration specified.  "+
				"Defaulting to %q", key)
		}
	}
	return m[key], nil
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
