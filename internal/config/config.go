// Package config loads the YAML configuration file.
//
// Secrets do not belong in the file: any string that holds one may reference
// an environment variable as ${NAME}, and a reference to an unset variable is
// an error rather than an empty value.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Database   Database        `yaml:"database"`
	Log        Log             `yaml:"log"`
	HTTP       HTTP            `yaml:"http"`
	Connectors []ConnectorSpec `yaml:"connectors"`
}

type Database struct {
	// URL is a PostgreSQL connection string.
	URL EnvString `yaml:"url"`
}

type Log struct {
	// Level is debug, info, warn or error.
	Level string `yaml:"level"`
	// Format is json or text.
	Format string `yaml:"format"`
}

// HTTP holds the settings every connector's HTTP client shares.
type HTTP struct {
	// Timeout bounds a single request attempt.
	Timeout Duration `yaml:"timeout"`
	Retry   Retry    `yaml:"retry"`
}

type Retry struct {
	MaxRetries int      `yaml:"max_retries"`
	BaseDelay  Duration `yaml:"base_delay"`
	MaxDelay   Duration `yaml:"max_delay"`
	// MaxWait is the longest rate-limit wait the run will sit through.
	MaxWait Duration `yaml:"max_wait"`
}

// Auth selects how a connector authenticates.
type Auth struct {
	// Type is bearer, basic or none. Empty means bearer when a token is set
	// and none otherwise.
	Type     string    `yaml:"type"`
	Token    EnvString `yaml:"token"`
	Username EnvString `yaml:"username"`
	Password EnvString `yaml:"password"`
}

// ConnectorSpec is one entry of the connectors list. Type and Auth are common
// to every connector; the remaining keys belong to the connector type and are
// read with DecodeSettings, so this package does not need to know them.
type ConnectorSpec struct {
	Type string
	Auth Auth

	node yaml.Node
}

func (c *ConnectorSpec) UnmarshalYAML(n *yaml.Node) error {
	var common struct {
		Type string `yaml:"type"`
		Auth Auth   `yaml:"auth"`
	}
	if err := n.Decode(&common); err != nil {
		return err
	}
	c.Type, c.Auth, c.node = common.Type, common.Auth, *n
	return nil
}

// DecodeSettings decodes the connector's entry into v, a struct describing
// the keys of one connector type.
func (c ConnectorSpec) DecodeSettings(v any) error {
	return c.node.Decode(v)
}

// Defaults returns the configuration used for anything the file leaves out.
func Defaults() Config {
	return Config{
		Log: Log{Level: "info", Format: "json"},
		HTTP: HTTP{
			Timeout: Duration(30 * time.Second),
			Retry: Retry{
				MaxRetries: 5,
				BaseDelay:  Duration(500 * time.Millisecond),
				MaxDelay:   Duration(30 * time.Second),
				MaxWait:    Duration(time.Hour),
			},
		},
	}
}

// Load reads and validates the file at path.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	cfg, err := Parse(raw)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes and validates a configuration document.
func Parse(raw []byte) (Config, error) {
	cfg := Defaults()
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true) // a misspelled key is an error, not a silent default
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c Config) validate() error {
	var errs []error
	if c.Database.URL == "" {
		errs = append(errs, errors.New("database.url is required"))
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log.level %q is not one of debug, info, warn, error", c.Log.Level))
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		errs = append(errs, fmt.Errorf("log.format %q is not one of json, text", c.Log.Format))
	}
	if c.HTTP.Timeout <= 0 {
		errs = append(errs, errors.New("http.timeout must be positive"))
	}
	if c.HTTP.Retry.MaxRetries < 0 {
		errs = append(errs, errors.New("http.retry.max_retries must not be negative"))
	}
	if c.HTTP.Retry.BaseDelay <= 0 || c.HTTP.Retry.MaxDelay < c.HTTP.Retry.BaseDelay {
		errs = append(errs, errors.New("http.retry needs 0 < base_delay <= max_delay"))
	}
	if len(c.Connectors) == 0 {
		errs = append(errs, errors.New("at least one connector is required"))
	}
	for i, conn := range c.Connectors {
		if conn.Type == "" {
			errs = append(errs, fmt.Errorf("connectors[%d].type is required", i))
		}
	}
	return errors.Join(errs...)
}

// EnvString is a string in which ${NAME} is replaced by the environment
// variable NAME when the file is loaded.
type EnvString string

// Only the braced form is a reference, so a "$" that happens to be part of a
// password is left alone.
var envRef = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*\}`)

func (e *EnvString) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	var missing []string
	expanded := envRef.ReplaceAllStringFunc(s, func(ref string) string {
		name := ref[2 : len(ref)-1]
		v := os.Getenv(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	})
	if len(missing) > 0 {
		return fmt.Errorf("line %d: environment variable %s is not set", n.Line, strings.Join(missing, ", "))
	}
	*e = EnvString(expanded)
	return nil
}

// Duration is a time.Duration written as a string such as "30s" or "1h".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: %q is not a duration such as 30s or 5m", n.Line, s)
	}
	*d = Duration(parsed)
	return nil
}
