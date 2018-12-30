package pgconn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/pgpassfile"
	"github.com/pkg/errors"
)

// Config is the settings used to establish a connection to a PostgreSQL server.
type Config struct {
	Host          string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port          uint16
	Database      string
	User          string
	Password      string
	TLSConfig     *tls.Config       // nil disabled TLS
	DialFunc      DialFunc          // e.g. net.Dialer.DialContext
	RuntimeParams map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)
}

// Temporarily public to aid in migration of main pgx package.
//
// TODO - remove
func (c *Config) NetworkAddress() (network, address string) {
	return c.networkAddress()
}

func (c *Config) networkAddress() (network, address string) {
	// If host is a valid path, then address is unix socket
	if _, err := os.Stat(c.Host); err == nil {
		network = "unix"
		address = c.Host
		if !strings.Contains(address, "/.s.PGSQL.") {
			address = filepath.Join(address, ".s.PGSQL.") + strconv.FormatInt(int64(c.Port), 10)
		}
	} else {
		network = "tcp"
		address = fmt.Sprintf("%s:%d", c.Host, c.Port)
	}

	return network, address
}

// ParseConfig builds a []*Config with similar behavior to the PostgreSQL standard C library libpq.
// It uses the same defaults as libpq (e.g. port=5432) and understands most PG* environment
// variables. connString may be a URL or a DSN. It also may be empty to only read from the
// environment. If a password is not supplied it will attempt to read the .pgpass file.
//
// Example DSN: "user=jack password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-ca"
//
// Example URL: "postgres://jack:secret@1.2.3.4:5432/mydb?sslmode=verify-ca"
//
// Multiple configs may be returned due to sslmode settings with fallback options (e.g.
// sslmode=prefer). Future implementations may also support multiple hosts
// (https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-MULTIPLE-HOSTS).
//
// ParseConfig currently recognizes the following environment variable and their parameter key word
// equivalents passed via database URL or DSN:
//
// PGHOST
// PGPORT
// PGDATABASE
// PGUSER
// PGPASSWORD
// PGPASSFILE
// PGSSLMODE
// PGSSLCERT
// PGSSLKEY
// PGSSLROOTCERT
// PGAPPNAME
// PGCONNECT_TIMEOUT
//
// See http://www.postgresql.org/docs/11/static/libpq-envars.html for details on the meaning of
// environment variables.
//
// See https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-PARAMKEYWORDS for parameter key
// word names. They are usually but not always the environment variable name downcased and without
// the "PG" prefix.
//
// Important TLS Security Notes:
//
// ParseConfig tries to match libpq behavior with regard to PGSSLMODE. This includes defaulting to
// "prefer" behavior if not set.
//
// See http://www.postgresql.org/docs/11/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION for details on
// what level of security each sslmode provides.
//
// "verify-ca" mode currently is treated as "verify-full". e.g. It has stronger
// security guarantees than it would with libpq. Do not rely on this behavior as it
// may be possible to match libpq in the future. If you need full security use
// "verify-full".
func ParseConfig(connString string) ([]*Config, error) {
	settings := defaultSettings()
	addEnvSettings(settings)

	if connString != "" {
		// connString may be a database URL or a DSN
		if strings.HasPrefix(connString, "postgres://") {
			url, err := url.Parse(connString)
			if err != nil {
				return nil, err
			}

			err = addURLSettings(settings, url)
			if err != nil {
				return nil, err
			}
		} else {
			err := addDSNSettings(settings, connString)
			if err != nil {
				return nil, err
			}
		}
	}

	baseConfig := &Config{
		Host:          settings["host"],
		Database:      settings["database"],
		User:          settings["user"],
		Password:      settings["password"],
		RuntimeParams: make(map[string]string),
	}

	if port, err := parsePort(settings["port"]); err == nil {
		baseConfig.Port = port
	} else {
		return nil, fmt.Errorf("invalid port: %v", settings["port"])
	}

	if connectTimeout, present := settings["connect_timeout"]; present {
		dialFunc, err := makeConnectTimeoutDialFunc(connectTimeout)
		if err != nil {
			return nil, err
		}
		baseConfig.DialFunc = dialFunc
	} else {
		defaultDialer := makeDefaultDialer()
		baseConfig.DialFunc = defaultDialer.DialContext
	}

	notRuntimeParams := map[string]struct{}{
		"host":            struct{}{},
		"port":            struct{}{},
		"database":        struct{}{},
		"user":            struct{}{},
		"password":        struct{}{},
		"passfile":        struct{}{},
		"connect_timeout": struct{}{},
		"sslmode":         struct{}{},
		"sslkey":          struct{}{},
		"sslcert":         struct{}{},
		"sslrootcert":     struct{}{},
	}

	for k, v := range settings {
		if _, present := notRuntimeParams[k]; present {
			continue
		}
		baseConfig.RuntimeParams[k] = v
	}

	var tlsConfigs []*tls.Config

	// Ignore TLS settings if Unix domain socket like libpq
	if network, _ := baseConfig.networkAddress(); network == "unix" {
		tlsConfigs = append(tlsConfigs, nil)
	} else {
		var err error
		tlsConfigs, err = configTLS(settings)
		if err != nil {
			return nil, err
		}
	}

	configs := make([]*Config, 0, len(tlsConfigs))
	for _, tlsConfig := range tlsConfigs {
		clone := *baseConfig
		clone.TLSConfig = tlsConfig
		configs = append(configs, &clone)
	}

	passfile, err := pgpassfile.ReadPassfile(settings["passfile"])
	if err == nil {
		for _, config := range configs {
			if config.Password == "" {
				host := config.Host
				if network, _ := config.networkAddress(); network == "unix" {
					host = "localhost"
				}

				config.Password = passfile.FindPassword(host, strconv.Itoa(int(config.Port)), config.Database, config.User)
			}
		}
	}

	return configs, nil
}

func defaultSettings() map[string]string {
	settings := make(map[string]string)
	settings["port"] = "5432"

	// Default to the OS user name. Purposely ignoring err getting user name from
	// OS. The client application will simply have to specify the user in that
	// case (which they typically will be doing anyway).
	user, err := user.Current()
	if err == nil {
		settings["user"] = user.Username
		settings["passfile"] = filepath.Join(user.HomeDir, ".pgpass")
	}

	return settings
}

func addEnvSettings(settings map[string]string) {
	nameMap := map[string]string{
		"PGHOST":            "host",
		"PGPORT":            "port",
		"PGDATABASE":        "database",
		"PGUSER":            "user",
		"PGPASSWORD":        "password",
		"PGPASSFILE":        "passfile",
		"PGAPPNAME":         "application_name",
		"PGCONNECT_TIMEOUT": "connect_timeout",
		"PGSSLMODE":         "sslmode",
		"PGSSLKEY":          "sslkey",
		"PGSSLCERT":         "sslcert",
		"PGSSLROOTCERT":     "sslrootcert",
	}

	for envname, realname := range nameMap {
		value := os.Getenv(envname)
		if value != "" {
			settings[realname] = value
		}
	}
}

func addURLSettings(settings map[string]string, url *url.URL) error {
	if url.User != nil {
		settings["user"] = url.User.Username()
		if password, present := url.User.Password(); present {
			settings["password"] = password
		}
	}

	parts := strings.SplitN(url.Host, ":", 2)
	if parts[0] != "" {
		settings["host"] = parts[0]
	}
	if len(parts) == 2 {
		settings["port"] = parts[1]
	}

	database := strings.TrimLeft(url.Path, "/")
	if database != "" {
		settings["database"] = database
	}

	for k, v := range url.Query() {
		settings[k] = v[0]
	}

	return nil
}

var dsnRegexp = regexp.MustCompile(`([a-zA-Z_]+)=((?:"[^"]+")|(?:[^ ]+))`)

func addDSNSettings(settings map[string]string, s string) error {
	m := dsnRegexp.FindAllStringSubmatch(s, -1)

	for _, b := range m {
		settings[b[1]] = b[2]
	}

	return nil
}

type pgTLSArgs struct {
	sslMode     string
	sslRootCert string
	sslCert     string
	sslKey      string
}

// configTLS uses libpq's TLS parameters to construct  []*tls.Config. It is
// necessary to allow returning multiple TLS configs as sslmode "allow" and
// "prefer" allow fallback.
func configTLS(settings map[string]string) ([]*tls.Config, error) {
	host := settings["host"]
	sslmode := settings["sslmode"]
	sslrootcert := settings["sslrootcert"]
	sslcert := settings["sslcert"]
	sslkey := settings["sslkey"]

	// Match libpq default behavior
	if sslmode == "" {
		sslmode = "prefer"
	}

	tlsConfig := &tls.Config{}

	switch sslmode {
	case "disable":
		return []*tls.Config{nil}, nil
	case "allow", "prefer":
		tlsConfig.InsecureSkipVerify = true
	case "require":
		tlsConfig.InsecureSkipVerify = sslrootcert == ""
	case "verify-ca", "verify-full":
		tlsConfig.ServerName = host
	default:
		return nil, errors.New("sslmode is invalid")
	}

	if sslrootcert != "" {
		caCertPool := x509.NewCertPool()

		caPath := sslrootcert
		caCert, err := ioutil.ReadFile(caPath)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to read CA file %q", caPath)
		}

		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.Wrap(err, "unable to add CA to cert pool")
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.ClientCAs = caCertPool
	}

	if (sslcert != "" && sslkey == "") || (sslcert == "" && sslkey != "") {
		return nil, fmt.Errorf(`both "sslcert" and "sslkey" are required`)
	}

	if sslcert != "" && sslkey != "" {
		cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read cert")
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	switch sslmode {
	case "allow":
		return []*tls.Config{nil, tlsConfig}, nil
	case "prefer":
		return []*tls.Config{tlsConfig, nil}, nil
	case "require", "verify-ca", "verify-full":
		return []*tls.Config{tlsConfig}, nil
	default:
		panic("BUG: bad sslmode should already have been caught")
	}
}

func parsePort(s string) (uint16, error) {
	port, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > math.MaxUint16 {
		return 0, errors.New("outside range")
	}
	return uint16(port), nil
}

func makeDefaultDialer() *net.Dialer {
	return &net.Dialer{KeepAlive: 5 * time.Minute}
}

func makeConnectTimeoutDialFunc(s string) (DialFunc, error) {
	timeout, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, errors.New("negative timeout")
	}

	d := makeDefaultDialer()
	d.Timeout = time.Duration(timeout) * time.Second
	return d.DialContext, nil
}