package influxdb

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

// HTTPConfig is the config data needed to create an HTTP Client
type HTTPConfig struct {
	// Addr should be of the form "http://host:port"
	// or "http://[ipv6-host%zone]:port".
	Addr string

	// Username is the influxdb username, optional
	Username string

	// Password is the influxdb password, optional
	Password string

	// UserAgent is the http User Agent, defaults to "InfluxDBClient"
	UserAgent string

	// Timeout for influxdb writes, defaults to no timeout
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config
}

// Client is a client interface for writing & querying the database
type Client interface {
	// Ping checks that status of cluster
	Ping(timeout time.Duration) (time.Duration, string, error)

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(lineData []byte, cfg *WriteConfig) error

	// Query makes an InfluxDB Query on the database. This will fail if using
	// the UDP client.
	Query(q Query) (*Response, error)

	// Close releases any resources a Client may be using.
	Close() error
}

// NewHTTPClient returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClient(conf HTTPConfig) (Client, error) {
	if conf.UserAgent == "" {
		conf.UserAgent = "InfluxDBClient"
	}

	u, err := url.Parse(conf.Addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		m := fmt.Sprintf("Unsupported protocol scheme: %s, your address"+
			" must start with http:// or https://", u.Scheme)
		return nil, errors.New(m)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
	}
	if conf.TLSConfig != nil {
		tr.TLSClientConfig = conf.TLSConfig
	}
	return &httpClient{
		url:       *u,
		username:  conf.Username,
		password:  conf.Password,
		useragent: conf.UserAgent,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
		transport: tr,
	}, nil
}

// Ping will check to see if the server is up with an optional timeout on waiting for leader.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *httpClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	now := time.Now()
	u := c.url
	u.Path = "ping"

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("User-Agent", c.useragent)

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	if timeout > 0 {
		params := req.URL.Query()
		params.Set("wait_for_leader", fmt.Sprintf("%.0fs", timeout.Seconds()))
		req.URL.RawQuery = params.Encode()
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}

	if resp.StatusCode != http.StatusNoContent {
		var err = fmt.Errorf(string(body))
		return 0, "", err
	}

	version := resp.Header.Get("X-Influxdb-Version")
	return time.Since(now), version, nil
}

// Close releases the client's resources.
func (c *httpClient) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

// httpClient is safe for concurrent use as the fields are all read-only
// once the client is instantiated.
type httpClient struct {
	// N.B - if url.UserInfo is accessed in future modifications to the
	// methods on client, you will need to syncronise access to url.
	url        url.URL
	username   string
	password   string
	useragent  string
	httpClient *http.Client
	transport  *http.Transport
}

func (bp *BatchPoints) Bytes(precision string) []byte {
	var b bytes.Buffer
	for _, p := range bp.Points() {
		_, _ = b.WriteString(p.PrecisionString(precision))
		_ = b.WriteByte('\n')
	}
	return b.Bytes()
}

func (c *httpClient) Write(lineData []byte, cfg *WriteConfig) error {
	u := c.url
	u.Path = "write"
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(lineData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", cfg.Database)
	if cfg.RetentionPolicy != "" {
		params.Set("rp", cfg.RetentionPolicy)
	}
	if cfg.Precision != "" {
		params.Set("precision", cfg.Precision)
	}
	if cfg.WriteConsistency != "" {
		params.Set("consistency", cfg.WriteConsistency)
	}
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = fmt.Errorf(string(body))
		return err
	}

	return nil
}

// Query sends a command to the server and returns the Response
func (c *httpClient) Query(q Query) (*Response, error) {
	u := c.url
	u.Path = "query"

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("q", q.Command)
	params.Set("db", q.Database)
	if q.Precision != "" {
		params.Set("epoch", q.Precision)
	}
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var response Response
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	decErr := dec.Decode(&response)

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}
	// If we got a valid decode error, send that back
	if decErr != nil {
		return nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
	}
	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server",
			resp.StatusCode)
	}
	return &response, nil
}
