package influxdb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

func init() { sql.Register("influxdb", drv{}) }

type drv struct{}

func (drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func Open(name string) (driver.Conn, error) {
	uri, err := url.Parse(name)
	if err != nil {
		return nil, err
	}
	switch uri.Scheme {
	case "udp":
		panic("udp scheme is not supported by the driver")
	case "http":
		var (
			user     string
			password string
		)
		if uri.User != nil {
			user = uri.User.Username()
			password, _ = uri.User.Password()
		}
		query := uri.Query()
		timeout, _ := time.ParseDuration(query.Get("timeout"))
		addr := "http://" + uri.Host
		precision := uri.Query().Get("precision")
		if precision == "" {
			precision = "ns"
		}
		c, err := NewHTTPClient(HTTPConfig{
			Addr:      addr,
			Username:  user,
			Password:  password,
			UserAgent: query.Get("ua"),
			Timeout:   timeout,
		})
		if err != nil {
			return nil, err
		}
		database := strings.TrimPrefix(uri.Path, `/`)
		return &conn{
			c:         c,
			database:  database,
			precision: precision,
		}, nil
	}
	// TODO: support https, udp
	return nil, fmt.Errorf("unsupported scheme %s", uri.Scheme)
}

type conn struct {
	c         Client
	database  string
	precision string
	tx        *tx
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query)
}

func (c *conn) Close() error {
	return c.c.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.tx != nil {
		return nil, errors.New("a Tx already begins")
	}
	var err error
	c.tx, err = newTx(c)
	return c.tx, err
}

type insertStmt struct {
	conn  *conn
	db    string
	query string
}

func (s *insertStmt) Close() error { return nil }

func (s *insertStmt) NumInput() int { return -1 }

func (s *insertStmt) Exec(args []driver.Value) (driver.Result, error) {
	err := s.conn.c.Write([]byte(s.query), &WriteConfig{
		Database: s.db,
	})
	return &result{0, 0}, err
}

func (s *insertStmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("not supported yet")
}

type stmt struct {
	conn  *conn
	query string
}

var rxInsert = regexp.MustCompile(`^(?i:INSERT\s+INTO)\s+([a-zA-Z0-9_\-]+)\.(.*)`)

func newStmt(conn *conn, query string) (driver.Stmt, error) {
	m := rxInsert.FindStringSubmatch(query)
	if len(m) != 3 {
		return &stmt{
			conn:  conn,
			query: query,
		}, nil
	}
	db, insertQuery := m[1], m[2]
	return &insertStmt{
		conn:  conn,
		db:    db,
		query: insertQuery,
	}, nil
}

func (s *stmt) Close() error { return nil }

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	resp, err := s.conn.c.Query(NewQuery(s.query, s.conn.database, s.conn.precision))
	if err != nil {
		return nil, err
	}
	if resp.Error() != nil {
		return nil, resp.Error()
	}
	return &result{0, len(resp.Results)}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("not supported yet")
}

type result struct {
	lastInsertedID int
	rowsAffected   int
}

func (r *result) LastInsertId() (int64, error) {
	return int64(r.lastInsertedID), nil
}

func (r *result) RowsAffected() (int64, error) {
	return int64(r.rowsAffected), nil
}

type tx struct {
	conn *conn
	bp   *BatchPoints
}

func newTx(c *conn) (*tx, error) {
	return &tx{
		conn: c,
		bp:   &BatchPoints{},
	}, nil
}

func (t *tx) Commit() error {
	err := t.conn.c.Write(t.bp.Bytes("ns"), &WriteConfig{
		Database: t.conn.database,
	})
	t.conn.tx = nil
	return err
}

func (t *tx) Rollback() error {
	var err error
	t.bp = &BatchPoints{}
	t.conn.tx = nil
	return err
}
