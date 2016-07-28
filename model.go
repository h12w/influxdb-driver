package influxdb

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/models"
)

// Result represents a resultset returned from a single statement.
type Result struct {
	Series   []models.Row
	Messages []*Message
	Err      string `json:"error,omitempty"`
}

// Point represents a single data point
type Point struct {
	models.Point
}

// NewPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended to
// send data with a timestamp.
func NewPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t ...time.Time,
) (*Point, error) {
	var T time.Time
	if len(t) > 0 {
		T = t[0]
	}

	pt, err := models.NewPoint(name, tags, fields, T)
	if err != nil {
		return nil, err
	}
	return &Point{
		Point: pt,
	}, nil
}

// BatchPointsConfig is the config data needed to create an instance of the BatchPoints struct
type WriteConfig struct {
	// Precision is the write precision of the points, defaults to "ns"
	Precision string

	// Database is the database to write points to
	Database string

	// RetentionPolicy is the retention policy of the points
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write
	WriteConsistency string
}

type BatchPoints struct {
	points []*Point
}

func (bp *BatchPoints) AddPoint(p *Point) {
	bp.points = append(bp.points, p)
}

func (bp *BatchPoints) AddPoints(ps []*Point) {
	bp.points = append(bp.points, ps...)
}

func (bp *BatchPoints) Points() []*Point {
	return bp.points
}

// Query defines a query to send to the server
type Query struct {
	Command   string
	Database  string
	Precision string
}

// NewQuery returns a query object
// database and precision strings can be empty strings if they are not needed
// for the query.
func NewQuery(command, database, precision string) Query {
	return Query{
		Command:   command,
		Database:  database,
		Precision: precision,
	}
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     string `json:"error,omitempty"`
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != "" {
		return fmt.Errorf(r.Err)
	}
	for _, result := range r.Results {
		if result.Err != "" {
			return fmt.Errorf(result.Err)
		}
	}
	return nil
}

// Message represents a user message.
type Message struct {
	Level string
	Text  string
}
