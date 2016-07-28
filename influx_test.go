package influxdb_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "h12.me/influxdb-driver"
	"h12.me/realtest/influx"
)

func TestOpen(t *testing.T) {
	in, err := influx.New()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	conn, err := sql.Open("influxdb", "http://"+in.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn == nil {
		t.Fatal("expect conn but got nil")
	}
	if _, err := conn.Exec("SHOW DATABASES"); err != nil {
		t.Fatal(err)
	}
}

func TestWrite(t *testing.T) {
	in, err := influx.New()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	addr := "http://" + in.Addr()
	fmt.Println(addr)
	conn, err := sql.Open("influxdb", addr)
	if err != nil {
		t.Fatal(err)
	}
	db, err := in.CreateRandomDatabase()
	if err != nil {
		t.Fatal(err)
	}
	defer in.DeleteDatabase(db)
	_, err = conn.Exec("INSERT INTO " + db + `.test,foo="bar",bat="baz" value=12,otherval=21 1422568543702900257`)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Println(in.Dump("SELECT * FROM test", db))
}
