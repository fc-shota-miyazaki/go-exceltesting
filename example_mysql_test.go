package exceltesting_test

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fc-shota-miyazaki/go-exceltesting"
	_ "github.com/go-sql-driver/mysql"
)

var mysqlConn *sql.DB

func openMySQLTestingConn(t *testing.T) *sql.DB {
	t.Helper()

	if mysqlConn != nil {
		return mysqlConn
	}

	uri := os.Getenv("EXCELTESTING_MYSQL_CONNECTION")
	if uri == "" {
		t.Skip("EXCELTESTING_MYSQL_CONNECTION が未設定のためMySQLテストをスキップします")
	}

	dsn := uri
	if strings.HasPrefix(uri, "mysql://") {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		user := u.User.Username()
		pass, _ := u.User.Password()
		host := u.Host
		if strings.HasPrefix(host, "localhost") {
			host = strings.Replace(host, "localhost", "127.0.0.1", 1)
		}
		dbname := strings.TrimPrefix(u.Path, "/")
		if u.RawQuery != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", user, pass, host, dbname, u.RawQuery)
		} else {
			dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, host, dbname)
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}
	// スキーマ投入
	if b, err := os.ReadFile(filepath.Join("testdata", "schema", "mysql_ddl.sql")); err == nil {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		queries := strings.Split(string(b), ";")
		for _, q := range queries {
			q = strings.TrimSpace(q)
			if q == "" {
				continue
			}
			if _, err := tx.Exec(q); err != nil {
				tx.Rollback()
				t.Fatalf("schema exec error: %v, query=%s", err, q)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	mysqlConn = db
	return mysqlConn
}

func TestExample_LoadMySQL(t *testing.T) {
	conn := openMySQLTestingConn(t)
	if _, err := conn.Exec("TRUNCATE TABLE company;"); err != nil {
		t.Fatal(err)
	}

	e := exceltesting.New(conn)

	e.Load(t, exceltesting.LoadRequest{
		TargetBookPath: filepath.Join("testdata", "load_example.xlsx"),
		SheetPrefix:    "",
		IgnoreSheet:    nil,
	})
}

func TestExample_LoadRawFromCSVMySQL(t *testing.T) {
	conn := openMySQLTestingConn(t)
	if _, err := conn.Exec("TRUNCATE TABLE company;"); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(filepath.Join("testdata", "sample.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cr := csv.NewReader(f)
	rs, err := cr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	err = exceltesting.LoadRaw(tx, exceltesting.LoadRawRequest{
		TableName: "company",
		Columns:   rs[0],
		Values:    rs[1:],
	})

	if err != nil {
		t.Fatal(err)
	}

	tx.Rollback()
}
