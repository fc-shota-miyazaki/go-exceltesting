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

func TestMain(m *testing.M) {
	// MySQL接続文字列
	uri := os.Getenv("EXCELTESTING_MYSQL_CONNECTION")
	if uri == "" {
		uri = "mysql://excellocal:password@mysql:3306/excellocal"
	}

	// go-sql-driver/mysql は URI ではなく DSN を受け取るため、必要に応じて変換
	dsn := uri
	if strings.HasPrefix(uri, "mysql://") {
		u, err := url.Parse(uri)
		if err != nil {
			os.Exit(1)
		}
		user := u.User.Username()
		pass, _ := u.User.Password()
		host := u.Host // host:port
		dbname := strings.TrimPrefix(u.Path, "/")
		if u.RawQuery != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", user, pass, host, dbname, u.RawQuery)
		} else {
			dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, host, dbname)
		}
	}

	var err error
	mysqlConn, err = sql.Open("mysql", dsn)
	if err != nil {
		os.Exit(1)
	}
	if err = mysqlConn.Ping(); err != nil {
		os.Exit(1)
	}

	code := m.Run()
	mysqlConn.Close()
	os.Exit(code)
}

func TestExample_LoadMySQL(t *testing.T) {
	if _, err := mysqlConn.Exec("TRUNCATE TABLE company;"); err != nil {
		t.Fatal(err)
	}

	e := exceltesting.New(mysqlConn)

	e.Load(t, exceltesting.LoadRequest{
		TargetBookPath: filepath.Join("testdata", "load_example.xlsx"),
		SheetPrefix:    "",
		IgnoreSheet:    nil,
	})
}

func TestExample_LoadRawFromCSVMySQL(t *testing.T) {
	if _, err := mysqlConn.Exec("TRUNCATE TABLE company;"); err != nil {
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

	tx, err := mysqlConn.Begin()
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
