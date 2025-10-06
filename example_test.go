package exceltesting_test

import (
	"database/sql"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fc-shota-miyazaki/go-exceltesting"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
)

var conn *sql.DB

// getDriverFromDSN は接続文字列から適切なドライバー名を返します
func getDriverFromDSN(dsn string) string {
	if strings.HasPrefix(dsn, "mysql://") {
		return "mysql"
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return "pgx"
	}
	// デフォルトはPostgreSQL
	return "pgx"
}

func TestMain(m *testing.M) {
	// 環境変数から接続文字列を取得
	uri := os.Getenv("EXCELTESTING_CONNECTION")
	if uri == "" {
		uri = "postgres://excellocal:password@localhost:15432/excellocal"
	}

	var err error
	driver := getDriverFromDSN(uri)
	conn, err = sql.Open(driver, uri)
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()
	m.Run()

}

func TestExample_Load(t *testing.T) {
	if _, err := conn.Exec("TRUNCATE company;"); err != nil {
		t.Fatal(err)
	}

	e := exceltesting.New(conn)

	e.Load(t, exceltesting.LoadRequest{
		TargetBookPath: filepath.Join("testdata", "load_example.xlsx"),
		SheetPrefix:    "",
		IgnoreSheet:    nil,
	})
}

func TestExample_LoadRawFromCSV(t *testing.T) {
	if _, err := conn.Exec("TRUNCATE company;"); err != nil {
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
