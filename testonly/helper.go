package testonly

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
)

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

func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()

	// 環境変数から接続文字列を取得
	dsn := os.Getenv("EXCELTESTING_CONNECTION")
	if dsn == "" {
		// デフォルトのPostgreSQL接続文字列
		const (
			DBUser = "excellocal"
			DBPass = "password"
			DBHost = "localhost"
			DBPort = "15432"
			DBName = "excellocal"
		)
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", DBUser, DBPass, DBHost, DBPort, DBName)
	}

	driver := getDriverFromDSN(dsn)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	return db
}

func ExecSQLFile(t *testing.T, db *sql.DB, filePath string) {
	t.Helper()

	b, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	queries := strings.Split(string(b), ";")
	for _, query := range queries {

		q := strings.TrimSpace(query)
		if q == "" {
			continue
		}
		if _, err = tx.Exec(q); err != nil {
			t.Fatalf("failed to exec sql, query = %s: %v", q, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
