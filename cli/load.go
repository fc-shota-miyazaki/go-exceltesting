package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/fc-shota-miyazaki/go-exceltesting"

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

func Load(dbSource string, r exceltesting.LoadRequest) error {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	driver := getDriverFromDSN(dbSource)
	db, err := sql.Open(driver, dbSource)
	if err != nil {
		return fmt.Errorf("database open: %w", err)
	}
	e := exceltesting.New(db)

	if err := e.LoadWithContext(ctx, r); err != nil {
		return fmt.Errorf("load: %w", err)
	}

	return nil
}
