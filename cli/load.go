package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"

	"github.com/fc-shota-miyazaki/go-exceltesting"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func Load(dbSource string, r exceltesting.LoadRequest) error {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	driver, dsn, err := normalizeDSN(dbSource)
	if err != nil {
		return fmt.Errorf("dsn normalize: %w", err)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return fmt.Errorf("database open: %w", err)
	}
	e := exceltesting.New(db)

	if err := e.LoadWithContext(ctx, r); err != nil {
		return fmt.Errorf("load: %w", err)
	}

	return nil
}
