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

func Compare(dbSource string, r exceltesting.CompareRequest) error {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	driver := getDriverFromDSN(dbSource)
	db, err := sql.Open(driver, dbSource)
	if err != nil {
		return fmt.Errorf("database open: %w", err)
	}
	e := exceltesting.New(db)

	equal, errors := e.CompareWithContext(ctx, r)
	if equal {
		return nil
	}

	return multiError{errs: errors}
}

type multiError struct {
	errs []error
}

func (m multiError) Error() string {
	b := strings.Builder{}
	for _, err := range m.errs {
		b.WriteString(err.Error())
		b.WriteString("\n")
	}
	return b.String()
}
