package cli

import (
	"fmt"
	"net/url"
	"strings"
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

// normalizeDSN は各ドライバに適したDSNへ正規化します
func normalizeDSN(dsn string) (driver string, normalized string, err error) {
	driver = getDriverFromDSN(dsn)
	if driver != "mysql" {
		return driver, dsn, nil
	}

	// go-sql-driver/mysql はURI形式ではなく DSN(user:pass@tcp(host:port)/db?opts) を受け取る
	if !strings.HasPrefix(dsn, "mysql://") {
		return driver, dsn, nil
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", fmt.Errorf("parse dsn: %w", err)
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	host := u.Host // host:port
	// IPv6での接続拒否回避のため、localhostはIPv4に正規化
	if strings.HasPrefix(host, "localhost") {
		host = strings.Replace(host, "localhost", "127.0.0.1", 1)
	}
	dbname := strings.TrimPrefix(u.Path, "/")
	if u.RawQuery != "" {
		return driver, fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", user, pass, host, dbname, u.RawQuery), nil
	}
	return driver, fmt.Sprintf("%s:%s@tcp(%s)/%s", user, pass, host, dbname), nil
}
