package exceltesting

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/xuri/excelize/v2"
	"golang.org/x/exp/slices"
)

const (
	tempTablePrefix = "temp_"
)

// New はExcelからテストデータを投入できる構造体のファクトリ関数です
func New(db *sql.DB) *exceltesing {
	if db == nil {
		panic("db is nil")
	}
	return &exceltesing{db}
}

type exceltesing struct {
	db *sql.DB
}

// Load はExcelのBookを読み込み、データベースに事前データを投入します。
func (e *exceltesing) Load(t *testing.T, r LoadRequest) {
	t.Helper()
	ctx := context.Background()

	if err := e.LoadWithContext(ctx, r); err != nil {
		t.Fatalf("load: %v", err)
	}
}

func (e *exceltesing) LoadWithContext(ctx context.Context, r LoadRequest) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("exceltesing: start transaction: %w", err)
	}
	defer tx.Rollback()

	f, err := excelize.OpenFile(r.TargetBookPath)
	if err != nil {
		return fmt.Errorf("exceltesing: excelize.OpenFile: %w", err)
	}
	defer f.Close()

	for _, sheet := range f.GetSheetList() {
		if slices.Contains(r.IgnoreSheet, sheet) {
			continue
		}
		if strings.HasPrefix(sheet, r.SheetPrefix) {
			table, err := e.loadExcelSheet(f, sheet)
			if err != nil {
				return fmt.Errorf("exceltesing: load excel sheet, sheet = %s: %w", sheet, err)
			}

			if r.EnableAutoCompleteNotNullColumn {
				cs, err := e.tableColumns(table.name)
				if err != nil {
					return fmt.Errorf("exceltesing: get table(%s)'s columns: %w", table.name, err)
				}
				for i := range cs {
					cs[i].data = defaultValueFromDBType(cs[i].dataType)
				}
				table.merge(cs)
			}

			if err := e.insertData(table); err != nil {
				return fmt.Errorf("exceltesing: insert data to %s: %w", table.name, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("exceltesing: commit: %w", err)
	}

	if r.EnableDumpCSV {
		if err := e.dumpBookAsCSV(r.TargetBookPath); err != nil {
			return fmt.Errorf("dump csv: %w", err)
		}
	}

	return nil
}

// Compare はExcelの期待結果と実際にデータベースに登録されているデータを比較して
// 差分がある場合は報告します。
// 値の比較は go-cmp (https://github.com/google/go-cmp) を利用しています。
func (e *exceltesing) Compare(t *testing.T, r CompareRequest) bool {
	t.Helper()

	equal, errors := e.CompareWithContext(context.Background(), r)
	for _, err := range errors {
		t.Error(err)
	}

	return equal
}

func (e *exceltesing) CompareWithContext(_ context.Context, r CompareRequest) (bool, []error) {
	tx, err := e.db.Begin()
	if err != nil {
		return false, []error{fmt.Errorf("exceltesting: failed to start transaction: %w", err)}
	}
	defer tx.Rollback()

	f, err := excelize.OpenFile(r.TargetBookPath)
	if err != nil {
		return false, []error{fmt.Errorf("exceltesting: failed to open excel file: %w", err)}
	}
	defer f.Close()

	equal := true
	var errs []error

	for _, sheet := range f.GetSheetList() {
		if slices.Contains(r.IgnoreSheet, sheet) {
			continue
		}
		if strings.HasPrefix(sheet, r.SheetPrefix) {
			table, err := e.loadExcelSheet(f, sheet)
			if err != nil {
				errs = append(errs, fmt.Errorf("exceltesting: failed to load excel sheet, sheet = %s: %v", sheet, err))
				equal = false
				continue
			}
			got, want, err := e.comparativeSource(table, &r)
			if err != nil {
				errs = append(errs, fmt.Errorf("exceltesting: failed to fetch comparative source: %w", err))
				equal = false
				continue
			}

			opts := []cmp.Option{
				cmpopts.EquateNaNs(),
				cmp.Comparer(func(x, y *big.Int) bool {
					return x.Cmp(y) == 0
				}),
				cmp.AllowUnexported(x{}),
			}
			if diff := cmp.Diff(want, got, opts...); diff != "" {
				errs = append(errs, fmt.Errorf("table(%s) mismatch (-want +got):\n%s", table.name, diff))
				equal = false
				continue
			}
		}
	}

	if r.EnableDumpCSV {
		if err := e.dumpBookAsCSV(r.TargetBookPath); err != nil {
			return false, []error{fmt.Errorf("dump csv: %w", err)}
		}
	}

	return equal, errs
}

// DumpCSV はExcelブックの全シートをCSVにDumpします。
//
// DumpRequest.TargetBookPaths で指定されたパスにディレクトリを作成し、
// CSVファイルをDumpします。
//
// Deprecated: LoadRequest.EnableDumpCSV や CompareRequest.EnableDumpCSV のオプションを利用してください
func (e *exceltesing) DumpCSV(t *testing.T, r DumpRequest) {
	t.Helper()

	e.dumpCSV(t, r.TargetBookPaths...)
}

func (e *exceltesing) dumpCSV(t *testing.T, paths ...string) {
	t.Helper()

	if err := e.dumpBookAsCSV(paths...); err != nil {
		t.Error(err)
	}
}

func (e *exceltesing) dumpBookAsCSV(paths ...string) error {
	const columnsRowNum = 9

	for _, path := range paths {
		ef, err := excelize.OpenFile(path)
		if err != nil {
			return fmt.Errorf("exceltesing: excelize.OpenFile: %w", err)
		}
		defer ef.Close()

		for _, sheet := range ef.GetSheetList() {
			rows, err := ef.Rows(sheet)
			if err != nil {
				return fmt.Errorf("exceltesing: rows: %w", err)
			}
			rr, err := ef.GetRows(sheet)
			if err != nil {
				return fmt.Errorf("exceltesing: get rows: %w", err)
			}
			outDir := filepath.Join(filepath.Dir(path), "csv")
			if _, err := os.Stat(outDir); os.IsNotExist(err) {
				if err := os.Mkdir(outDir, 0755); err != nil {
					return fmt.Errorf("exceltesing: create directory: %w", err)
				}
			}

			if len(rr) == columnsRowNum {
				continue
			}

			outFileName := fmt.Sprintf("%s_%s.csv", getFileNameWithoutExt(path), sheet)
			f, err := os.Create(filepath.Join(outDir, outFileName))
			if err != nil {
				return fmt.Errorf("exceltesing: create file: %w", err)
			}
			defer f.Close()

			writer := csv.NewWriter(f)
			defer writer.Flush()

			rowCnt := 0
			for rows.Next() {
				cols, err := rows.Columns()
				if err != nil {
					return fmt.Errorf("exceltesing: rows.Columns: %w", err)
				}
				if 3 <= rowCnt && rowCnt <= 6 {
					rowCnt++
					continue
				}
				if rowCnt >= 7 {
					if len(cols) == 0 {
						rowCnt++
						continue
					}
					cols = cols[1:]
				}
				if err := writer.Write(cols); err != nil {
					return fmt.Errorf("exceltesing: writer.Write(): %w", err)
				}
				rowCnt++
			}
		}
	}
	return nil
}

// LoadRequest はExcelからデータを投入するための設定です。
type LoadRequest struct {
	// ロード対象Excelパス
	TargetBookPath string
	// ロード対象シートプレフィックス
	SheetPrefix string
	// 無視シート
	IgnoreSheet []string
	// EnableAutoCompleteNotNullColumn はExcel上でカラムの指定がない場合にデフォルト値で補完します
	// カラムにNOT NULL制約がある場合のみ補完します
	EnableAutoCompleteNotNullColumn bool
	// EnableDumpCSV はExcelファイルをCSVファイルとしてDumpします
	EnableDumpCSV bool
}

// CompareRequest はExcelとデータベースの値を比較するための設定です。
type CompareRequest struct {
	// ロード対象Excelパス
	TargetBookPath string
	// ロード対象シートプレフィックス
	SheetPrefix string
	// 無視シート
	IgnoreSheet []string
	// 無視するカラム名
	IgnoreColumns []string
	// EnableDumpCSV はExcelファイルをCSVファイルとしてDumpします
	EnableDumpCSV bool
}

// DumpRequest はExcelをCSVにDumpするための設定です。
type DumpRequest struct {
	// dump対象Excelパス
	TargetBookPaths []string
}

func (e *exceltesing) loadExcelSheet(f *excelize.File, targetSheet string) (*table, error) {
	var (
		tableNmCell        = "A2"
		columnDefineRowNum = 9
	)

	formatVersion := extractSheetFormatVersion(f, targetSheet)
	if formatVersion == "2.0" {
		columnDefineRowNum = 6
	}

	tableNm, err := f.GetCellValue(targetSheet, tableNmCell)
	if err != nil {
		return nil, fmt.Errorf("get cell value: %w", err)
	}
	if tableNm == "" {
		return nil, fmt.Errorf("table name is empty")
	}

	rows, err := f.GetRows(targetSheet)
	if err != nil {
		return nil, fmt.Errorf("get row: %w", err)
	}

	columns := getExcelColumns(rows, columnDefineRowNum)
	data, err := getExcelData(rows, columnDefineRowNum)
	if err != nil {
		return nil, fmt.Errorf("get excel data: %w", err)
	}

	return &table{
		name:    tableNm,
		columns: columns,
		data:    data,
	}, nil
}

// comparativeSource はデータベースに格納されている実際のテーブルの値と、Excelから取得した期待する結果の値を
// 比較可能な値として取得します。
func (e *exceltesing) comparativeSource(t *table, req *CompareRequest) ([][]x, [][]x, error) {
	pk, err := e.getPrimaryKeyColumns(t.name)
	if err != nil {
		return nil, nil, err
	}

	q1, cs, err := e.buildComparingQuery(t, pk, req)
	if err != nil {
		return nil, nil, err
	}

	got, err := e.getComparingData(q1, len(cs))
	if err != nil {
		return nil, nil, err
	}

	if err := e.createTempTable(t.name); err != nil {
		return nil, nil, fmt.Errorf("create temporary table: %w", err)
	}

	c := t.DeepCopy()
	c.name = tempTablePrefix + c.name
	if err := e.insertData(&c); err != nil {
		return nil, nil, fmt.Errorf("insert data to %s: %w", c.name, err)
	}

	q2, _, err := e.buildComparingQuery(&c, pk, req)
	if err != nil {
		return nil, nil, err
	}

	want, err := e.getComparingData(q2, len(cs))
	if err != nil {
		return nil, nil, err
	}

	return convert(got, cs), convert(want, cs), nil
}

func (e *exceltesing) insertData(t *table) error {
	if _, err := e.db.ExecContext(context.TODO(), fmt.Sprintf(`TRUNCATE TABLE %s;`, t.name)); err != nil {
		return fmt.Errorf("truncate table %s: %w", t.name, err)
	}

	if len(t.data) == 0 {
		return nil
	}

	insertSQL := t.buildInsertSQL()
	_, err := e.db.ExecContext(context.TODO(), insertSQL)
	return err
}

func (e *exceltesing) createTempTable(tableName string) error {
	// PostgreSQL 互換
	queryPG := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s AS SELECT * FROM %s WHERE 0 = 1;", tempTablePrefix+tableName, tableName)
	if _, err := e.db.Exec(queryPG); err == nil {
		return nil
	}
	// MySQL 互換
	queryMySQL := fmt.Sprintf("CREATE TEMPORARY TABLE IF NOT EXISTS %s AS SELECT * FROM %s WHERE 0 = 1;", tempTablePrefix+tableName, tableName)
	_, err := e.db.Exec(queryMySQL)
	return err
}

func (e *exceltesing) buildComparingQuery(t *table, primaryKey string, req *CompareRequest) (string, []string, error) {
	columns := make([]string, 0, len(t.columns))
	for _, c := range t.columns {
		if slices.Contains(req.IgnoreColumns, c) {
			continue
		}
		columns = append(columns, c)
	}

	var querySQL string
	querySQL += "SELECT "
	for i, column := range columns {
		if i > 0 {
			querySQL += ", "
		}
		querySQL += column
	}
	querySQL += fmt.Sprintf(" FROM %s ORDER BY %s;", t.name, primaryKey)
	return querySQL, columns, nil
}

func (e *exceltesing) getComparingData(q string, len int) ([][]any, error) {
	var got [][]any

	rows, err := e.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		g := make([]any, len)
		for i := range g {
			g[i] = &g[i]
		}
		if err := rows.Scan(g...); err != nil {
			return nil, err
		}
		got = append(got, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return got, nil
}

type dbColumn struct {
	name     string
	dataType string
	data     string
}

func (e *exceltesing) tableColumns(tableName string) ([]dbColumn, error) {
	var columns []dbColumn
	// まずはPostgreSQL向けクエリで試行
	rows, err := e.db.Query(getTableNotNullColumns, tableName)
	if err != nil {
		// MySQL向けのクエリにフォールバック
		const mysqlNotNullQuery = `
SELECT
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = ?
  AND is_nullable = 'NO'
  AND column_default IS NULL
ORDER BY ordinal_position;`
		rows, err = e.db.Query(mysqlNotNullQuery, tableName)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	for rows.Next() {
		var c dbColumn
		if err := rows.Scan(&c.name, &c.dataType); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

// getPrimaryKeyColumns は主キー列名をカンマ区切りで返します（複合主キー対応）
func (e *exceltesing) getPrimaryKeyColumns(tableName string) (string, error) {
	// PostgreSQL向けクエリを試行
	var pk string
	if err := e.db.QueryRow(getPrimaryKeyQuery, tableName).Scan(&pk); err == nil && pk != "" {
		return pk, nil
	}
	// MySQL向けクエリにフォールバック
	const mysqlPKQuery = `
SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') AS column_names
FROM information_schema.KEY_COLUMN_USAGE
WHERE table_schema = DATABASE()
  AND table_name = ?
  AND constraint_name = 'PRIMARY';`
	if err := e.db.QueryRow(mysqlPKQuery, tableName).Scan(&pk); err != nil {
		return "", err
	}
	if strings.TrimSpace(pk) == "" {
		return "", fmt.Errorf("primary key not found: %s", tableName)
	}
	return pk, nil
}

func getExcelColumns(rows [][]string, rowNum int) []string {
	columns := make([]string, 0, len(rows[rowNum-1]))

	// 1列目は説明項目で値と無関係のため読み飛ばす
	for _, cell := range rows[rowNum-1][1:] {
		cell = strings.Trim(strings.Trim(cell, "　"), " ")
		if cell == "" {
			// 空カラムのskip
			continue
		}
		columns = append(columns, cell)
	}

	return columns
}

func getExcelData(rows [][]string, rowNum int) ([][]string, error) {
	columns := getExcelColumns(rows, rowNum)

	var data [][]string
	for _, row := range rows[rowNum:] {
		rowStr := ""
		for _, cell := range row {
			rowStr = rowStr + strings.Trim(strings.Trim(cell, "　"), " ")
		}
		if rowStr == "" {
			continue
		}
		// 1列目が空ならskip
		if row[0] == "" {
			continue
		}
		// 不足セルを空文字でパディングして列数を揃える
		padded := make([]string, len(columns))
		for j := 0; j < len(columns); j++ {
			idx := 1 + j // 先頭列(番号)を除外
			if idx < len(row) {
				padded[j] = strings.Trim(strings.Trim(row[idx], "　"), " ")
			} else {
				padded[j] = ""
			}
		}
		data = append(data, padded)
	}
	return data, nil
}

func getFileNameWithoutExt(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	return base[0 : len(base)-len(ext)]
}

// x はDBの値にカラムを付与した構造体です。
// 比較出力を簡潔にするため、column/value は string に正規化します。
type x struct {
	column string
	value  string
}

func convert(vs [][]any, columns []string) [][]x {
	resp := make([][]x, len(vs))
	for i, r := range vs {
		for j, v := range r {
			// すべて文字列へ正規化
			var s string
			switch t := v.(type) {
			case nil:
				s = ""
			case []byte:
				s = string(t)
			default:
				s = fmt.Sprint(t)
			}
			resp[i] = append(resp[i], x{column: columns[j], value: s})
		}
	}
	return resp
}

// extractSheetFormatVersion is extracting exceltesting sheet format version.
// default 1.0
func extractSheetFormatVersion(f *excelize.File, sheet string) string {
	index := f.GetSheetIndex(sheet)
	if index == -1 {
		return "1.0"
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		return "1.0"
	}
	if len(rows) < 3 {
		return "1.0"
	}

	row := rows[2] // 3行目に記載があるとする
	if len(row) < 2 {
		return "1.0"
	}

	if strings.TrimSpace(strings.ToLower(row[0])) == "version" {
		return strings.TrimSpace(row[1])
	}

	return "1.0"
}
