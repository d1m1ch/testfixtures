package testfixtures

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	paramTypeDollar = iota + 1
	paramTypeQuestion
	paramTypeAtSign
)

type loadFunction func(tx *sql.Tx) error

type helper interface {
	init(*sql.DB) error
	disableReferentialIntegrity(*sql.DB, loadFunction) error
	paramType() int
	databaseName(queryable) (string, error)
	tableNames(queryable) ([]string, error)
	isTableModified(queryable, string) (bool, error)
	afterLoad(queryable) error
	saveState(queryable, []string) error
	quoteKeyword(string) string
	whileInsertOnTable(*sql.Tx, string, func() error) error
	cleanTables(tx *sql.Tx, tables ...string) error
}

type queryable interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}

// batchSplitter is an interface with method which returns byte slice for
// splitting SQL batches. This need to split sql statements and run its
// separately.
//
// For Microsoft SQL Server batch splitter is "GO". For details see
// https://docs.microsoft.com/en-us/sql/t-sql/language-elements/sql-server-utilities-statements-go
type batchSplitter interface {
	splitter() []byte
}

var (
	_ helper = &mySQL{}
	_ helper = &postgreSQL{}
	_ helper = &sqlite{}
	_ helper = &sqlserver{}
)

type baseHelper struct{}

func (baseHelper) init(_ *sql.DB) error {
	return nil
}

func (baseHelper) quoteKeyword(str string) string {
	return fmt.Sprintf(`"%s"`, str)
}

func (baseHelper) whileInsertOnTable(_ *sql.Tx, _ string, fn func() error) error {
	return fn()
}

func (baseHelper) isTableModified(_ queryable, _ string) (bool, error) {
	return true, nil
}

func (baseHelper) afterLoad(_ queryable) error {
	return nil
}

func (baseHelper) saveState(queryable, []string) error {
	return nil
}

func (h baseHelper) cleanTables(tx *sql.Tx, tables ...string) error {

	sb := &strings.Builder{}
	for _, table := range tables {
		sb.WriteString(fmt.Sprintf("DELETE FROM %s ;\n", table))
	}

	if _, err := tx.Exec(sb.String()); err != nil {
		return fmt.Errorf(`testfixtures: could not clean tables "%v using query %s": %w`, tables, sb.String(), err)
	}
	return nil
}
