package mysqldump

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"
	"time"
)

type Table struct {
	Name   string
	SQL    string
	Values string
}

type Dump struct {
	DumpVersion   string
	ServerVersion string
	Tables        []*Table
	CompleteTime  string
	Database      string
	DbDrop        string
}

const version = "0.2.0"

const tmpl = `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}
{{ .DbDrop }}

USE {{ .Database }};
{{range .Tables}}
--
-- Table structure for table {{ .Name }}
--

DROP TABLE IF EXISTS {{ .Name }};
{{ .SQL }};
{{ if .Values }}
--
-- Dumping data for table {{ .Name }}
--

LOCK TABLES {{ .Name }} WRITE;
INSERT INTO {{ .Name }} VALUES {{ .Values }};
UNLOCK TABLES;
{{end}}{{ end }}
-- Dump completed on {{ .CompleteTime }}
`

// Creates a MYSQL Dump based on the options supplied through the dumper.
func (d *Dumper) Dump() (string, error) {
	if d.FileName == "" {
		name := time.Now().Format(d.format)
		d.FileName = path.Join(d.dir, name+".sql")
	}

	var database string
	var dbdrop string

	if e, _ := exists(d.FileName); e {
		return d.FileName, errors.New("Dump '" + d.FileName + "' already exists.")
	}

	// Create .sql file
	f, err := os.Create(d.FileName)

	if err != nil {
		return d.FileName, err
	}

	defer f.Close()

	data := Dump{
		DumpVersion: version,
		Tables:      make([]*Table, 0),
		Database:    database,
		DbDrop:      dbdrop,
	}

	// Get server version
	if data.ServerVersion, err = getServerVersion(d.db); err != nil {
		return d.FileName, err
	}

	if data.Database, err = getDatabase(d.db); err != nil {
		return d.FileName, err
	}
	if d.DropDB {
		data.DbDrop = fmt.Sprintf("DROP DATABASE IF EXISTS %s;\nCREATE DATABASE %s;SET FOREIGN_KEY_CHECKS=0;", data.Database, data.Database)
	}
	// Get tables
	tables, err := getTables(d.db)
	if err != nil {
		return d.FileName, err
	}

	// Get sql for each table
	for _, name := range tables {
		if t, err := createTable(d.db, name); err == nil {
			data.Tables = append(data.Tables, t)
		} else {
			return d.FileName, err
		}
	}

	// Set complete time
	data.CompleteTime = time.Now().String()

	// Write dump to file
	t, err := template.New("mysqldump").Parse(tmpl)
	if err != nil {
		return d.FileName, err
	}
	if err = t.Execute(f, data); err != nil {
		return d.FileName, err
	}

	return d.FileName, nil
}

func (d *Dumper) DumpTables(tables []string) (string, error) {
	if d.FileName == "" {
		name := time.Now().Format(d.format)
		d.FileName = path.Join(d.dir, name+".sql")
	}

	var dumpTables []*Table
	var database string
	var dbdrop string

	// Check dump directory
	if e, _ := exists(d.FileName); e {
		return d.FileName, errors.New("Dump '" + d.FileName + "' already exists.")
	}

	// Create .sql file
	f, err := os.Create(d.FileName)

	if err != nil {
		return d.FileName, err
	}

	defer f.Close()

	data := Dump{
		DumpVersion: version,
		Tables:      dumpTables,
		Database:    database,
		DbDrop:      dbdrop,
	}

	// Get server version
	if data.ServerVersion, err = getServerVersion(d.db); err != nil {
		return d.FileName, err
	}

	// Set data.DbDrop to nothing since we're only working with one or more tables.
	data.DbDrop = ""

	if data.Database, err = getDatabase(d.db); err != nil {
		return d.FileName, err
	}

	// Get sql for each table
	for _, name := range tables {
		if t, err := createTable(d.db, name); err == nil {
			data.Tables = append(data.Tables, t)
		} else {
			return d.FileName, err
		}
	}

	// Set complete time
	data.CompleteTime = time.Now().String()

	// Write dump to file
	t, err := template.New("mysqldump").Parse(tmpl)
	if err != nil {
		return d.FileName, err
	}
	if err = t.Execute(f, data); err != nil {
		return d.FileName, err
	}

	return d.FileName, nil
}

func getTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)
	// Get table list
	tQuery := fmt.Sprintf("SHOW TABLES")
	rows, err := db.Query(tQuery)
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}

	return tables, rows.Err()
}

func getServerVersion(db *sql.DB) (string, error) {
	var server_version sql.NullString
	if err := db.QueryRow("SELECT version()").Scan(&server_version); err != nil {
		return "", err
	}
	return server_version.String, nil
}

func getDatabase(db *sql.DB) (string, error) {
	var server_database sql.NullString
	if err := db.QueryRow("SELECT database()").Scan(&server_database); err != nil {
		return "", err
	}

	return server_database.String, nil
}

func createTable(db *sql.DB, name string) (*Table, error) {
	var err error
	t := &Table{Name: name}

	if t.SQL, err = createTableSQL(db, name); err != nil {
		return nil, err
	}

	if t.Values, err = createTableValues(db, name); err != nil {
		return nil, err
	}

	return t, nil
}

func createTableSQL(db *sql.DB, name string) (string, error) {
	// Get table creation SQL
	var table_return sql.NullString
	var table_sql sql.NullString
	err := db.QueryRow("SHOW CREATE TABLE "+name).Scan(&table_return, &table_sql)

	if err != nil {
		return "", err
	}
	if table_return.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return table_sql.String, nil
}

func createTableValues(db *sql.DB, name string) (string, error) {
	// Get Data
	rows, err := db.Query("SELECT * FROM " + name)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", errors.New("No columns in table " + name + ".")
	}

	// Read data
	data_text := make([]string, 0)
	for rows.Next() {
		// Init temp data storage

		//ptrs := make([]interface{}, len(columns))
		//var ptrs []interface {} = make([]*sql.NullString, len(columns))

		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i, _ := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = value.String
			}
		}

		data_text = append(data_text, "('"+strings.Join(dataStrings, "','")+"')")
	}

	return strings.Join(data_text, ","), rows.Err()
}
