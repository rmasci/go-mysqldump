package mysqldump

import (
	"archive/zip"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/rmasci/verbose"
)

 //TODO: Work not only on Pump Dump but Restore.
 //1. Pump Dump in parallel, configure the number of concurrent processes working.
 //2. Work on splitting a normal dump into separate files within a zip archive.
 //3. Can we restore...Looks like all I'd have to do is run through each


/*
Register a new dumper.

	db: Database that will be dumped (https://golang.org/pkg/database/sql/#DB).
	dir: Path to the directory where the dumps will be stored.
	format: Format to be used to name each dump file. Uses time.Time.Format (https://golang.org/pkg/time/#Time.Format). format appended with '.sql'.
*/

func PumpNew(db *sql.DB, name string) (*Data, string, error) {
	verb := verbose.New("default")
	var data *Data
	verb.V = true
	if name == "" {
		dir := "/var/tmp/"
		format := "20060102-1504"
		name = fmt.Sprintf("sqlpump-%v", time.Now().Format(format))
		name = path.Join(dir, name)
	}
	if !strings.HasSuffix(name, ".zip") {
		name += ".zip"
	}
	verb.Println("Name:", name)
	// Check dump directory
	if e, _ := exists(name); e {
		return nil, name, errors.New("Dump '" + name + "' already exists.")
	}

	f, err := os.Create(name)
	if err != nil {
		return nil, name, err
	}
	z := zip.NewWriter(f)
	data = &Data{
		ZOut:        z,
		ZFile:       f,
		Connection:  db,
		FileName:    name,
		GzipDump:    false,
		PrintTables: true,
	}
	verb.Println("Getting DB Name")
	data.DBName, err = getDBName(data.Connection)
	verb.Printf("DB Name %v, err %v\n", data.DBName, err)
	return data, name, nil
}

// // Pump dumps the database using go routines.
func (data *Data) Pump() error {
	meta := metaData{
		DumpVersion: Version,
	}
	data.GzipDump = false

	// Start the read only transaction and defer the rollback until the end
	// This way the database will have the exact state it did at the begining of
	// the backup and nothing can be accidentally committed
	if err := data.begin(); err != nil {
		return err
	}
	defer data.rollback()

	if err := meta.updateServerVersion(data); err != nil {
		return err
	}
	/* At some point I want to add dump at the top of the sql.
	if data.DropDB {
		data.headerTmpl=fmt.Sprintf("%v\nDROP %v", )
	}
	*/
	if err := data.headerTmpl.Execute(data.Out, meta); err != nil {
		return err
	}

	tables, err := data.getTables()
	if err != nil {
		return err
	}
	for _, t := range tables {
		var err error
		t += ".sql"
		data.Out, err = data.ZOut.Create(t)
		if err != nil {
			return err
		}
		err = data.Dump(t)
		if err != nil {
			return err
		}
		err = data.ZOut.Flush()
		if err != nil {
			return err
		}
	}
	data.PumpClose()
	return nil
}

func (data *Data) PumpClose() {
	data.ZOut.Close()
	data.ZFile.Close()
}

//	if err != nil {
//		return err
//	}
//	// Create Zip file
//
//}
//
//func GetTables(db *sql.DB, ignore ...interface{}) ([]string, error) {
//	tables := make([]string, 0)
//
//	rows, err := db.Query("SHOW TABLES")
//	if err != nil {
//		return tables, err
//	}
//	defer rows.Close()
//
//	for rows.Next() {
//		var table sql.NullString
//		if err := rows.Scan(&table); err != nil {
//			return tables, err
//		}
//		if table.Valid && !isIgnoredTable(table.String, ignore) {
//			tables = append(tables, table.String)
//		}
//	}
//	return tables, rows.Err()
//}
//
//func isIgnoredTable(table string, ignore ...interface{}) bool {
//	for _,i := range ignore {
//		t:=fmt.Sprintf("%v", i)
//		if t == table {
//			return true
//		}
//	}
//	return false
//}
