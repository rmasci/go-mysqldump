package mysqldump

import (
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

/*
Register a new dumper.

	db: Database that will be dumped (https://golang.org/pkg/database/sql/#DB).
	dir: Path to the directory where the dumps will be stored.
	format: Format to be used to name each dump file. Uses time.Time.Format (https://golang.org/pkg/time/#Time.Format). format appended with '.sql'.
*/
func Register(db *sql.DB, gzipdump bool, a ...interface{}) (*Data, string, error) {
	var p, name string
	if len(a) <= 0 {
		dir := "/var/tmp/"
		format := "20060102-1504"
		name = fmt.Sprintf("sqldump-%v", time.Now().Format(format))
		p = path.Join(dir, name+".sql")
	} else {
		p = fmt.Sprintf("%v", a[0])
	}
	// Check dump directory
	if e, _ := exists(p); e {
		return nil, p, errors.New("Dump '" + p + "' already exists.")
	}

	if gzipdump {
		p=fmt.Sprintf("%s.gz", p)
		// Create .sql file
		f, err := os.Create(p)
		if err != nil {
			return nil, p, err
		}
		gz:=gzip.NewWriter(f)

		return &Data{
			FileName: p,
			GzipDump: true,
			Out:        gz,
			Connection: db,
		}, p, nil
	}
	f, err := os.Create(p)
	if err != nil {
		return nil, p, err
	}
	return &Data{
		Out:        f,
		Connection: db,
	}, p, nil
}

// Dump Creates a MYSQL dump from the connection to the stream.
func Dump(db *sql.DB, out io.Writer) error {
	return (&Data{
		Connection: db,
		Out:        out,
	}).Dump()
}

// Close the dumper.
// Will also close the database the dumper is connected to as well as the out stream if it has a Close method.
//
// Not required.
func (d *Data) Close() error {
	defer func() {
		d.Connection = nil
		d.Out = nil
	}()
	if out, ok := d.Out.(io.Closer); ok {
		out.Close()
	}
	return d.Connection.Close()
}

func exists(p string) (bool, os.FileInfo) {
	f, err := os.Open(p)
	if err != nil {
		return false, nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return false, nil
	}
	return true, fi
}

func isDir(p string) bool {
	if e, fi := exists(p); e {
		return fi.Mode().IsDir()
	}
	return false
}
