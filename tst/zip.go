package main

import (
	"archive/zip"
	"fmt"
	"os"
)

func main() {
	text := `
For God so loved the world
That he gave his only begotten Son
that whosoever beliveth in him shall
not perish but have everlasting life.
`
	outfile, _ := os.Create("myfile.zip")
	defer outfile.Close()
	z := zip.NewWriter(outfile)
	defer z.Close()
	filez := []string{"1.txt", "2.txt", "3.txt"}
	for _, f := range filez {
		zw, _ := z.Create(f)
		zf := os.Open(zw)
		fmt.Fprintf(zw, text)
		//zw.Write([]byte(text))
	}
}
