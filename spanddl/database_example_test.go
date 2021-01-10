package spanddl_test

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip-spanner/spanddl"
)

func ExampleDatabase() {
	var db spanddl.Database
	files, err := filepath.Glob("../testdata/migrations/freight/*.up.sql")
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	for _, file := range files {
		fileContent, err := ioutil.ReadFile(file)
		if err != nil {
			panic(err) // TODO: Handle error.
		}
		ddl, err := spansql.ParseDDL(file, string(fileContent))
		if err != nil {
			panic(err) // TODO: Handle error.
		}
		if err := db.ApplyDDL(ddl); err != nil {
			panic(err) // TODO: Handle error.
		}
	}
	for _, table := range db.Tables {
		fmt.Println(table.Name)
	}
	// Output:
	// shippers
	// sites
	// shipments
	// line_items
}
