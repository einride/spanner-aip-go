package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/internal/codegen/descriptorcodegen"
	"go.einride.tech/aip-spanner/internal/codegen/tablecodegen"
	"go.einride.tech/aip-spanner/spanddl"
	"gopkg.in/yaml.v2"
)

const generatedBy = "aip-spanner-go"

func main() {
	log.SetFlags(0)
	configFilePath := flag.String("config", "aip-spanner-go.yaml", "config file")
	flag.Parse()
	if flag.Arg(0) != "generate" {
		log.Fatal("usage: aip-spanner-go generate [-config <config>]")
	}
	configFile, err := os.Open(*configFilePath)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := configFile.Close(); err != nil {
			log.Panic(err)
		}
	}()
	var config struct {
		SchemaGlobs []string `yaml:"schema"`
		Package     struct {
			Name string `yaml:"name"`
			Path string `yaml:"path"`
		} `yaml:"package"`
	}
	if err := yaml.NewDecoder(configFile).Decode(&config); err != nil {
		log.Panic(err)
	}
	var db spanddl.Database
	for _, schemaGlob := range config.SchemaGlobs {
		schemaFiles, err := filepath.Glob(schemaGlob)
		if err != nil {
			log.Panic(err)
		}
		for _, schemaFile := range schemaFiles {
			schema, err := ioutil.ReadFile(schemaFile)
			if err != nil {
				log.Panic(err)
			}
			ddl, err := spansql.ParseDDL(schemaFile, string(schema))
			if err != nil {
				log.Panic(err)
			}
			if err := db.ApplyDDL(ddl); err != nil {
				log.Panic(err)
			}
		}
	}
	if err := os.MkdirAll(config.Package.Path, 0o775); err != nil {
		log.Panic(err)
	}
	filename := filepath.Join(config.Package.Path, "descriptor_gen.go")
	f := codegen.NewFile(codegen.FileConfig{
		Filename:    filename,
		Package:     config.Package.Name,
		GeneratedBy: generatedBy,
	})
	descriptorcodegen.DatabaseDescriptorCodeGenerator{
		Database: &db,
	}.GenerateCode(f)
	content, err := f.Content()
	if err != nil {
		log.Panic(err)
	}
	if err := ioutil.WriteFile(filename, content, 0o600); err != nil {
		log.Panic(err)
	}
	log.Println("wrote:", filename)
	if len(db.Tables) > 0 {
		filename := filepath.Join(config.Package.Path, "tables_gen.go")
		f := codegen.NewFile(codegen.FileConfig{
			Filename:    filename,
			Package:     config.Package.Name,
			GeneratedBy: generatedBy,
		})
		for _, table := range db.Tables {
			tablecodegen.ReadTransactionCodeGenerator{Table: table}.GenerateCode(f)
			tablecodegen.RowIteratorCodeGenerator{Table: table}.GenerateCode(f)
			tablecodegen.PrimaryKeyCodeGenerator{Table: table}.GenerateCode(f)
			tablecodegen.PartialKeyCodeGenerator{Table: table}.GenerateCode(f)
			tablecodegen.KeyRangeCodeGenerator{Table: table}.GenerateCode(f)
			tablecodegen.RowCodeGenerator{Table: table}.GenerateCode(f)
			interleavedTables := db.InterleavedTables(table.Name)
			if len(interleavedTables) == 0 {
				continue
			}
			tablecodegen.InterleavedReadTransactionCodeGenerator{
				Table:             table,
				InterleavedTables: interleavedTables,
			}.GenerateCode(f)
			tablecodegen.InterleavedRowIteratorCodeGenerator{
				Table:             table,
				InterleavedTables: interleavedTables,
			}.GenerateCode(f)
			tablecodegen.InterleavedRowCodeGenerator{
				Table:             table,
				InterleavedTables: interleavedTables,
			}.GenerateCode(f)
		}
		tablecodegen.CommonCodeGenerator{}.GenerateCode(f)
		content, err := f.Content()
		if err != nil {
			log.Panic(err)
		}
		if err := ioutil.WriteFile(filename, content, 0o600); err != nil {
			log.Panic(err)
		}
		log.Println("wrote:", filename)
	}
}
