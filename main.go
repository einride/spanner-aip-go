package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/internal/codegen/databasecodegen"
	"go.einride.tech/spanner-aip/internal/codegen/descriptorcodegen"
	"go.einride.tech/spanner-aip/internal/config"
	"gopkg.in/yaml.v3"
)

const generatedBy = "spanner-aip-go"

func main() {
	log.SetFlags(0)
	configFilePath := flag.String("config", "spanner.yaml", "config file")
	flag.Parse()
	if flag.Arg(0) != "generate" {
		log.Fatal("usage: spanner-aip-go generate [-config <config>]")
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
	var codeGenerationConfig config.CodeGenerationConfig
	if err := yaml.NewDecoder(configFile).Decode(&codeGenerationConfig); err != nil {
		log.Panic(err)
	}
	for _, databaseConfig := range codeGenerationConfig.Databases {
		db, err := databaseConfig.LoadDatabase()
		if err != nil {
			log.Panic(err)
		}
		for _, resourceConfig := range databaseConfig.Resources {
			table, ok := db.Table(spansql.ID(resourceConfig.Table))
			if !ok {
				log.Panicf("unknown table %s in database %s", resourceConfig.Table, databaseConfig.Name)
			}
			messageDescriptor, err := resourceConfig.LoadMessageDescriptor()
			if err != nil {
				log.Panic(err)
			}
			// TODO: Use table and message descriptor for code generation.
			_, _ = table, messageDescriptor
		}
		if err := os.MkdirAll(databaseConfig.Package.Path, 0o775); err != nil {
			log.Panic(err)
		}
		filename := filepath.Join(databaseConfig.Package.Path, "descriptor_gen.go")
		f := codegen.NewFile(codegen.FileConfig{
			Filename:    filename,
			Package:     databaseConfig.Package.Name,
			GeneratedBy: generatedBy,
		})
		descriptorcodegen.DatabaseDescriptorCodeGenerator{
			Database: db,
		}.GenerateCode(f)
		content, err := f.Content()
		if err != nil {
			log.Panic(err)
		}
		if err := os.WriteFile(filename, content, 0o600); err != nil {
			log.Panic(err)
		}
		log.Println("wrote:", filename)
		if len(db.Tables) > 0 {
			filename := filepath.Join(databaseConfig.Package.Path, "database_gen.go")
			f := codegen.NewFile(codegen.FileConfig{
				Filename:    filename,
				Package:     databaseConfig.Package.Name,
				GeneratedBy: generatedBy,
			})
			databasecodegen.DatabaseCodeGenerator{Database: db}.GenerateCode(f)
			content, err := f.Content()
			if err != nil {
				log.Panic(err)
			}
			if err := os.WriteFile(filename, content, 0o600); err != nil {
				log.Panic(err)
			}
			log.Println("wrote:", filename)
		}
	}
}
