package config

import (
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/spanner-aip/spanddl"
)

// DatabaseConfig contains code generation config for a database.
type DatabaseConfig struct {
	// Name of the database.
	Name string `yaml:"name"`
	// SchemaGlobs are read in ass
	SchemaGlobs []string `yaml:"schema"`
	// Package is the config for database's generated Go package.
	Package GoPackageConfig `yaml:"package"`
}

// LoadDatabase loads the configured database.
func (c *DatabaseConfig) LoadDatabase() (*spanddl.Database, error) {
	var db spanddl.Database
	for _, schemaGlob := range c.SchemaGlobs {
		schemaFiles, err := filepath.Glob(schemaGlob)
		if err != nil {
			return nil, fmt.Errorf("load database %s: %w", c.Name, err)
		}
		for _, schemaFile := range schemaFiles {
			schema, err := os.ReadFile(schemaFile)
			if err != nil {
				return nil, fmt.Errorf("load database %s: %w", c.Name, err)
			}
			ddl, err := spansql.ParseDDL(schemaFile, string(schema))
			if err != nil {
				return nil, fmt.Errorf("load database %s: %w", c.Name, err)
			}
			if err := db.ApplyDDL(ddl); err != nil {
				return nil, fmt.Errorf("load database %s: %w", c.Name, err)
			}
		}
	}
	return &db, nil
}

// GoPackageConfig contains code generation config for a Go package.
type GoPackageConfig struct {
	// Name is the package name.
	Name string `yaml:"name"`
	// Path is the package import path.
	Path string `yaml:"path"`
}
