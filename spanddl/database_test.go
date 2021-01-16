package spanddl

import (
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"gotest.tools/v3/assert"
)

func TestDatabase_ApplyDDL(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		ddls          []string
		expected      *Database
		errorContains string
	}{
		{
			name: "create table",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				  FirstName  STRING(1024),
				  LastName   STRING(1024),
				  SingerInfo BYTES(MAX),
				  BirthDate  DATE,
				) PRIMARY KEY(SingerId);`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "FirstName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "LastName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "SingerInfo", Type: spansql.Type{Base: spansql.Bytes, Len: spansql.MaxLen}},
							{Name: "BirthDate", Type: spansql.Type{Base: spansql.Date}},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
					},
				},
			},
		},

		{
			name: "add column",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				) PRIMARY KEY(SingerId);`,

				`ALTER TABLE Singers ADD COLUMN BirthDate DATE`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "BirthDate", Type: spansql.Type{Base: spansql.Date}},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
					},
				},
			},
		},

		{
			name: "set column options",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				  Timestamp TIMESTAMP,
				) PRIMARY KEY(SingerId);`,

				`ALTER TABLE Singers ALTER COLUMN Timestamp SET OPTIONS (allow_commit_timestamp=true)`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{
								Name: "Timestamp",
								Type: spansql.Type{Base: spansql.Timestamp},
								Options: spansql.ColumnOptions{
									AllowCommitTimestamp: boolPtr(true),
								},
							},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
					},
				},
			},
		},

		{
			name: "create interleaved",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				  FirstName  STRING(1024),
				  LastName   STRING(1024),
				  SingerInfo BYTES(MAX),
				  BirthDate  DATE,
				) PRIMARY KEY(SingerId);`,

				`CREATE TABLE Albums (
				  SingerId     INT64 NOT NULL,
				  AlbumId      INT64 NOT NULL,
				  AlbumTitle   STRING(MAX),
				) PRIMARY KEY (SingerId, AlbumId),
				  INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "FirstName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "LastName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "SingerInfo", Type: spansql.Type{Base: spansql.Bytes, Len: spansql.MaxLen}},
							{Name: "BirthDate", Type: spansql.Type{Base: spansql.Date}},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
						InterleavedTables: []*Table{
							{
								Name: "Albums",
								Columns: []*Column{
									{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
									{Name: "AlbumId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
									{Name: "AlbumTitle", Type: spansql.Type{Base: spansql.String, Len: spansql.MaxLen}},
								},
								PrimaryKey: []spansql.KeyPart{
									{Column: "SingerId"},
									{Column: "AlbumId"},
								},
								Interleave: &spansql.Interleave{
									Parent:   "Singers",
									OnDelete: spansql.CascadeOnDelete,
								},
							},
						},
					},

					{
						Name: "Albums",
						Columns: []*Column{
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "AlbumId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "AlbumTitle", Type: spansql.Type{Base: spansql.String, Len: spansql.MaxLen}},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
							{Column: "AlbumId"},
						},
						Interleave: &spansql.Interleave{
							Parent:   "Singers",
							OnDelete: spansql.CascadeOnDelete,
						},
					},
				},
			},
		},

		{
			name: "create index",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				  FirstName  STRING(1024),
				  LastName   STRING(1024),
				  SingerInfo BYTES(MAX),
				  BirthDate  DATE,
				) PRIMARY KEY(SingerId);`,

				`CREATE INDEX SingersByFirstLastName ON Singers(FirstName, LastName)`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "FirstName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "LastName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "SingerInfo", Type: spansql.Type{Base: spansql.Bytes, Len: spansql.MaxLen}},
							{Name: "BirthDate", Type: spansql.Type{Base: spansql.Date}},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
					},
				},

				Indexes: []*Index{
					{
						Name:  "SingersByFirstLastName",
						Table: "Singers",
						Columns: []spansql.KeyPart{
							{Column: "FirstName"},
							{Column: "LastName"},
						},
					},
				},
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var db Database
			for _, ddl := range tt.ddls {
				ddl, err := spansql.ParseDDL(tt.name, ddl)
				assert.NilError(t, err)
				err = db.ApplyDDL(ddl)
				if tt.errorContains != "" {
					assert.ErrorContains(t, err, tt.errorContains)
					break
				} else {
					assert.NilError(t, err)
				}
			}
			assert.DeepEqual(t, tt.expected, &db)
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
