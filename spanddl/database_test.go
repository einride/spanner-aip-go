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
		errorDdlIndex int
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
			name: "drop missing table",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				  FirstName  STRING(1024),
				  LastName   STRING(1024),
				  SingerInfo BYTES(MAX),
				  BirthDate  DATE,
				) PRIMARY KEY(SingerId);`,

				`DROP TABLE Albums`,
			},
			errorDdlIndex: 1,
			errorContains: "DROP TABLE: table Albums does not exist",
		},

		{
			name: "create table with row deletion policy",
			ddls: []string{
				`CREATE TABLE Singers (
				  CreatedAt TIMESTAMP NOT NULL,
				  SingerId   INT64 NOT NULL,
				  FirstName  STRING(1024),
				  LastName   STRING(1024),
				  SingerInfo BYTES(MAX),
				  BirthDate  DATE,
				) PRIMARY KEY(SingerId)
				, ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 30 DAY));`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "CreatedAt", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true},
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
							{Name: "FirstName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "LastName", Type: spansql.Type{Base: spansql.String, Len: 1024}},
							{Name: "SingerInfo", Type: spansql.Type{Base: spansql.Bytes, Len: spansql.MaxLen}},
							{Name: "BirthDate", Type: spansql.Type{Base: spansql.Date}},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
						RowDeletionPolicy: &spansql.RowDeletionPolicy{
							Column:  "CreatedAt",
							NumDays: 30,
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
			name: "add row deletion policy",
			ddls: []string{
				`CREATE TABLE Singers (
				  CreatedAt TIMESTAMP NOT NULL,
				  SingerId   INT64 NOT NULL,
				) PRIMARY KEY(SingerId);`,

				`ALTER TABLE Singers ADD ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 1 DAY));`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "CreatedAt", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true},
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
						RowDeletionPolicy: &spansql.RowDeletionPolicy{
							Column:  "CreatedAt",
							NumDays: 1,
						},
					},
				},
			},
		},

		{
			name: "replace row deletion policy",
			ddls: []string{
				`CREATE TABLE Singers (
				  CreatedAt TIMESTAMP NOT NULL,
				  ModifiedAt TIMESTAMP NOT NULL,
				  SingerId   INT64 NOT NULL,
				) PRIMARY KEY(SingerId)
				, ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 30 DAY));`,

				`ALTER TABLE Singers REPLACE ROW DELETION POLICY (OLDER_THAN(ModifiedAt, INTERVAL 7 DAY));`,
			},
			expected: &Database{
				Tables: []*Table{
					{
						Name: "Singers",
						Columns: []*Column{
							{Name: "CreatedAt", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true},
							{Name: "ModifiedAt", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true},
							{Name: "SingerId", Type: spansql.Type{Base: spansql.Int64}, NotNull: true},
						},
						PrimaryKey: []spansql.KeyPart{
							{Column: "SingerId"},
						},
						RowDeletionPolicy: &spansql.RowDeletionPolicy{
							Column:  "ModifiedAt",
							NumDays: 7,
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
			name: "drop interleaved",
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
				  INTERLEAVE IN PARENT Singers ON DELETE CASCADE;`,

				`DROP TABLE Albums;`,
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
						InterleavedTables: []*Table{},
					},
				},
			},
		},

		{
			name: "drop table with interleaved tables",
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
				  INTERLEAVE IN PARENT Singers ON DELETE CASCADE;`,

				`CREATE TABLE Genres (
				  SingerId     INT64 NOT NULL,
				  GenreId      INT64 NOT NULL,
				  GenreTitle   STRING(MAX),
				) PRIMARY KEY (SingerId, AlbumId),
				  INTERLEAVE IN PARENT Singers ON DELETE CASCADE;`,

				`DROP TABLE Singers;`,
			},
			errorDdlIndex: 3,
			errorContains: "DROP TABLE: table Singers has interleaved tables Albums, Genres",
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
		{
			name: "drop index",
			ddls: []string{
				`CREATE TABLE Singers (
				  SingerId   INT64 NOT NULL,
				  FirstName  STRING(1024),
				  LastName   STRING(1024),
				  SingerInfo BYTES(MAX),
				  BirthDate  DATE,
				) PRIMARY KEY(SingerId);`,

				`CREATE INDEX SingersByFirstLastName ON Singers(FirstName, LastName)`,

				`DROP INDEX SingersByFirstLastName`,
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

				Indexes: []*Index{},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var db Database
			for i, ddl := range tt.ddls {
				ddl, err := spansql.ParseDDL(tt.name, ddl)
				assert.NilError(t, err)
				err = db.ApplyDDL(ddl)
				if tt.errorDdlIndex == i && tt.errorContains != "" {
					assert.ErrorContains(t, err, tt.errorContains)
					return
				}
				assert.NilError(t, err)
			}
			assert.DeepEqual(t, tt.expected, &db)
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
