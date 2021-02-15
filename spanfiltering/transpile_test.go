package spanfiltering

import (
	"testing"

	syntaxv1 "go.einride.tech/aip/examples/proto/gen/einride/example/syntax/v1"
	"go.einride.tech/aip/filtering"
	"gotest.tools/v3/assert"
)

func TestTranspileFilter(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		filter        string
		declarations  []filtering.DeclarationOption
		expectedSQL   string
		errorContains string
	}{
		{
			name:   "simple flag",
			filter: "read",
			declarations: []filtering.DeclarationOption{
				filtering.DeclareIdent("read", filtering.TypeBool),
			},
			expectedSQL: "read",
		},

		{
			name:   "negated simple flag",
			filter: "NOT read",
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("read", filtering.TypeBool),
			},
			expectedSQL: "(NOT read)",
		},

		{
			name:   "string equality and flag",
			filter: `author = "Karin Boye" AND NOT read`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("author", filtering.TypeString),
				filtering.DeclareIdent("read", filtering.TypeBool),
			},
			expectedSQL: `((author = "Karin Boye") AND (NOT read))`,
		},

		{
			name:   "string equality and flag",
			filter: `create_time > timestamp("2021-02-14T14:49:34+01:00")`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			expectedSQL: `(create_time > (TIMESTAMP '2021-02-14 14:49:34.000000 +01:00'))`,
		},

		{
			name:   "enum equality",
			filter: `example_enum = ENUM_ONE`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `(example_enum = 1)`,
		},

		{
			name:   "empty filter",
			filter: ``,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `TRUE`,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			declarations, err := filtering.NewDeclarations(tt.declarations...)
			assert.NilError(t, err)
			filter, err := filtering.ParseFilter(&mockRequest{filter: tt.filter}, declarations)
			if err != nil && tt.errorContains != "" {
				assert.ErrorContains(t, err, tt.errorContains)
				return
			}
			assert.NilError(t, err)
			actual, err := TranspileFilter(filter)
			if err != nil && tt.errorContains != "" {
				assert.ErrorContains(t, err, tt.errorContains)
				return
			}
			assert.NilError(t, err)
			assert.Equal(t, tt.expectedSQL, actual.SQL())
		})
	}
}

type mockRequest struct {
	filter string
}

func (m *mockRequest) GetFilter() string {
	return m.filter
}
