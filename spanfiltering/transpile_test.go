package spanfiltering

import (
	"testing"
	"time"

	"go.einride.tech/aip/filtering"
	syntaxv1 "go.einride.tech/aip/proto/gen/einride/example/syntax/v1"
	"gotest.tools/v3/assert"
)

func TestTranspileFilter(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name           string
		filter         string
		declarations   []filtering.DeclarationOption
		expectedSQL    string
		expectedParams map[string]interface{}
		errorContains  string
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
			expectedSQL: `((author = @param_0) AND (NOT read))`,
			expectedParams: map[string]interface{}{
				"param_0": "Karin Boye",
			},
		},

		{
			name:   "string negated equality",
			filter: `author != "Karin Boye"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("author", filtering.TypeString),
			},
			expectedSQL: `(author != @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": "Karin Boye",
			},
		},

		{
			name:   "timestamp",
			filter: `create_time > timestamp("2021-02-14T14:49:34+01:00")`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			expectedSQL: `(create_time > (@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": mustParseTime(t, "2021-02-14T14:49:34+01:00"),
			},
		},

		{
			name:   "enum equality",
			filter: `example_enum = ENUM_ONE`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `(example_enum = @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": int64(1),
			},
		},

		{
			name:   "enum negated equality",
			filter: `example_enum != ENUM_ONE`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `(example_enum != @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": int64(1),
			},
		},

		{
			name:   "has: repeated string",
			filter: `repeated_string:"value"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("repeated_string", filtering.TypeList(filtering.TypeString)),
			},
			expectedSQL: `(@param_0 IN UNNEST(repeated_string))`,
			expectedParams: map[string]interface{}{
				"param_0": "value",
			},
		},

		{
			name:   "empty filter",
			filter: ``,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `TRUE`,
		},

		{
			name:   "substring matching",
			filter: `author = "*Boye*"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("author", filtering.TypeString),
			},
			expectedSQL: `(author LIKE @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": "%Boye%",
			},
		},

		{
			name:   "substring matching with '*'",
			filter: `author = "*Bo*ye*"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("author", filtering.TypeString),
			},
			errorContains: "wildcard only supported in leading or trailing positions",
		},
	} {
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
			actual, params, err := TranspileFilter(filter)
			if err != nil && tt.errorContains != "" {
				assert.ErrorContains(t, err, tt.errorContains)
				return
			}
			assert.NilError(t, err)
			assert.Equal(t, tt.expectedSQL, actual.SQL())
			assert.DeepEqual(t, tt.expectedParams, params)
		})
	}
}

func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	assert.NilError(t, err)
	return tm
}

type mockRequest struct {
	filter string
}

func (m *mockRequest) GetFilter() string {
	return m.filter
}
