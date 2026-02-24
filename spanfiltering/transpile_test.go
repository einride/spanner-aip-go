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
		options        []TranspileOption
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
			name:    "enum equality as strings",
			options: []TranspileOption{WithEnumValuesAsStrings()},
			filter:  `example_enum = ENUM_ONE`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `(example_enum = @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": "ENUM_ONE",
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

		{
			name:    "searchNgrams: 2-arg basic with custom name",
			filter:  `fuzzySearch(display_name, "abc")`,
			options: []TranspileOption{WithSearchNgrams("fuzzySearch")},
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name", filtering.TypeString),
				DeclareSearchNgramsFunction("fuzzySearch"),
			},
			expectedSQL: `(SEARCH_NGRAMS(display_name_tokens, @param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
			},
		},

		{
			name:    "searchNgrams: 5-arg all set",
			filter:  `fuzzySearch(display_name, "abc", "en", 3, 0.8)`,
			options: []TranspileOption{WithSearchNgrams("fuzzySearch")},
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name", filtering.TypeString),
				DeclareSearchNgramsFunction("fuzzySearch"),
			},
			expectedSQL: `(SEARCH_NGRAMS(display_name_tokens, @param_0, ` +
				`language_tag => @param_1, min_ngrams => @param_2, ` +
				`min_ngrams_percent => @param_3))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
				"param_1": "en",
				"param_2": int64(3),
				"param_3": float64(0.8),
			},
		},

		{
			name:    "searchNgrams: 5-arg skip all optional",
			filter:  `fuzzySearch(display_name, "abc", "", 0, 0.0)`,
			options: []TranspileOption{WithSearchNgrams("fuzzySearch")},
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name", filtering.TypeString),
				DeclareSearchNgramsFunction("fuzzySearch"),
			},
			expectedSQL: `(SEARCH_NGRAMS(display_name_tokens, @param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
			},
		},

		{
			name:    "searchNgrams: too-short search string",
			filter:  `fuzzySearch(display_name, "a")`,
			options: []TranspileOption{WithSearchNgrams("fuzzySearch")},
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name", filtering.TypeString),
				DeclareSearchNgramsFunction("fuzzySearch"),
			},
			errorContains: "must be at least 2 characters",
		},

		{
			name:    "searchNgrams: combined with AND",
			filter:  `fuzzySearch(display_name, "abc") AND author = "Karin Boye"`,
			options: []TranspileOption{WithSearchNgrams("fuzzySearch")},
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name", filtering.TypeString),
				filtering.DeclareIdent("author", filtering.TypeString),
				DeclareSearchNgramsFunction("fuzzySearch"),
			},
			expectedSQL: `((SEARCH_NGRAMS(display_name_tokens, @param_0)) AND (author = @param_1))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
				"param_1": "Karin Boye",
			},
		},

		{
			name:   "searchNgrams: without option returns error",
			filter: `fuzzySearch(display_name, "abc")`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name", filtering.TypeString),
				DeclareSearchNgramsFunction("fuzzySearch"),
			},
			errorContains: "unsupported function call",
		},

		{
			name:    "searchNgrams: standard functions not overridable",
			filter:  `create_time > timestamp("2021-02-14T14:49:34+01:00")`,
			options: []TranspileOption{WithSearchNgrams("timestamp")},
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			expectedSQL: `(create_time > (@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": mustParseTime(t, "2021-02-14T14:49:34+01:00"),
			},
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
			actual, params, err := TranspileFilter(filter, tt.options...)
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
