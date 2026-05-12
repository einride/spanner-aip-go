package spanfiltering

import (
	"fmt"
	"strings"
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
			name:   "has: timestamp wildcard",
			filter: `create_time:*`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			expectedSQL: `(create_time IS NOT NULL AND create_time != @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": time.Unix(0, 0).UTC(),
			},
		},

		{
			name:   "has: timestamp exact match",
			filter: `create_time:"2021-02-14T14:49:34+01:00"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			errorContains: "only supports wildcard",
		},

		{
			name:   "has: negated timestamp wildcard",
			filter: `NOT create_time:*`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			expectedSQL: `(NOT (create_time IS NOT NULL AND create_time != @param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": time.Unix(0, 0).UTC(),
			},
		},

		{
			name:   "has: negated timestamp exact match",
			filter: `NOT create_time:"2021-02-14T14:49:34+01:00"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
			},
			errorContains: "only supports wildcard",
		},

		{
			name:   "has: string wildcard",
			filter: `name:*`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("name", filtering.TypeString),
			},
			expectedSQL: `(name IS NOT NULL AND name != @param_0)`,
			expectedParams: map[string]interface{}{
				"param_0": "",
			},
		},

		{
			name:   "has: negated string wildcard",
			filter: `NOT name:*`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("name", filtering.TypeString),
			},
			expectedSQL: `(NOT (name IS NOT NULL AND name != @param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": "",
			},
		},

		{
			name:   "has: string exact match",
			filter: `name:"John"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("name", filtering.TypeString),
			},
			errorContains: "only supports wildcard",
		},

		{
			name:   "searchNgrams: 2-arg basic",
			filter: `searchNgrams(display_name_tokens, "abc")`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name_tokens", filtering.TypeString),
				DeclareSearchNgramsFunction(),
			},
			expectedSQL: `(SEARCH_NGRAMS(display_name_tokens, @param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
			},
		},

		{
			name:   "searchNgrams: 5-arg all set",
			filter: `searchNgrams(display_name_tokens, "abc", "en", 3, 0.8)`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name_tokens", filtering.TypeString),
				DeclareSearchNgramsFunction(),
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
			name:   "searchNgrams: 5-arg skip language_tag",
			filter: `searchNgrams(display_name_tokens, "abc", "", 3, 0.0)`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name_tokens", filtering.TypeString),
				DeclareSearchNgramsFunction(),
			},
			expectedSQL: `(SEARCH_NGRAMS(display_name_tokens, @param_0, min_ngrams => @param_1))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
				"param_1": int64(3),
			},
		},

		{
			name:   "searchNgrams: 5-arg skip all optional",
			filter: `searchNgrams(display_name_tokens, "abc", "", 0, 0.0)`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name_tokens", filtering.TypeString),
				DeclareSearchNgramsFunction(),
			},
			expectedSQL: `(SEARCH_NGRAMS(display_name_tokens, @param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
			},
		},

		{
			name:   "searchNgrams: combined with AND",
			filter: `searchNgrams(display_name_tokens, "abc") AND author = "Karin Boye"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("display_name_tokens", filtering.TypeString),
				filtering.DeclareIdent("author", filtering.TypeString),
				DeclareSearchNgramsFunction(),
			},
			expectedSQL: `((SEARCH_NGRAMS(display_name_tokens, @param_0)) AND (author = @param_1))`,
			expectedParams: map[string]interface{}{
				"param_0": "abc",
				"param_1": "Karin Boye",
			},
		},

		{
			name:   "or-fold: same column two strings",
			filter: `id = "a" OR id = "b"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("id", filtering.TypeString),
			},
			expectedSQL: `(id IN UNNEST(@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": []string{"a", "b"},
			},
		},

		{
			name:   "or-fold: preserves value order",
			filter: `id = "b" OR id = "a"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("id", filtering.TypeString),
			},
			expectedSQL: `(id IN UNNEST(@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": []string{"b", "a"},
			},
		},

		{
			name:   "or-fold: int64",
			filter: `count = 1 OR count = 2 OR count = 3`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("count", filtering.TypeInt),
			},
			expectedSQL: `(count IN UNNEST(@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": []int64{1, 2, 3},
			},
		},

		{
			name:   "or-fold: mixed columns",
			filter: `a = 1 OR b = 2 OR a = 3`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("a", filtering.TypeInt),
				filtering.DeclareIdent("b", filtering.TypeInt),
			},
			expectedSQL: `((a IN UNNEST(@param_0)) OR (b = @param_1))`,
			expectedParams: map[string]interface{}{
				"param_0": []int64{1, 3},
				"param_1": int64(2),
			},
		},

		{
			name:   "or-no-fold: mixed ops same column",
			filter: `id = "a" OR id > "b"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("id", filtering.TypeString),
			},
			expectedSQL: `((id = @param_0) OR (id > @param_1))`,
			expectedParams: map[string]interface{}{
				"param_0": "a",
				"param_1": "b",
			},
		},

		{
			name:   "or-fold: nested in AND",
			filter: `(id = "a" OR id = "b") AND read`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("id", filtering.TypeString),
				filtering.DeclareIdent("read", filtering.TypeBool),
			},
			expectedSQL: `((id IN UNNEST(@param_0)) AND read)`,
			expectedParams: map[string]interface{}{
				"param_0": []string{"a", "b"},
			},
		},

		{
			name:   "or-fold: enum values as int",
			filter: `example_enum = ENUM_ONE OR example_enum = ENUM_TWO`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `(example_enum IN UNNEST(@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": []int64{1, 2},
			},
		},

		{
			name:    "or-fold: enum values as strings",
			options: []TranspileOption{WithEnumValuesAsStrings()},
			filter:  `example_enum = ENUM_ONE OR example_enum = ENUM_TWO`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareEnumIdent("example_enum", syntaxv1.Enum(0).Type()),
			},
			expectedSQL: `(example_enum IN UNNEST(@param_0))`,
			expectedParams: map[string]interface{}{
				"param_0": []string{"ENUM_ONE", "ENUM_TWO"},
			},
		},

		{
			name:   "or-no-fold: substring match in chain",
			filter: `author = "*x*" OR author = "y"`,
			declarations: []filtering.DeclarationOption{
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("author", filtering.TypeString),
			},
			expectedSQL: `((author LIKE @param_0) OR (author = @param_1))`,
			expectedParams: map[string]interface{}{
				"param_0": "%x%",
				"param_1": "y",
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

// TestTranspileFilter_LargeOrChainFoldsToInUnnest exercises the 75-OR
// Spanner planner limit. Without the OR fold, a chain of 80 equality
// disjuncts on the same column would emit 79 nested LogicalOp{Or} nodes
// and Spanner would reject the query with "Number of nested boolean
// predicates exceeds the maximum allowed limit of 75".
func TestTranspileFilter_LargeOrChainFoldsToInUnnest(t *testing.T) {
	t.Parallel()
	const n = 80
	ids := make([]string, n)
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("id-%02d", i)
		parts[i] = fmt.Sprintf(`correlation_id = "id-%02d"`, i)
	}
	filterStr := strings.Join(parts, " OR ")
	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("correlation_id", filtering.TypeString),
	)
	assert.NilError(t, err)
	filter, err := filtering.ParseFilter(&mockRequest{filter: filterStr}, declarations)
	assert.NilError(t, err)
	actual, params, err := TranspileFilter(filter)
	assert.NilError(t, err)
	assert.Equal(t, `(correlation_id IN UNNEST(@param_0))`, actual.SQL())
	assert.Equal(t, 0, strings.Count(actual.SQL(), " OR "))
	assert.DeepEqual(t, map[string]interface{}{"param_0": ids}, params)
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
