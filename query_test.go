package bloomsearch

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// TestQueryExpressionJSONRoundTrip is the regression for expression trees
// serializing to empty objects: a Query must survive a JSON round trip with
// identical semantics, so a remote MetaStore receives real conditions rather
// than an always-true prefilter.
func TestQueryExpressionJSONRoundTrip(t *testing.T) {
	prefilterExpression := PrefilterOr(
		PrefilterAnd(
			Partition(PartitionIn("tenant-a", "tenant-b")),
			MinMax("timestamp", NumericBetween(1000, 2000)),
		),
		MinMax("severity", NumericGreaterThanEqual(4)),
	)
	bloomExpression := And(
		Or(Field("user.name"), FieldToken("service", "payment")),
		Token("timeout"),
	)
	regexExpression := RegexOr(
		RegexAnd(FieldRegex("message", "timeout|retry"), FieldRegex("level", "^err")),
		FieldRegex("service", "^pay"),
	)

	original := &Query{
		Prefilter: &QueryPrefilter{Expression: &prefilterExpression},
		Bloom:     &BloomQuery{Expression: &bloomExpression},
		Regex:     &RegexQuery{Expression: &regexExpression},
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal query: %v", err)
	}
	for _, fragment := range []string{"tenant-a", "PARTITION", "MINMAX", "user.name", "payment", "timeout|retry"} {
		if !strings.Contains(string(encoded), fragment) {
			t.Fatalf("marshaled query is missing %q — conditions were erased: %s", fragment, encoded)
		}
	}

	decoded := &Query{}
	if err := json.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("failed to unmarshal query: %v", err)
	}

	// Structural equality: re-marshaling the decoded query reproduces the
	// original bytes.
	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("failed to re-marshal query: %v", err)
	}
	if string(encoded) != string(reencoded) {
		t.Fatalf("re-marshaled query differs:\noriginal: %s\ndecoded:  %s", encoded, reencoded)
	}

	// Prefilter semantics: original and decoded agree with the expected
	// verdict on metadata exercising every branch.
	prefilterCases := []struct {
		name     string
		metadata DataBlockMetadata
		want     bool
	}{
		{"partition and timestamp match", DataBlockMetadata{PartitionID: "tenant-a", MinMaxIndexes: map[string]MinMaxIndex{"timestamp": {Min: 1500, Max: 1600}}}, true},
		{"timestamp range touches lower bound", DataBlockMetadata{PartitionID: "tenant-b", MinMaxIndexes: map[string]MinMaxIndex{"timestamp": {Min: 500, Max: 1000}}}, true},
		{"severity branch matches", DataBlockMetadata{PartitionID: "tenant-c", MinMaxIndexes: map[string]MinMaxIndex{"severity": {Min: 5, Max: 9}}}, true},
		{"severity too low", DataBlockMetadata{PartitionID: "tenant-c", MinMaxIndexes: map[string]MinMaxIndex{"severity": {Min: 1, Max: 3}}}, false},
		{"timestamp out of range", DataBlockMetadata{PartitionID: "tenant-a", MinMaxIndexes: map[string]MinMaxIndex{"timestamp": {Min: 5000, Max: 6000}}}, false},
		{"missing metadata", DataBlockMetadata{}, false},
	}
	for _, c := range prefilterCases {
		originalGot := EvaluateDataBlockMetadata(&c.metadata, original.Prefilter)
		decodedGot := EvaluateDataBlockMetadata(&c.metadata, decoded.Prefilter)
		if originalGot != c.want {
			t.Fatalf("prefilter %q: original evaluated to %v, want %v", c.name, originalGot, c.want)
		}
		if decodedGot != c.want {
			t.Fatalf("prefilter %q: decoded evaluated to %v, want %v", c.name, decodedGot, c.want)
		}
	}

	// Bloom semantics over sample rows.
	bloomCases := []struct {
		row  string
		want bool
	}{
		{`{"user":{"name":"dan"},"message":"connection timeout"}`, true},
		{`{"service":"payment","message":"a timeout happened"}`, true},
		{`{"service":"payment","message":"all good"}`, false},
		{`{"other":"x","message":"timeout"}`, false},
	}
	for _, c := range bloomCases {
		originalGot := testJSONForBloomQuery([]byte(c.row), original.Bloom, ".", BasicWhitespaceLowerTokenizer)
		decodedGot := testJSONForBloomQuery([]byte(c.row), decoded.Bloom, ".", BasicWhitespaceLowerTokenizer)
		if originalGot != c.want {
			t.Fatalf("bloom %s: original evaluated to %v, want %v", c.row, originalGot, c.want)
		}
		if decodedGot != c.want {
			t.Fatalf("bloom %s: decoded evaluated to %v, want %v", c.row, decodedGot, c.want)
		}
	}

	// Regex semantics over sample rows.
	originalRegex, err := compileRegexQuery(original.Regex)
	if err != nil {
		t.Fatalf("failed to compile original regex query: %v", err)
	}
	decodedRegex, err := compileRegexQuery(decoded.Regex)
	if err != nil {
		t.Fatalf("failed to compile decoded regex query: %v", err)
	}
	regexCases := []struct {
		row  string
		want bool
	}{
		{`{"message":"retry now","level":"error"}`, true},
		{`{"service":"payments"}`, true},
		{`{"message":"retry now","level":"info"}`, false},
	}
	for _, c := range regexCases {
		row := gjson.Parse(c.row)
		originalGot := testGJSONForQuery(row, nil, originalRegex, ".", BasicWhitespaceLowerTokenizer)
		decodedGot := testGJSONForQuery(row, nil, decodedRegex, ".", BasicWhitespaceLowerTokenizer)
		if originalGot != c.want {
			t.Fatalf("regex %s: original evaluated to %v, want %v", c.row, originalGot, c.want)
		}
		if decodedGot != c.want {
			t.Fatalf("regex %s: decoded evaluated to %v, want %v", c.row, decodedGot, c.want)
		}
	}
}

// prefilterExpressionToSQL renders a PrefilterExpression as a SQL WHERE
// clause using only the exported tree, the way a SQL-backed MetaStore would.
// It assumes a data_blocks table with partition_id, min_<field>, and
// max_<field> columns, and covers the operators this test exercises.
func prefilterExpressionToSQL(expression *PrefilterExpression) (string, error) {
	switch expression.ExpressionType {
	case PrefilterExpressionCondition:
		condition := expression.Condition
		switch condition.ConditionType {
		case PrefilterConditionPartition:
			partition := condition.PartitionCondition
			switch partition.Operator {
			case OpEqual:
				return fmt.Sprintf("partition_id = '%s'", partition.Value), nil
			case OpIn:
				quoted := make([]string, len(partition.Values))
				for i, value := range partition.Values {
					quoted[i] = "'" + value + "'"
				}
				return fmt.Sprintf("partition_id IN (%s)", strings.Join(quoted, ", ")), nil
			}
			return "", fmt.Errorf("unsupported partition operator %q", partition.Operator)
		case PrefilterConditionMinMax:
			minMax := condition.MinMaxCondition
			field := condition.MinMaxFieldName
			switch minMax.Operator {
			case OpBetween:
				return fmt.Sprintf("(min_%s <= %d AND max_%s >= %d)", field, minMax.Max, field, minMax.Min), nil
			case OpGreaterThanEqual:
				return fmt.Sprintf("max_%s >= %d", field, minMax.Value), nil
			}
			return "", fmt.Errorf("unsupported minmax operator %q", minMax.Operator)
		}
		return "", fmt.Errorf("unsupported condition type %q", condition.ConditionType)
	case PrefilterExpressionAnd, PrefilterExpressionOr:
		combinator := " AND "
		if expression.ExpressionType == PrefilterExpressionOr {
			combinator = " OR "
		}
		clauses := make([]string, len(expression.Children))
		for i := range expression.Children {
			clause, err := prefilterExpressionToSQL(&expression.Children[i])
			if err != nil {
				return "", err
			}
			clauses[i] = clause
		}
		return "(" + strings.Join(clauses, combinator) + ")", nil
	}
	return "", fmt.Errorf("unsupported expression type %q", expression.ExpressionType)
}

// TestPrefilterExpressionSQLTranslation shows a third-party MetaStore
// translating an exported PrefilterExpression tree into SQL.
func TestPrefilterExpressionSQLTranslation(t *testing.T) {
	expression := PrefilterAnd(
		Partition(PartitionIn("tenant-a", "tenant-b")),
		PrefilterOr(
			MinMax("timestamp", NumericBetween(1000, 2000)),
			MinMax("severity", NumericGreaterThanEqual(4)),
		),
	)

	sql, err := prefilterExpressionToSQL(&expression)
	if err != nil {
		t.Fatalf("failed to translate expression: %v", err)
	}

	want := "(partition_id IN ('tenant-a', 'tenant-b') AND ((min_timestamp <= 2000 AND max_timestamp >= 1000) OR max_severity >= 4))"
	if sql != want {
		t.Fatalf("unexpected SQL:\ngot:  %s\nwant: %s", sql, want)
	}
}
