package bloomsearch

import "testing"

func TestNewQueryDefaultsToImplicitAndExpression(t *testing.T) {
	query := NewQuery().
		Field("retry_count").
		Token("error").
		FieldToken("service", "payment").
		Build()

	if query.Bloom.Expression == nil {
		t.Fatalf("expected bloom expression to be set")
	}

	if query.Bloom.Expression.expressionType != bloomExpressionAnd {
		t.Fatalf("expected root expression type %q, got %q", bloomExpressionAnd, query.Bloom.Expression.expressionType)
	}

	if len(query.Bloom.Expression.children) != 3 {
		t.Fatalf("expected 3 child expressions, got %d", len(query.Bloom.Expression.children))
	}
}

func TestMatchSupportsNestedBooleanExpressions(t *testing.T) {
	query := NewQuery().
		Match(
			Or(
				And(
					Field("retry_count"),
					Token("error"),
				),
				FieldToken("service", "payment"),
			),
		).
		Build()

	if query.Bloom.Expression == nil {
		t.Fatalf("expected bloom expression to be set")
	}

	if query.Bloom.Expression.expressionType != bloomExpressionOr {
		t.Fatalf("expected root expression type %q, got %q", bloomExpressionOr, query.Bloom.Expression.expressionType)
	}

	if len(query.Bloom.Expression.children) != 2 {
		t.Fatalf("expected 2 child expressions, got %d", len(query.Bloom.Expression.children))
	}

	firstChild := query.Bloom.Expression.children[0]
	if firstChild.expressionType != bloomExpressionAnd {
		t.Fatalf("expected first child expression type %q, got %q", bloomExpressionAnd, firstChild.expressionType)
	}
}

func TestFieldAfterMatchGetsAndedWithExistingExpression(t *testing.T) {
	query := NewQuery().
		Match(Or(Field("service"), Field("level"))).
		Token("error").
		Build()

	if query.Bloom.Expression == nil {
		t.Fatalf("expected bloom expression to be set")
	}

	if query.Bloom.Expression.expressionType != bloomExpressionAnd {
		t.Fatalf("expected root expression type %q, got %q", bloomExpressionAnd, query.Bloom.Expression.expressionType)
	}

	if len(query.Bloom.Expression.children) != 2 {
		t.Fatalf("expected 2 child expressions, got %d", len(query.Bloom.Expression.children))
	}
}

func TestMatchPrefilterSupportsAndBetweenPartitionAndMinMax(t *testing.T) {
	query := NewQuery().
		MatchPrefilter(
			PrefilterAnd(
				Partition(PartitionEquals("auth_partition")),
				Partition(PartitionIn("api_partition", "financial_partition")),
				MinMax("timestamp", NumericBetween(1000, 2000)),
				MinMax("response_time", NumericLessThan(1000)),
			),
		).
		Build()

	if query.Prefilter.Expression == nil {
		t.Fatalf("expected prefilter expression to be set")
	}

	if query.Prefilter.Expression.expressionType != prefilterExpressionAnd {
		t.Fatalf("expected root prefilter expression type %q, got %q", prefilterExpressionAnd, query.Prefilter.Expression.expressionType)
	}

	if len(query.Prefilter.Expression.children) != 4 {
		t.Fatalf("expected 4 prefilter child expressions, got %d", len(query.Prefilter.Expression.children))
	}
}

func TestMatchPrefilterSupportsOrAndBetweenPartitionAndMinMax(t *testing.T) {
	query := NewQuery().
		MatchPrefilter(
			PrefilterOr(
				Partition(PartitionEquals("auth_partition")),
				MinMax("response_time", NumericGreaterThanEqual(200)),
			),
		).
		Build()

	partitionMetadata := &DataBlockMetadata{
		PartitionID: "auth_partition",
		MinMaxIndexes: map[string]MinMaxIndex{
			"response_time": {Min: 10, Max: 20},
		},
	}
	if !EvaluateDataBlockMetadata(partitionMetadata, query.Prefilter) {
		t.Fatalf("expected partition branch to match prefilter")
	}

	minmaxMetadata := &DataBlockMetadata{
		PartitionID: "financial_partition",
		MinMaxIndexes: map[string]MinMaxIndex{
			"response_time": {Min: 210, Max: 250},
		},
	}
	if !EvaluateDataBlockMetadata(minmaxMetadata, query.Prefilter) {
		t.Fatalf("expected minmax branch to match prefilter")
	}

	noMatchMetadata := &DataBlockMetadata{
		PartitionID: "financial_partition",
		MinMaxIndexes: map[string]MinMaxIndex{
			"response_time": {Min: 10, Max: 20},
		},
	}
	if EvaluateDataBlockMetadata(noMatchMetadata, query.Prefilter) {
		t.Fatalf("expected prefilter to reject non-matching metadata")
	}
}

func TestNewQuerySupportsImplicitRegexAndExpression(t *testing.T) {
	query := NewQuery().
		FieldRegex("service", "^pay").
		FieldRegex("message", "timeout$").
		Build()

	if query.Regex.Expression == nil {
		t.Fatalf("expected regex expression to be set")
	}

	if query.Regex.Expression.expressionType != regexExpressionAnd {
		t.Fatalf("expected root regex expression type %q, got %q", regexExpressionAnd, query.Regex.Expression.expressionType)
	}

	if len(query.Regex.Expression.children) != 2 {
		t.Fatalf("expected 2 regex child expressions, got %d", len(query.Regex.Expression.children))
	}
}

func TestMatchRegexSupportsNestedBooleanExpressions(t *testing.T) {
	query := NewQuery().
		MatchRegex(
			RegexOr(
				RegexAnd(
					FieldRegex("service", "^auth$"),
					FieldRegex("message", "failed"),
				),
				FieldRegex("level", "^error$"),
			),
		).
		Build()

	if query.Regex.Expression == nil {
		t.Fatalf("expected regex expression to be set")
	}

	if query.Regex.Expression.expressionType != regexExpressionOr {
		t.Fatalf("expected root regex expression type %q, got %q", regexExpressionOr, query.Regex.Expression.expressionType)
	}

	if len(query.Regex.Expression.children) != 2 {
		t.Fatalf("expected 2 regex child expressions, got %d", len(query.Regex.Expression.children))
	}
}

func TestRegexFieldGuardBloomQueryPreservesBooleanShape(t *testing.T) {
	regexQuery := &RegexQuery{
		Expression: &RegexExpression{
			expressionType: regexExpressionOr,
			children: []RegexExpression{
				{
					expressionType: regexExpressionCondition,
					condition:      &RegexCondition{Field: "service", Pattern: "^pay"},
				},
				{
					expressionType: regexExpressionAnd,
					children: []RegexExpression{
						{
							expressionType: regexExpressionCondition,
							condition:      &RegexCondition{Field: "level", Pattern: "^error$"},
						},
						{
							expressionType: regexExpressionCondition,
							condition:      &RegexCondition{Field: "message", Pattern: "timeout"},
						},
					},
				},
			},
		},
	}

	guard := RegexFieldGuardBloomQuery(regexQuery)
	if guard == nil || guard.Expression == nil {
		t.Fatalf("expected regex field guard bloom expression")
	}

	if guard.Expression.expressionType != bloomExpressionOr {
		t.Fatalf("expected top level OR bloom expression, got %q", guard.Expression.expressionType)
	}

	if len(guard.Expression.children) != 2 {
		t.Fatalf("expected 2 top level children, got %d", len(guard.Expression.children))
	}

	if guard.Expression.children[0].condition == nil || guard.Expression.children[0].condition.Type != BloomField || guard.Expression.children[0].condition.Field != "service" {
		t.Fatalf("expected first child to be bloom field condition for service")
	}
}
