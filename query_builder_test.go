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

	if query.Bloom.Expression.ExpressionType != BloomExpressionAnd {
		t.Fatalf("expected root expression type %q, got %q", BloomExpressionAnd, query.Bloom.Expression.ExpressionType)
	}

	if len(query.Bloom.Expression.Children) != 3 {
		t.Fatalf("expected 3 child expressions, got %d", len(query.Bloom.Expression.Children))
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

	if query.Bloom.Expression.ExpressionType != BloomExpressionOr {
		t.Fatalf("expected root expression type %q, got %q", BloomExpressionOr, query.Bloom.Expression.ExpressionType)
	}

	if len(query.Bloom.Expression.Children) != 2 {
		t.Fatalf("expected 2 child expressions, got %d", len(query.Bloom.Expression.Children))
	}

	firstChild := query.Bloom.Expression.Children[0]
	if firstChild.ExpressionType != BloomExpressionAnd {
		t.Fatalf("expected first child expression type %q, got %q", BloomExpressionAnd, firstChild.ExpressionType)
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

	if query.Bloom.Expression.ExpressionType != BloomExpressionAnd {
		t.Fatalf("expected root expression type %q, got %q", BloomExpressionAnd, query.Bloom.Expression.ExpressionType)
	}

	if len(query.Bloom.Expression.Children) != 2 {
		t.Fatalf("expected 2 child expressions, got %d", len(query.Bloom.Expression.Children))
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

	if query.Prefilter.Expression.ExpressionType != PrefilterExpressionAnd {
		t.Fatalf("expected root prefilter expression type %q, got %q", PrefilterExpressionAnd, query.Prefilter.Expression.ExpressionType)
	}

	if len(query.Prefilter.Expression.Children) != 4 {
		t.Fatalf("expected 4 prefilter child expressions, got %d", len(query.Prefilter.Expression.Children))
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

func TestPrefilterPartitionConditionRejectsMissingPartitionMetadata(t *testing.T) {
	query := NewQuery().
		MatchPrefilter(Partition(PartitionNotEquals("auth_partition"))).
		Build()

	missingPartitionMetadata := &DataBlockMetadata{
		PartitionID: "",
	}

	if EvaluateDataBlockMetadata(missingPartitionMetadata, query.Prefilter) {
		t.Fatalf("expected strict partition prefilter to reject missing partition metadata")
	}
}

func TestPrefilterMinMaxConditionRejectsMissingMinMaxMetadata(t *testing.T) {
	query := NewQuery().
		MatchPrefilter(MinMax("timestamp", NumericGreaterThanEqual(1000))).
		Build()

	missingMinMaxMetadata := &DataBlockMetadata{
		PartitionID: "auth_partition",
	}

	if EvaluateDataBlockMetadata(missingMinMaxMetadata, query.Prefilter) {
		t.Fatalf("expected strict minmax prefilter to reject missing minmax metadata")
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

	if query.Regex.Expression.ExpressionType != RegexExpressionAnd {
		t.Fatalf("expected root regex expression type %q, got %q", RegexExpressionAnd, query.Regex.Expression.ExpressionType)
	}

	if len(query.Regex.Expression.Children) != 2 {
		t.Fatalf("expected 2 regex child expressions, got %d", len(query.Regex.Expression.Children))
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

	if query.Regex.Expression.ExpressionType != RegexExpressionOr {
		t.Fatalf("expected root regex expression type %q, got %q", RegexExpressionOr, query.Regex.Expression.ExpressionType)
	}

	if len(query.Regex.Expression.Children) != 2 {
		t.Fatalf("expected 2 regex child expressions, got %d", len(query.Regex.Expression.Children))
	}
}

func TestRegexFieldGuardBloomQueryPreservesBooleanShape(t *testing.T) {
	regexQuery := &RegexQuery{
		Expression: &RegexExpression{
			ExpressionType: RegexExpressionOr,
			Children: []RegexExpression{
				{
					ExpressionType: RegexExpressionCondition,
					Condition:      &RegexCondition{Field: "service", Pattern: "^pay"},
				},
				{
					ExpressionType: RegexExpressionAnd,
					Children: []RegexExpression{
						{
							ExpressionType: RegexExpressionCondition,
							Condition:      &RegexCondition{Field: "level", Pattern: "^error$"},
						},
						{
							ExpressionType: RegexExpressionCondition,
							Condition:      &RegexCondition{Field: "message", Pattern: "timeout"},
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

	if guard.Expression.ExpressionType != BloomExpressionOr {
		t.Fatalf("expected top level OR bloom expression, got %q", guard.Expression.ExpressionType)
	}

	if len(guard.Expression.Children) != 2 {
		t.Fatalf("expected 2 top level children, got %d", len(guard.Expression.Children))
	}

	if guard.Expression.Children[0].Condition == nil || guard.Expression.Children[0].Condition.Type != BloomField || guard.Expression.Children[0].Condition.Field != "service" {
		t.Fatalf("expected first child to be bloom field condition for service")
	}
}
