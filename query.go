package bloomsearch

// QueryOperator represents the type of comparison operation
type QueryOperator string

const (
	// Equality operators
	OpEqual    QueryOperator = "EQ"
	OpNotEqual QueryOperator = "NE"

	// Comparison operators
	OpGreaterThan      QueryOperator = "GT"
	OpGreaterThanEqual QueryOperator = "GTE"
	OpLessThan         QueryOperator = "LT"
	OpLessThanEqual    QueryOperator = "LTE"

	// Set operators
	OpIn    QueryOperator = "IN"
	OpNotIn QueryOperator = "NOT_IN"

	// Range operators
	OpBetween    QueryOperator = "BETWEEN"
	OpNotBetween QueryOperator = "NOT_BETWEEN"
)

// StringCondition represents a condition on string values (like partition IDs)
type StringCondition struct {
	Operator QueryOperator `json:",omitempty"` // for EQ, NE, GT, GTE, LT, LTE
	Value    string        `json:",omitempty"` // for EQ, NE, GT, GTE, LT, LTE
	Values   []string      `json:",omitempty"` // for IN, NOT_IN
	Min      string        `json:",omitempty"` // for BETWEEN, NOT_BETWEEN
	Max      string        `json:",omitempty"` // for BETWEEN, NOT_BETWEEN
}

// NumericCondition represents a condition on numeric values (like MinMaxIndex)
type NumericCondition struct {
	Operator QueryOperator `json:",omitempty"` // for EQ, NE, GT, GTE, LT, LTE
	Value    int64         `json:",omitempty"` // for EQ, NE, GT, GTE, LT, LTE
	Values   []int64       `json:",omitempty"` // for IN, NOT_IN
	Min      int64         `json:",omitempty"` // for BETWEEN, NOT_BETWEEN
	Max      int64         `json:",omitempty"` // for BETWEEN, NOT_BETWEEN
}

// Combinator specifies how conditions should be combined
type Combinator string

const (
	CombinatorAND Combinator = "AND"
	CombinatorOR  Combinator = "OR"
)

type prefilterConditionType string

const (
	prefilterConditionPartition prefilterConditionType = "PARTITION"
	prefilterConditionMinMax    prefilterConditionType = "MINMAX"
)

type PrefilterCondition struct {
	conditionType      prefilterConditionType
	partitionCondition *StringCondition
	minMaxFieldName    string
	minMaxCondition    *NumericCondition
}

type prefilterExpressionType string

const (
	prefilterExpressionCondition prefilterExpressionType = "CONDITION"
	prefilterExpressionAnd       prefilterExpressionType = "AND"
	prefilterExpressionOr        prefilterExpressionType = "OR"
)

type PrefilterExpression struct {
	expressionType prefilterExpressionType
	condition      *PrefilterCondition
	children       []PrefilterExpression
}

func Partition(condition StringCondition) PrefilterExpression {
	return PrefilterExpression{
		expressionType: prefilterExpressionCondition,
		condition: &PrefilterCondition{
			conditionType:      prefilterConditionPartition,
			partitionCondition: &condition,
		},
	}
}

func MinMax(fieldName string, condition NumericCondition) PrefilterExpression {
	return PrefilterExpression{
		expressionType: prefilterExpressionCondition,
		condition: &PrefilterCondition{
			conditionType:   prefilterConditionMinMax,
			minMaxFieldName: fieldName,
			minMaxCondition: &condition,
		},
	}
}

func PrefilterAnd(expressions ...PrefilterExpression) PrefilterExpression {
	return PrefilterExpression{
		expressionType: prefilterExpressionAnd,
		children:       flattenPrefilterExpressions(expressions, prefilterExpressionAnd),
	}
}

func PrefilterOr(expressions ...PrefilterExpression) PrefilterExpression {
	return PrefilterExpression{
		expressionType: prefilterExpressionOr,
		children:       flattenPrefilterExpressions(expressions, prefilterExpressionOr),
	}
}

func flattenPrefilterExpressions(expressions []PrefilterExpression, expressionType prefilterExpressionType) []PrefilterExpression {
	flattened := make([]PrefilterExpression, 0, len(expressions))
	for _, expression := range expressions {
		if expression.expressionType == expressionType && expression.condition == nil {
			flattened = append(flattened, expression.children...)
			continue
		}
		flattened = append(flattened, expression)
	}
	return flattened
}

// QueryPrefilter represents prefilter conditions against partition IDs and minmax indexes.
// Expressions can be combined arbitrarily with AND/OR using PrefilterAnd and PrefilterOr.
type QueryPrefilter struct {
	Expression *PrefilterExpression `json:",omitempty"`
}

func NewQueryPrefilter() *QueryPrefilter {
	return &QueryPrefilter{}
}

// Helper functions for creating common conditions

// PartitionEquals creates a partition equality condition
func PartitionEquals(value string) StringCondition {
	return StringCondition{Operator: OpEqual, Value: value}
}

// PartitionIn creates a partition IN condition
func PartitionIn(values ...string) StringCondition {
	return StringCondition{Operator: OpIn, Values: values}
}

// PartitionNotEquals creates a partition not equal condition
func PartitionNotEquals(value string) StringCondition {
	return StringCondition{Operator: OpNotEqual, Value: value}
}

// PartitionNotIn creates a partition NOT IN condition
func PartitionNotIn(values ...string) StringCondition {
	return StringCondition{Operator: OpNotIn, Values: values}
}

// PartitionGreaterThan creates a partition greater than condition
func PartitionGreaterThan(value string) StringCondition {
	return StringCondition{Operator: OpGreaterThan, Value: value}
}

// PartitionGreaterThanEqual creates a partition greater than or equal condition
func PartitionGreaterThanEqual(value string) StringCondition {
	return StringCondition{Operator: OpGreaterThanEqual, Value: value}
}

// PartitionLessThan creates a partition less than condition
func PartitionLessThan(value string) StringCondition {
	return StringCondition{Operator: OpLessThan, Value: value}
}

// PartitionLessThanEqual creates a partition less than or equal condition
func PartitionLessThanEqual(value string) StringCondition {
	return StringCondition{Operator: OpLessThanEqual, Value: value}
}

// PartitionBetween creates a partition BETWEEN condition (inclusive)
func PartitionBetween(min, max string) StringCondition {
	return StringCondition{Operator: OpBetween, Min: min, Max: max}
}

// PartitionNotBetween creates a partition NOT BETWEEN condition (exclusive)
func PartitionNotBetween(min, max string) StringCondition {
	return StringCondition{Operator: OpNotBetween, Min: min, Max: max}
}

// NumericEquals creates a numeric equality condition
func NumericEquals(value int64) NumericCondition {
	return NumericCondition{Operator: OpEqual, Value: value}
}

// NumericNotEquals creates a numeric not equal condition
func NumericNotEquals(value int64) NumericCondition {
	return NumericCondition{Operator: OpNotEqual, Value: value}
}

// NumericGreaterThan creates a numeric greater than condition
func NumericGreaterThan(value int64) NumericCondition {
	return NumericCondition{Operator: OpGreaterThan, Value: value}
}

// NumericGreaterThanEqual creates a numeric greater than or equal condition
func NumericGreaterThanEqual(value int64) NumericCondition {
	return NumericCondition{Operator: OpGreaterThanEqual, Value: value}
}

// NumericLessThan creates a numeric less than condition
func NumericLessThan(value int64) NumericCondition {
	return NumericCondition{Operator: OpLessThan, Value: value}
}

// NumericLessThanEqual creates a numeric less than or equal condition
func NumericLessThanEqual(value int64) NumericCondition {
	return NumericCondition{Operator: OpLessThanEqual, Value: value}
}

// NumericIn creates a numeric IN condition
func NumericIn(values ...int64) NumericCondition {
	return NumericCondition{Operator: OpIn, Values: values}
}

// NumericNotIn creates a numeric NOT IN condition
func NumericNotIn(values ...int64) NumericCondition {
	return NumericCondition{Operator: OpNotIn, Values: values}
}

// NumericBetween creates a numeric BETWEEN condition (inclusive)
func NumericBetween(min, max int64) NumericCondition {
	return NumericCondition{Operator: OpBetween, Min: min, Max: max}
}

// NumericNotBetween creates a numeric NOT BETWEEN condition (exclusive)
func NumericNotBetween(min, max int64) NumericCondition {
	return NumericCondition{Operator: OpNotBetween, Min: min, Max: max}
}

// Evaluation functions for checking if data blocks match query conditions

// EvaluateStringCondition checks if a string value matches the given condition
func EvaluateStringCondition(value string, condition StringCondition) bool {
	switch condition.Operator {
	case OpEqual:
		return value == condition.Value
	case OpNotEqual:
		return value != condition.Value
	case OpGreaterThan:
		return value > condition.Value
	case OpGreaterThanEqual:
		return value >= condition.Value
	case OpLessThan:
		return value < condition.Value
	case OpLessThanEqual:
		return value <= condition.Value
	case OpIn:
		for _, v := range condition.Values {
			if value == v {
				return true
			}
		}
		return false
	case OpNotIn:
		for _, v := range condition.Values {
			if value == v {
				return false
			}
		}
		return true
	case OpBetween:
		return value >= condition.Min && value <= condition.Max
	case OpNotBetween:
		return value < condition.Min || value > condition.Max
	default:
		return false
	}
}

// EvaluateNumericCondition checks if a numeric value matches the given condition
func EvaluateNumericCondition(value int64, condition NumericCondition) bool {
	switch condition.Operator {
	case OpEqual:
		return value == condition.Value
	case OpNotEqual:
		return value != condition.Value
	case OpGreaterThan:
		return value > condition.Value
	case OpGreaterThanEqual:
		return value >= condition.Value
	case OpLessThan:
		return value < condition.Value
	case OpLessThanEqual:
		return value <= condition.Value
	case OpIn:
		for _, v := range condition.Values {
			if value == v {
				return true
			}
		}
		return false
	case OpNotIn:
		for _, v := range condition.Values {
			if value == v {
				return false
			}
		}
		return true
	case OpBetween:
		return value >= condition.Min && value <= condition.Max
	case OpNotBetween:
		return value < condition.Min || value > condition.Max
	default:
		return false
	}
}

// EvaluateMinMaxCondition checks if a MinMaxIndex overlaps with the given condition
// This is used for range-based filtering where we want to include data blocks that might contain matching values
func EvaluateMinMaxCondition(minMaxIndex MinMaxIndex, condition NumericCondition) bool {
	switch condition.Operator {
	case OpEqual:
		// The range contains the target value
		return minMaxIndex.Min <= condition.Value && condition.Value <= minMaxIndex.Max
	case OpNotEqual:
		// The range might contain values other than the target value
		return minMaxIndex.Min != condition.Value || minMaxIndex.Max != condition.Value
	case OpGreaterThan:
		// The range has values greater than the target
		return minMaxIndex.Max > condition.Value
	case OpGreaterThanEqual:
		// The range has values greater than or equal to the target
		return minMaxIndex.Max >= condition.Value
	case OpLessThan:
		// The range has values less than the target
		return minMaxIndex.Min < condition.Value
	case OpLessThanEqual:
		// The range has values less than or equal to the target
		return minMaxIndex.Min <= condition.Value
	case OpIn:
		// The range might contain any of the target values
		for _, v := range condition.Values {
			if minMaxIndex.Min <= v && v <= minMaxIndex.Max {
				return true
			}
		}
		return false
	case OpNotIn:
		// The range might contain values not in the target set
		// This is complex - we include the block if it's not entirely contained within the NOT_IN set
		return true // Conservative approach - let bloom filter handle the detailed filtering
	case OpBetween:
		// The ranges overlap
		return minMaxIndex.Min <= condition.Max && condition.Min <= minMaxIndex.Max
	case OpNotBetween:
		// The range might contain values outside the target range
		return minMaxIndex.Min < condition.Min || minMaxIndex.Max > condition.Max
	default:
		return false
	}
}

// EvaluateDataBlockMetadata checks if a DataBlockMetadata matches the prefilter expression.
func EvaluateDataBlockMetadata(metadata *DataBlockMetadata, query *QueryPrefilter) bool {
	if query == nil || query.Expression == nil {
		return true
	}
	return evaluatePrefilterExpression(metadata, query.Expression)
}

func evaluatePrefilterExpression(metadata *DataBlockMetadata, expression *PrefilterExpression) bool {
	if expression == nil {
		return true
	}

	switch expression.expressionType {
	case prefilterExpressionCondition:
		if expression.condition == nil {
			return true
		}
		return evaluatePrefilterCondition(metadata, expression.condition)
	case prefilterExpressionOr:
		if len(expression.children) == 0 {
			return false
		}
		for i := range expression.children {
			if evaluatePrefilterExpression(metadata, &expression.children[i]) {
				return true
			}
		}
		return false
	case prefilterExpressionAnd:
		for i := range expression.children {
			if !evaluatePrefilterExpression(metadata, &expression.children[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func evaluatePrefilterCondition(metadata *DataBlockMetadata, condition *PrefilterCondition) bool {
	switch condition.conditionType {
	case prefilterConditionPartition:
		if condition.partitionCondition == nil {
			return true
		}
		return EvaluateStringCondition(metadata.PartitionID, *condition.partitionCondition)
	case prefilterConditionMinMax:
		if condition.minMaxCondition == nil {
			return true
		}
		minMaxIndex, exists := metadata.MinMaxIndexes[condition.minMaxFieldName]
		if !exists {
			return false
		}
		return EvaluateMinMaxCondition(minMaxIndex, *condition.minMaxCondition)
	default:
		return false
	}
}

// FilterDataBlocks filters a slice of DataBlockMetadata based on query conditions
func FilterDataBlocks(blocks []DataBlockMetadata, query *QueryPrefilter) []DataBlockMetadata {
	if query == nil {
		// If no query, return all blocks, as we can't prune any
		return blocks
	}

	var filtered []DataBlockMetadata
	for _, block := range blocks {
		if EvaluateDataBlockMetadata(&block, query) {
			filtered = append(filtered, block)
		}
	}
	return filtered
}

// =============================================================================
// Bloom Query API (for field, token, fieldtoken searches)
// =============================================================================

type BloomConditionType string

const (
	BloomField      BloomConditionType = "FIELD"
	BloomToken      BloomConditionType = "TOKEN"
	BloomFieldToken BloomConditionType = "FIELD_TOKEN"
)

type BloomCondition struct {
	Type  BloomConditionType
	Field string // for FIELD and FIELD_TOKEN
	Token string // for TOKEN and FIELD_TOKEN
}

type BloomExpressionType string

const (
	bloomExpressionCondition BloomExpressionType = "CONDITION"
	bloomExpressionAnd       BloomExpressionType = "AND"
	bloomExpressionOr        BloomExpressionType = "OR"
)

type BloomExpression struct {
	expressionType BloomExpressionType
	condition      *BloomCondition
	children       []BloomExpression
}

type BloomQuery struct {
	Expression *BloomExpression `json:",omitempty"`
}

type RegexCondition struct {
	Field   string
	Pattern string
}

type RegexExpressionType string

const (
	regexExpressionCondition RegexExpressionType = "CONDITION"
	regexExpressionAnd       RegexExpressionType = "AND"
	regexExpressionOr        RegexExpressionType = "OR"
)

type RegexExpression struct {
	expressionType RegexExpressionType
	condition      *RegexCondition
	children       []RegexExpression
}

type RegexQuery struct {
	Expression *RegexExpression `json:",omitempty"`
}

func Field(field string) BloomExpression {
	return BloomExpression{
		expressionType: bloomExpressionCondition,
		condition: &BloomCondition{
			Type:  BloomField,
			Field: field,
		},
	}
}

func Token(token string) BloomExpression {
	return BloomExpression{
		expressionType: bloomExpressionCondition,
		condition: &BloomCondition{
			Type:  BloomToken,
			Token: token,
		},
	}
}

func FieldToken(field, token string) BloomExpression {
	return BloomExpression{
		expressionType: bloomExpressionCondition,
		condition: &BloomCondition{
			Type:  BloomFieldToken,
			Field: field,
			Token: token,
		},
	}
}

func And(expressions ...BloomExpression) BloomExpression {
	return BloomExpression{
		expressionType: bloomExpressionAnd,
		children:       flattenExpressions(expressions, bloomExpressionAnd),
	}
}

func Or(expressions ...BloomExpression) BloomExpression {
	return BloomExpression{
		expressionType: bloomExpressionOr,
		children:       flattenExpressions(expressions, bloomExpressionOr),
	}
}

func flattenExpressions(expressions []BloomExpression, expressionType BloomExpressionType) []BloomExpression {
	flattened := make([]BloomExpression, 0, len(expressions))
	for _, expression := range expressions {
		if expression.expressionType == expressionType && expression.condition == nil {
			flattened = append(flattened, expression.children...)
			continue
		}
		flattened = append(flattened, expression)
	}
	return flattened
}

func FieldRegex(field, pattern string) RegexExpression {
	return RegexExpression{
		expressionType: regexExpressionCondition,
		condition: &RegexCondition{
			Field:   field,
			Pattern: pattern,
		},
	}
}

func RegexAnd(expressions ...RegexExpression) RegexExpression {
	return RegexExpression{
		expressionType: regexExpressionAnd,
		children:       flattenRegexExpressions(expressions, regexExpressionAnd),
	}
}

func RegexOr(expressions ...RegexExpression) RegexExpression {
	return RegexExpression{
		expressionType: regexExpressionOr,
		children:       flattenRegexExpressions(expressions, regexExpressionOr),
	}
}

func flattenRegexExpressions(expressions []RegexExpression, expressionType RegexExpressionType) []RegexExpression {
	flattened := make([]RegexExpression, 0, len(expressions))
	for _, expression := range expressions {
		if expression.expressionType == expressionType && expression.condition == nil {
			flattened = append(flattened, expression.children...)
			continue
		}
		flattened = append(flattened, expression)
	}
	return flattened
}

func regexExpressionToBloomFieldExpression(expression *RegexExpression) *BloomExpression {
	if expression == nil {
		return nil
	}

	switch expression.expressionType {
	case regexExpressionCondition:
		if expression.condition == nil {
			return nil
		}
		condition := &BloomCondition{
			Type:  BloomField,
			Field: expression.condition.Field,
		}
		return &BloomExpression{
			expressionType: bloomExpressionCondition,
			condition:      condition,
		}
	case regexExpressionAnd:
		children := make([]BloomExpression, 0, len(expression.children))
		for i := range expression.children {
			child := regexExpressionToBloomFieldExpression(&expression.children[i])
			if child != nil {
				children = append(children, *child)
			}
		}
		return &BloomExpression{
			expressionType: bloomExpressionAnd,
			children:       children,
		}
	case regexExpressionOr:
		children := make([]BloomExpression, 0, len(expression.children))
		for i := range expression.children {
			child := regexExpressionToBloomFieldExpression(&expression.children[i])
			if child != nil {
				children = append(children, *child)
			}
		}
		return &BloomExpression{
			expressionType: bloomExpressionOr,
			children:       children,
		}
	default:
		return nil
	}
}

func RegexFieldGuardBloomQuery(query *RegexQuery) *BloomQuery {
	if query == nil || query.Expression == nil {
		return nil
	}
	expression := regexExpressionToBloomFieldExpression(query.Expression)
	if expression == nil {
		return nil
	}
	return &BloomQuery{Expression: expression}
}

func AndBloomQueries(left, right *BloomQuery) *BloomQuery {
	if left == nil || left.Expression == nil {
		return right
	}
	if right == nil || right.Expression == nil {
		return left
	}
	combinedExpression := And(*left.Expression, *right.Expression)
	return &BloomQuery{Expression: &combinedExpression}
}

// Query combines prefiltering (partitions/minmax), bloom filtering, and regex scan filtering.
type Query struct {
	Prefilter *QueryPrefilter // for partitions and minmax indexes
	Bloom     *BloomQuery     // for field/token/fieldtoken searches
	Regex     *RegexQuery     // for field-scoped regex scan filtering
}

// NewQuery creates a query builder with an implicit AND expression.
func NewQuery() *QueryBuilder {
	return &QueryBuilder{
		query: &Query{
			Prefilter: NewQueryPrefilter(),
			Bloom:     &BloomQuery{},
			Regex:     &RegexQuery{},
		},
		implicitBloomAnd: make([]BloomExpression, 0),
		implicitRegexAnd: make([]RegexExpression, 0),
	}
}

type QueryBuilder struct {
	query *Query

	bloomExplicitSet bool
	implicitBloomAnd []BloomExpression
	regexExplicitSet bool
	implicitRegexAnd []RegexExpression
}

func (b *QueryBuilder) Field(field string) *QueryBuilder {
	b.addBloomExpression(Field(field))
	return b
}

func (b *QueryBuilder) Token(token string) *QueryBuilder {
	b.addBloomExpression(Token(token))
	return b
}

func (b *QueryBuilder) FieldToken(field, token string) *QueryBuilder {
	b.addBloomExpression(FieldToken(field, token))
	return b
}

func (b *QueryBuilder) where(expression BloomExpression) *QueryBuilder {
	b.bloomExplicitSet = true
	b.implicitBloomAnd = b.implicitBloomAnd[:0]
	b.query.Bloom.Expression = &expression
	return b
}

func (b *QueryBuilder) Match(expression BloomExpression) *QueryBuilder {
	return b.where(expression)
}

func (b *QueryBuilder) FieldRegex(field, pattern string) *QueryBuilder {
	b.addRegexExpression(FieldRegex(field, pattern))
	return b
}

func (b *QueryBuilder) whereRegex(expression RegexExpression) *QueryBuilder {
	b.regexExplicitSet = true
	b.implicitRegexAnd = b.implicitRegexAnd[:0]
	b.query.Regex.Expression = &expression
	return b
}

func (b *QueryBuilder) MatchRegex(expression RegexExpression) *QueryBuilder {
	return b.whereRegex(expression)
}

// Prefilter methods
func (b *QueryBuilder) MatchPrefilter(expression PrefilterExpression) *QueryBuilder {
	b.query.Prefilter.Expression = &expression
	return b
}

func (b *QueryBuilder) addBloomExpression(expression BloomExpression) {
	if b.bloomExplicitSet {
		if b.query.Bloom.Expression == nil {
			b.query.Bloom.Expression = &expression
			return
		}
		combined := And(*b.query.Bloom.Expression, expression)
		b.query.Bloom.Expression = &combined
		return
	}
	b.implicitBloomAnd = append(b.implicitBloomAnd, expression)
}

func (b *QueryBuilder) addRegexExpression(expression RegexExpression) {
	if b.regexExplicitSet {
		if b.query.Regex.Expression == nil {
			b.query.Regex.Expression = &expression
			return
		}
		combined := RegexAnd(*b.query.Regex.Expression, expression)
		b.query.Regex.Expression = &combined
		return
	}
	b.implicitRegexAnd = append(b.implicitRegexAnd, expression)
}

func (b *QueryBuilder) Build() *Query {
	if !b.bloomExplicitSet && len(b.implicitBloomAnd) > 0 {
		expression := And(b.implicitBloomAnd...)
		b.query.Bloom.Expression = &expression
	}
	if !b.regexExplicitSet && len(b.implicitRegexAnd) > 0 {
		expression := RegexAnd(b.implicitRegexAnd...)
		b.query.Regex.Expression = &expression
	}
	return b.query
}
