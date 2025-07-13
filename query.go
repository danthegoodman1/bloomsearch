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

// MinMaxFieldCombinator specifies how conditions across different MinMaxIndex fields should be combined
type MinMaxFieldCombinator string

const (
	// MinMaxCombineWithAND requires all MinMaxIndex field conditions to match
	MinMaxCombineWithAND MinMaxFieldCombinator = "AND"
	// MinMaxCombineWithOR requires any MinMaxIndex field condition to match
	MinMaxCombineWithOR MinMaxFieldCombinator = "OR"
)

// QueryCondition represents a complete query with conditions on partitions and MinMaxIndexes.
//
// Partition conditions are OR-ed together (since a block can only belong to one partition)
// MinMaxIndex conditions are OR-ed within a field, and either AND or OR-ed across fields (Default is AND).
type QueryCondition struct {
	// Partition conditions - these are OR-ed together (since a block can only belong to one partition)
	PartitionConditions []StringCondition `json:",omitempty"`

	// MinMaxIndex conditions - map field name to list of conditions (conditions are OR-ed within a field)
	MinMaxConditions map[string][]NumericCondition `json:",omitempty"`

	// How to combine conditions across different MinMaxIndex fields (AND or OR)
	// Default is AND if not specified
	MinMaxFieldCombinator MinMaxFieldCombinator `json:",omitempty"`
}

// NewQueryCondition creates a new empty query condition with AND MinMaxIndex field combinator
func NewQueryCondition() *QueryCondition {
	return &QueryCondition{
		PartitionConditions:   make([]StringCondition, 0),
		MinMaxConditions:      make(map[string][]NumericCondition),
		MinMaxFieldCombinator: MinMaxCombineWithAND,
	}
}

// AddPartitionCondition adds a partition condition
func (q *QueryCondition) AddPartitionCondition(condition StringCondition) *QueryCondition {
	q.PartitionConditions = append(q.PartitionConditions, condition)
	return q
}

// AddMinMaxCondition adds a MinMaxIndex condition for a specific field
func (q *QueryCondition) AddMinMaxCondition(fieldName string, condition NumericCondition) *QueryCondition {
	if q.MinMaxConditions == nil {
		q.MinMaxConditions = make(map[string][]NumericCondition)
	}
	q.MinMaxConditions[fieldName] = append(q.MinMaxConditions[fieldName], condition)
	return q
}

// WithMinMaxFieldCombinator sets how conditions across different MinMaxIndex fields should be combined
func (q *QueryCondition) WithMinMaxFieldCombinator(combinator MinMaxFieldCombinator) *QueryCondition {
	q.MinMaxFieldCombinator = combinator
	return q
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

// EvaluateDataBlockMetadata checks if a DataBlockMetadata matches the query conditions
func EvaluateDataBlockMetadata(metadata *DataBlockMetadata, query *QueryCondition) bool {
	// Check partition conditions (any can match, since a block can only belong to one partition)
	if len(query.PartitionConditions) > 0 {
		partitionMatches := false
		for _, partitionCondition := range query.PartitionConditions {
			if EvaluateStringCondition(metadata.PartitionID, partitionCondition) {
				partitionMatches = true
				break
			}
		}
		if !partitionMatches {
			return false
		}
	}

	// Check MinMaxIndex conditions
	if len(query.MinMaxConditions) == 0 {
		// No MinMaxIndex conditions to check
		return true
	}

	// Handle OR case first, then fall through to AND (default)
	if query.MinMaxFieldCombinator == MinMaxCombineWithOR {
		// Any field can match (within each field conditions are OR-ed)
		for fieldName, conditions := range query.MinMaxConditions {
			minMaxIndex, exists := metadata.MinMaxIndexes[fieldName]
			if !exists {
				// If the field doesn't exist in the metadata, skip this field
				continue
			}

			// Check if any condition for this field matches
			for _, condition := range conditions {
				if EvaluateMinMaxCondition(minMaxIndex, condition) {
					// Found a match, we can return true immediately
					return true
				}
			}
		}

		// No field matched
		return false
	}

	// Default AND behavior: All fields must match (within each field conditions are OR-ed)
	for fieldName, conditions := range query.MinMaxConditions {
		minMaxIndex, exists := metadata.MinMaxIndexes[fieldName]
		if !exists {
			// If the field doesn't exist in the metadata, we can't match the condition
			return false
		}

		// At least one condition for this field must match
		fieldMatches := false
		for _, condition := range conditions {
			if EvaluateMinMaxCondition(minMaxIndex, condition) {
				fieldMatches = true
				break
			}
		}

		if !fieldMatches {
			return false
		}
	}

	// Everything matched
	return true
}

// FilterDataBlocks filters a slice of DataBlockMetadata based on query conditions
func FilterDataBlocks(blocks []DataBlockMetadata, query *QueryCondition) []DataBlockMetadata {
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
