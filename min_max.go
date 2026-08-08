package bloomsearch

import (
	"math"
)

// MinMaxIndex records the observed numeric range of a field. Values outside
// the int64 range clamp to the int64 extremes at ingest (see
// ConvertToMinMaxInt64), so a bound equal to math.MaxInt64 or math.MinInt64
// is treated as saturated by EvaluateMinMaxCondition: the true extreme may
// lie beyond it, and boundary comparisons include rather than exclude the
// block.
type MinMaxIndex struct {
	Min int64
	Max int64
}

// ConvertToMinMaxInt64 converts any numeric value to int64 min/max values.
// For integers, min and max are the same.
// For floats, min uses Floor and max uses Ceil.
// Values outside the int64 range clamp to math.MinInt64/math.MaxInt64 so the
// index widens conservatively instead of wrapping (a wrapped value could make
// strict prefilters exclude blocks that contain matches).
// Returns false if the value is not a numeric type or is NaN (the value is not
// indexed).
func ConvertToMinMaxInt64(value any) (minVal int64, maxVal int64, ok bool) {
	switch v := value.(type) {
	case float32:
		return floatToMinMaxInt64(float64(v))
	case float64:
		return floatToMinMaxInt64(v)
	default:
		intVal, isInt := toInt64(value)
		if !isInt {
			return 0, 0, false
		}
		return intVal, intVal, true
	}
}

func floatToMinMaxInt64(v float64) (minVal int64, maxVal int64, ok bool) {
	if math.IsNaN(v) {
		return 0, 0, false
	}
	return clampFloatToInt64(math.Floor(v)), clampFloatToInt64(math.Ceil(v)), true
}

// ConvertToInt64 converts any numeric value to int64.
// For floats, it rounds to the nearest integer; values outside the int64 range
// clamp to the int64 bounds.
// Returns false if the value is not a numeric type or is NaN.
func ConvertToInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case float32:
		return floatToInt64(float64(v))
	case float64:
		return floatToInt64(v)
	default:
		return toInt64(value)
	}
}

func floatToInt64(v float64) (int64, bool) {
	if math.IsNaN(v) {
		return 0, false
	}
	return clampFloatToInt64(math.Round(v)), true
}

// clampFloatToInt64 converts a float64 to int64, clamping values beyond the
// int64 range (the direct conversion is implementation-defined on overflow).
func clampFloatToInt64(v float64) int64 {
	// float64(math.MaxInt64) is exactly 2^63, which already exceeds MaxInt64
	if v >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if v <= float64(math.MinInt64) {
		return math.MinInt64
	}
	return int64(v)
}

// toInt64 converts any integer type to int64, clamping unsigned values above
// math.MaxInt64 instead of wrapping negative.
func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return clampUint64ToInt64(uint64(v)), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return clampUint64ToInt64(v), true
	default:
		return 0, false
	}
}

func clampUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// UpdateMinMaxIndex updates an existing MinMaxIndex with new min/max values.
func UpdateMinMaxIndex(existing MinMaxIndex, newMin, newMax int64) MinMaxIndex {
	if newMin < existing.Min {
		existing.Min = newMin
	}
	if newMax > existing.Max {
		existing.Max = newMax
	}
	return existing
}
