package bloomsearch

import (
	"math"
)

type MinMaxIndex struct {
	Min int64
	Max int64
}

// ConvertToMinMaxInt64 converts any numeric value to int64 min/max values.
// For integers, min and max are the same.
// For floats, min uses Floor and max uses Ceil.
// Returns false if the value is not a numeric type.
func ConvertToMinMaxInt64(value any) (minVal int64, maxVal int64, ok bool) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		intVal, _ := toInt64(v)
		return intVal, intVal, true
	case float32:
		return int64(math.Floor(float64(v))), int64(math.Ceil(float64(v))), true
	case float64:
		return int64(math.Floor(v)), int64(math.Ceil(v)), true
	default:
		return 0, 0, false
	}
}

// ConvertToInt64 converts any numeric value to int64.
// For floats, it rounds to the nearest integer.
// Returns false if the value is not a numeric type.
func ConvertToInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return toInt64(v)
	case float32:
		return int64(math.Round(float64(v))), true
	case float64:
		return int64(math.Round(v)), true
	default:
		return 0, false
	}
}

// toInt64 converts any integer type to int64
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
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	default:
		return 0, false
	}
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
