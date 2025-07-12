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
	case int:
		minVal = int64(v)
		maxVal = int64(v)
	case int8:
		minVal = int64(v)
		maxVal = int64(v)
	case int16:
		minVal = int64(v)
		maxVal = int64(v)
	case int32:
		minVal = int64(v)
		maxVal = int64(v)
	case int64:
		minVal = v
		maxVal = v
	case uint:
		minVal = int64(v)
		maxVal = int64(v)
	case uint8:
		minVal = int64(v)
		maxVal = int64(v)
	case uint16:
		minVal = int64(v)
		maxVal = int64(v)
	case uint32:
		minVal = int64(v)
		maxVal = int64(v)
	case uint64:
		minVal = int64(v)
		maxVal = int64(v)
	case float32:
		minVal = int64(math.Floor(float64(v)))
		maxVal = int64(math.Ceil(float64(v)))
	case float64:
		minVal = int64(math.Floor(v))
		maxVal = int64(math.Ceil(v))
	default:
		return 0, 0, false
	}
	return minVal, maxVal, true
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
