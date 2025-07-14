package bloomsearch

import (
	"fmt"
	"time"
)

// FormatRate formats rate per second
func FormatRate(count int64, duration time.Duration) string {
	if duration == 0 {
		return "∞"
	}
	rate := float64(count) / duration.Seconds()
	return fmt.Sprintf("%.1f", rate)
}

// FormatBytesPerSecond formats bytes per second into human-readable format (B/s, KB/s, MB/s, GB/s, TB/s)
func FormatBytesPerSecond(bytes int64, duration time.Duration) string {
	if duration == 0 {
		return "∞ B/s"
	}

	bytesPerSecond := float64(bytes) / duration.Seconds()

	if bytesPerSecond < 1024 {
		return fmt.Sprintf("%.1f B/s", bytesPerSecond)
	}

	const unit = 1024
	if bytesPerSecond < unit*unit {
		return fmt.Sprintf("%.1f KB/s", bytesPerSecond/unit)
	}
	if bytesPerSecond < unit*unit*unit {
		return fmt.Sprintf("%.1f MB/s", bytesPerSecond/(unit*unit))
	}
	if bytesPerSecond < unit*unit*unit*unit {
		return fmt.Sprintf("%.1f GB/s", bytesPerSecond/(unit*unit*unit))
	}
	return fmt.Sprintf("%.1f TB/s", bytesPerSecond/(unit*unit*unit*unit))
}
