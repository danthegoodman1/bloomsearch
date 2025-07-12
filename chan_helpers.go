package bloomsearch

// TryWriteChannel attempts to write a value to a channel without blocking.
// Returns true if the write was successful, false if the channel is full or nobody is listening.
func TryWriteChannel[T any](ch chan<- T, value T) bool {
	select {
	case ch <- value:
		return true
	default:
		return false
	}
}

// TryWriteToChannels sends a value to all channels using TryWriteChannel
func TryWriteToChannels[T any](channels []chan T, value T) {
	for _, ch := range channels {
		if ch != nil {
			TryWriteChannel(ch, value)
		}
	}
}
