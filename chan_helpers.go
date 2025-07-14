package bloomsearch

import "context"

// TryWriteChannel attempts to write a value to a channel without blocking.
// Returns true if the write was successful, false if the channel is full, nobody is listening, or the channel is nil.
func TryWriteChannel[T any](ch chan<- T, value T) bool {
	if ch == nil {
		return false
	}
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

// SendWithContext attempts to send a value to a channel while respecting context cancellation.
// Returns an error if the context is done before the send completes.
func SendWithContext[T any](ctx context.Context, ch chan<- T, value T) error {
	select {
	case ch <- value:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
