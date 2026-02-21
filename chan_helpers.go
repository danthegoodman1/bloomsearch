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

// SendOptionalWithContext sends to a channel if it is non-nil.
func SendOptionalWithContext[T any](ctx context.Context, ch chan<- T, value T) error {
	if ch == nil {
		return nil
	}
	return SendWithContext(ctx, ch, value)
}

// SendToChannelsWithContext sends to each non-nil channel, blocking per channel until sent or context cancellation.
func SendToChannelsWithContext[T any](ctx context.Context, channels []chan T, value T) error {
	for _, ch := range channels {
		if err := SendOptionalWithContext(ctx, ch, value); err != nil {
			return err
		}
	}
	return nil
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
