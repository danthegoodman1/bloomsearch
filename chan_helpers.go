package bloomsearch

import (
	"context"
	"errors"
)

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
// Every channel is attempted even when earlier sends fail, so a canceled
// context on one waiter never starves delivery to the others; the errors are
// joined in the return value.
func SendToChannelsWithContext[T any](ctx context.Context, channels []chan T, value T) error {
	var errs []error
	for _, ch := range channels {
		if err := SendOptionalWithContext(ctx, ch, value); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SendWithContext attempts to send a value to a channel while respecting context cancellation.
// Returns an error if the context is done before the send completes.
// A ready channel always receives the value, even when ctx is already
// canceled: the non-blocking attempt runs first, so cancellation only stops
// sends that would otherwise block.
func SendWithContext[T any](ctx context.Context, ch chan<- T, value T) error {
	select {
	case ch <- value:
		return nil
	default:
	}
	select {
	case ch <- value:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
