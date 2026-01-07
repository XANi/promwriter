package promwriter

import "fmt"

type QueueFullError struct{}

func (e QueueFullError) Error() string {
	return fmt.Sprintf("send queue full")
}
