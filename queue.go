package queuer

import "errors"

var (
	ErrQueuePushOnly = errors.New("queue is push-only")
)

type Queue interface {
	Start() (ch chan Object)
	Publish(bts []byte) error
	PublishWithRoutingKey(rkey string, bts []byte) error
	Receive() <-chan Object
	Stop()
	EnableBatch()
	Enqueue(bts []byte)
}

type Object interface {
	Body() []byte
	Done() error
}
