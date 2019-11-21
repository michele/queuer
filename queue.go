package queuer

type Queue interface {
	Start() (ch chan Object)
	Publish(bts []byte) error
	PublishWithRoutingKey(rkey string, bts []byte) error
	Receive() <-chan Object
	Stop()
}

type Object interface {
	Body() []byte
	Done() error
}
