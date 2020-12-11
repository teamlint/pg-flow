package event

// Publisher 事件发布接口
type Publisher interface {
	Publish(subject string, evt *Event) error
	Close() error
}
