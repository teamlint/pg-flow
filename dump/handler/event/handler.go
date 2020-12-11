package event

import (
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/dump/handler"
	"github.com/teamlint/pg-flow/event"
)

// EventHandler 事件Handler
type EventHandler struct {
	pub         event.Publisher // 事件发布器
	topicPrefix string          // 主题前缀
}

func New(pub event.Publisher, prefix string) handler.Handler {
	return &EventHandler{pub: pub, topicPrefix: prefix}
}

// Register 注册事件Handler
func Register(cfg *config.Config) {
	// publisher
	publisher, err := event.GetPublisher(cfg.Publisher.Type)
	if err != nil {
		logrus.Fatal(err)
	}

	// handler
	handler.RegisterHandler("event", New(publisher, cfg.Publisher.TopicPrefix))
}

func (h *EventHandler) Handle(evt *event.Event) error {
	if err := h.pub.Publish(evt.GetSubject(h.topicPrefix), evt); err != nil {
		logrus.Debugf("event handler err = %v\n", err)
		return err
	}
	return nil
}
