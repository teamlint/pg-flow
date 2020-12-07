package nats

import (
	"fmt"

	"github.com/nats-io/stan.go"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/event"
)

const (
	ErrNatsConnection = "nats connection error"
)

//go:generate  easyjson nats_publisher.go

// NatsPublisher represent event publisher.
type NatsPublisher struct {
	conn stan.Conn
}

// Close NATS connection.
func (n NatsPublisher) Close() error {
	return n.conn.Close()
}

// Publish serializes the event and publishes it on the bus.
func (n NatsPublisher) Publish(subject string, evt event.Event) error {
	msg, err := evt.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal err: %w", err)
	}
	return n.conn.Publish(subject, msg)
}

// New return new NatsPublisher instance.
func New(conn stan.Conn) *NatsPublisher {
	return &NatsPublisher{conn: conn}
}

// Register 注册 NATS 事件发布器
func Register(cfg *config.Config) {
	// nats publisher
	sc, err := stan.Connect(cfg.Publisher.ClusterID, cfg.Publisher.ClientID, stan.NatsURL(cfg.Publisher.Address))
	if err != nil {
		logrus.WithError(err).Fatalln(ErrNatsConnection)
	}

	event.RegisterPublisher("nats", New(sc))
}
