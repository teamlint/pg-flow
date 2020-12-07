package nats

import (
	"github.com/stretchr/testify/mock"
	"github.com/teamlint/pg-flow/event"
)

type publisherMock struct {
	mock.Mock
}

func (p *publisherMock) Publish(subject string, evt event.Event) error {
	args := p.Called(subject, evt)
	return args.Error(0)
}

func (p *publisherMock) Close() error {
	return p.Called().Error(0)
}
