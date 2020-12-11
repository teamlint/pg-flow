package handler

import (
	"errors"

	"github.com/teamlint/pg-flow/event"
)

var (
	handlers           map[string]Handler
	ErrHandlerNotFound = errors.New("dump handler not found")
)

// Handler dump 数据处理器
type Handler interface {
	Handle(evt *event.Event) error
}

func GetHandler(name string) (Handler, error) {
	if pub, ok := handlers[name]; ok {
		return pub, nil
	}
	return nil, ErrHandlerNotFound
}
func RegisterHandler(name string, h Handler) {
	if _, ok := handlers[name]; !ok {
		handlers[name] = h
	}
}

func init() {
	handlers = make(map[string]Handler)
}
