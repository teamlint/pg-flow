package event

import (
	"errors"
	"fmt"
	"time"
)

const (
	OverID = "_OVER_" // 事件结束 ID
)

var (
	publishers           map[string]Publisher
	ErrPublisherNotFound = errors.New("publisher not found")
)

// Event event structure for publishing to the NATS server.
//easyjson:json
type Event struct {
	ID         string                 `json:"id"`
	Schema     string                 `json:"schema"`
	Table      string                 `json:"table"`
	Action     string                 `json:"action"`
	Data       map[string]interface{} `json:"data"`
	CommitTime time.Time              `json:"commit_time"`
}

// GetSubject creates subject name from the prefix, schema and table name.
func (e Event) GetSubject(prefix string) string {
	return fmt.Sprintf("%s_%s_%s", prefix, e.Schema, e.Table)
}

// IsOver 判断是否是结束事件
func (e Event) IsOver() bool {
	return e.ID == OverID
}

func GetPublisher(name string) (Publisher, error) {
	if pub, ok := publishers[name]; ok {
		return pub, nil
	}
	return nil, ErrPublisherNotFound
}
func RegisterPublisher(name string, pub Publisher) {
	if _, ok := publishers[name]; !ok {
		publishers[name] = pub
	}
}

func init() {
	publishers = make(map[string]Publisher)
}
