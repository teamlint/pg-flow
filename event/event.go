package event

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	publishers           map[string]Publisher
	ErrPublisherNotFound = errors.New("publisher not found")
)

// Event event structure for publishing to the NATS server.
//easyjson:json
type Event struct {
	ID        uuid.UUID              `json:"id"`
	Schema    string                 `json:"schema"`
	Table     string                 `json:"table"`
	Action    string                 `json:"action"`
	Data      map[string]interface{} `json:"data"`
	EventTime time.Time              `json:"commitTime"`
}

// GetSubject creates subject name from the prefix, schema and table name.
func (e Event) GetSubject(prefix string) string {
	return fmt.Sprintf("%s_%s_%s", prefix, e.Schema, e.Table)
}

func GetPublisher(name string) (Publisher, error) {
	if pub, ok := publishers[name]; ok {
		return pub, nil
	}
	return nil, ErrPublisherNotFound
}
func RegisterPublisher(name string, pub Publisher) {
	publishers[name] = pub

}
func init() {
	publishers = make(map[string]Publisher)
}

// func NewEvent(tableMap map[string][]string) []event.Event {
// CreateEventsWithFilter filter WAL message by table,
// action and create events for each value.
// func NewEventsWithFilter(tableMap map[string][]string) []event.Event {
// 	var events []event.Event

// 	for _, item := range w.Actions {
// 		data := make(map[string]interface{})
// 		for _, val := range item.Columns {
// 			data[val.name] = val.value
// 		}
// 		evt := event.Event{
// 			ID:        uuid.New(),
// 			Schema:    item.Schema,
// 			Table:     item.Table,
// 			Action:    item.Kind.string(),
// 			Data:      data,
// 			EventTime: *w.CommitTime,
// 		}

// 		actions, validTable := tableMap[item.Table]
// 		validAction := inArray(actions, item.Kind.string())
// 		if validTable && validAction {
// 			events = append(events, evt)
// 		} else {
// 			logrus.WithFields(
// 				logrus.Fields{
// 					"schema": item.Schema,
// 					"table":  item.Table,
// 					"action": item.Kind,
// 				}).
// 				Infoln("wal message skip by filter")
// 		}
// 	}
// 	return events
// }

// // inArray checks whether the value is in an array.
// func inArray(arr []string, value string) bool {
// 	for _, v := range arr {
// 		if strings.EqualFold(v, value) {
// 			return true
// 		}
// 	}
// 	return false
// }
