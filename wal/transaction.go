package wal

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/pgtype"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/event"
)

// ActionKind kind of action on WAL message.
type ActionKind string

// kind of WAL message.
const (
	ActionKindInsert ActionKind = "INSERT"
	ActionKindUpdate ActionKind = "UPDATE"
	ActionKindDelete ActionKind = "DELETE"
	// SQL Timestamp
	SQLTimestamptzLayout = "2006-01-02 15:04:05.999999-07"
	SQLTimestampLayout   = "2006-01-02 15:04:05.999999"
	SQLDateLayout        = "2006-01-02"
)

// Transaction transaction specified WAL message.
type Transaction struct {
	LSN           int64
	BeginTime     *time.Time
	CommitTime    *time.Time
	RelationStore map[int32]RelationData
	Actions       []ActionData
}

// NewTransaction create and initialize new WAL transaction.
func NewTransaction() *Transaction {
	return &Transaction{
		RelationStore: make(map[int32]RelationData),
	}
}

func (k ActionKind) string() string {
	return string(k)
}

// RelationData kind of WAL message data.
type RelationData struct {
	Schema  string
	Table   string
	Columns []Column
}

// ActionData kind of WAL message data.
type ActionData struct {
	Schema  string
	Table   string
	Kind    ActionKind
	Columns []Column
}

// Column of the table with which changes occur.
type Column struct {
	name      string
	value     interface{}
	valueType int
	isKey     bool
}

// AssertValue converts bytes to a specific type depending
// on the type of this data in the database table.
func (c *Column) AssertValue(src []byte) {
	var val interface{}
	var err error
	strSrc := string(src)
	switch c.valueType {
	case pgtype.BoolOID:
		val, _ = strconv.ParseBool(strSrc)
	case pgtype.Int4OID:
		val, _ = strconv.Atoi(strSrc)
	case pgtype.Int8OID:
		val, _ = strconv.ParseInt(strSrc, 10, 64)
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		val = strSrc
	case pgtype.DateOID:
		if strSrc == "" {
			val = nil
		} else {
			val, err = time.Parse(SQLDateLayout, strSrc)
			if err != nil {
				logrus.WithField("pgtype", c.valueType).Errorf("%s => %v ", strSrc, val)
			}
		}
	case pgtype.TimestampOID:
		if strSrc == "" {
			val = nil
		} else {
			val, err = time.Parse(SQLTimestampLayout, strSrc)
			if err != nil {
				logrus.WithField("pgtype", c.valueType).Errorf("%s => %v ", strSrc, val)
			}
		}
	case pgtype.TimestamptzOID:
		if strSrc == "" {
			val = nil
		} else {
			tz, err := time.Parse(SQLTimestamptzLayout, strSrc)
			if err != nil {
				logrus.WithField("pgtype", c.valueType).Errorf("%s => %v ", strSrc, tz)
			}
			val = tz.UTC()
			// logrus.WithField("pgtype", c.valueType).Infof("%s => %v ", strSrc, val)
		}
	case pgtype.JSONBOID, pgtype.JSONOID:
		if strSrc == "" {
			val = nil
		} else {
			val = strSrc
		}
	default:
		logrus.WithField("pgtype", c.valueType).Warnln("unknown oid type")
		val = strSrc
	}
	c.value = val
}

// Clear transaction data.
func (w *Transaction) Clear() {
	w.CommitTime = nil
	w.BeginTime = nil
	w.Actions = nil
}

// CreateActionData create action  from WAL message data.
func (w Transaction) CreateActionData(
	relationID int32,
	rows []TupleData,
	kind ActionKind,
) (a ActionData, err error) {
	rel, ok := w.RelationStore[relationID]
	if !ok {
		return a, errors.New("relation not found")
	}
	a = ActionData{
		Schema: rel.Schema,
		Table:  rel.Table,
		Kind:   kind,
	}
	var columns []Column
	for num, row := range rows {
		column := Column{
			name:      rel.Columns[num].name,
			valueType: rel.Columns[num].valueType,
			isKey:     rel.Columns[num].isKey,
		}
		column.AssertValue(row.Value)

		columns = append(columns, column)
	}
	a.Columns = columns
	return a, nil
}

// CreateEventsWithFilter filter WAL message by table,
// action and create events for each value.
func (w *Transaction) CreateEventsWithFilter(schema string, tableMap map[string][]string) []*event.Event {
	var events []*event.Event

	for _, item := range w.Actions {
		data := make(map[string]interface{})
		for _, val := range item.Columns {
			data[val.name] = val.value
		}
		evt := event.Event{
			ID:         uuid.New().String(),
			Schema:     item.Schema,
			Table:      item.Table,
			Action:     item.Kind.string(),
			Data:       data,
			CommitTime: *w.CommitTime,
		}

		validSchema := eqSchema(schema, item.Schema)
		actions, validTable := tableMap[item.Table]
		validAction := inArray(actions, item.Kind.string())
		if validSchema && validTable && validAction {
			events = append(events, &evt)
		} else {
			logrus.WithFields(
				logrus.Fields{
					"schema": item.Schema,
					"table":  item.Table,
					"action": item.Kind,
				}).
				Infoln("wal message skip by filter")
		}
	}
	return events
}

// inArray checks whether the value is in an array.
func inArray(arr []string, value string) bool {
	for _, v := range arr {
		if strings.EqualFold(v, value) {
			return true
		}
	}
	return false
}
func eqSchema(schema string, value string) bool {
	if schema == "" {
		return true
	}
	return strings.EqualFold(schema, value)
}
