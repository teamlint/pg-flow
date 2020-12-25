// This code was derived from https://github.com/hellobike/amazonriver

package dump

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/dump/handler"
	"github.com/teamlint/pg-flow/event"

	sp "github.com/xwb1989/sqlparser"
)

const (
	// 标识 SQL 文件内容结束语句
	DumpCompleteStatement = "-- PostgreSQL database dump complete\n"
)

// sqlParser pg_dump sql解析器
type sqlParser struct {
	r   io.Reader
	buf bytes.Buffer
	n   int
}

func newSQLParser(r io.Reader) *sqlParser {
	return &sqlParser{r: r}
}

// Parse 解析sql文件,使用handler进行处理
func (p *sqlParser) Parse(h handler.Handler) error {
	rb := bufio.NewReaderSize(p.r, 1024*16)
	for {
		line, err := rb.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				evt := event.Event{
					ID:         event.OverID,
					CommitTime: time.Now().UTC(),
				}
				logrus.WithField("id", evt.ID).Infoln("event is over")
				if err = h.Handle(&evt); err != nil {
					logrus.Debugf("event.over handle err = %v\n", err)
					return err
				}
				break
			}
			return err
		}
		logrus.Debugf("sql.line = %v\n", line)
		evt := p.parseSQL(line)
		if evt == nil {
			continue
		}
		// logrus.Debugf("sql.evt = %+v\n", evt)
		if err := h.Handle(evt); err != nil {
			logrus.WithError(err).Debugln("sqlParser.Parse")
			return err
		}

	}
	return nil
}

// parseSQL 解析 SQL 语句为事件
func (p *sqlParser) parseEvent() *event.Event {
	s := strings.ReplaceAll(p.buf.String(), `"`, "")
	// logrus.Debugf("parseEvent.statement[%v] = %v\n", p.buf.Len(), s)
	stmt, err := sp.Parse(s)
	if err != nil {
		logrus.WithError(err).Warn("parseEvent")
		p.buf.Reset()
		return nil
	}
	switch row := stmt.(type) {
	case *sp.Insert:
		var data = map[string]interface{}{}
		var columns []string
		for _, clm := range row.Columns {
			columns = append(columns, clm.String())
		}
		if values, ok := row.Rows.(sp.Values); ok {

			value := values[0]
			for i, col := range value {
				name := columns[i]
				switch val := col.(type) {
				case *sp.SQLVal:
					data[name] = p.parseSQLVal(val)
				case *sp.NullVal:
					data[name] = nil
				}
			}
			evt := event.Event{
				ID:         uuid.New().String(),
				Schema:     row.Table.Qualifier.String(),
				Table:      row.Table.Name.String(),
				Action:     ActionKindInsert,
				Data:       data,
				CommitTime: time.Now().UTC(),
			}
			p.buf.Reset()
			logrus.WithField("ID", evt.ID).
				WithField("schema", evt.Schema).
				WithField("table", evt.Table).
				WithField("action", evt.Action).
				// WithField("data", evt.Data).
				Infoln("event was send")
			return &evt
		}
	}
	return nil
}

// parseSQL 解析SQL语句
func (p *sqlParser) parseSQL(line string) *event.Event {
	// if !(strings.HasPrefix(line, ActionKindInsert) || strings.HasPrefix(line, "--")) {
	if !strings.HasPrefix(line, ActionKindInsert) {
		if p.buf.Len() > 0 {
			// 文件结束
			if line == DumpCompleteStatement {
				logrus.WithField("dump.complete.statement", "end").Debugln(line)
				return p.parseEvent()
			}

			_, err := p.buf.WriteString(line)
			if err != nil {
				logrus.WithError(err).WithField("statement", "add").Errorln(line)
			}
			return nil
		}
		return nil
	}
	// statement begin
	if p.buf.Len() == 0 {
		// 忽略注释语句
		if strings.HasPrefix(line, "--") {
			return nil
		}
		// INSERT 语句开始
		if strings.HasPrefix(line, ActionKindInsert) {
			p.statementAdd(line)
			return nil
		}
	}

	defer p.statementAdd(line) // 解析缓存的sql语句后,添加当前行
	return p.parseEvent()
}

// statementAdd 增加部分sql语句
func (p *sqlParser) statementAdd(line string) error {
	_, err := p.buf.WriteString(line)
	if err != nil {
		logrus.WithError(err).WithField("statement", "add").Error(line)
	}
	return nil
}

func (p *sqlParser) parseSQLVal(val *sp.SQLVal) interface{} {
	switch val.Type {
	case sp.StrVal:
		return string(val.Val)
	case sp.IntVal:
		ret, _ := strconv.ParseInt(string(val.Val), 10, 64)
		return ret
	case sp.FloatVal:
		ret, _ := strconv.ParseFloat(string(val.Val), 64)
		return ret
	case sp.HexNum:
		return string(val.Val)
	case sp.HexVal:
		return string(val.Val)
	case sp.ValArg:
		return string(val.Val)
	case sp.BitVal:
		return string(val.Val)

	}
	return string(val.Val)
}
