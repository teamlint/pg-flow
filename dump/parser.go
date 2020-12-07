/*
 * Copyright 2018 Shanghai Junzheng Network Technology Co.,Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This code was derived from https://github.com/hellobike/amazonriver

package dump

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/event"

	sp "github.com/xwb1989/sqlparser"
)

type parser struct {
	r io.Reader
}

func newParser(r io.Reader) *parser {
	return &parser{r: r}
}

func (p *parser) parse(pub event.Publisher, topicPrefix string) error {
	rb := bufio.NewReaderSize(p.r, 1024*16)
	for {
		line, err := rb.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		logrus.Debugf("sql.line = %v\n", line)
		evt := p.parseSQL(line)
		if evt == nil {
			continue
		}
		logrus.Debugf("sql.evt = %+v\n", evt)
		if err := pub.Publish(evt.GetSubject(topicPrefix), *evt); err != nil {
			logrus.Debugf("event publish err = %v\n", err)
			return err
		}

	}
	return nil
}

// parseSQL 解析 SQL 语句为事件
func (p *parser) parseSQL(line string) *event.Event {
	if !strings.HasPrefix(line, ActionKindInsert) {
		return nil
	}

	line = strings.ReplaceAll(line, `"`, "")
	stmt, err := sp.Parse(line)
	if err != nil {
		logrus.Debugf("parseSQL.err = %v\n", err)
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
				ID:        uuid.New(),
				Schema:    row.Table.Qualifier.String(),
				Table:     row.Table.Name.String(),
				Action:    ActionKindInsert,
				Data:      data,
				EventTime: time.Now(),
			}
			logrus.WithField("ID", evt.ID).
				WithField("schema", evt.Schema).
				WithField("table", evt.Table).
				WithField("action", evt.Action).
				Infoln("event was send")
			return &evt
		}
	}
	return nil
}

func (p *parser) parseSQLVal(val *sp.SQLVal) interface{} {
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
