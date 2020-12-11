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
	"io"
	"reflect"
	"testing"

	"github.com/teamlint/pg-flow/event"
)

func Test_parser_parseWalData(t *testing.T) {
	type fields struct {
		r io.Reader
	}
	type args struct {
		line string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *event.Event
	}{
		{
			name: "test1",
			fields: fields{
				r: nil,
			},
			args: args{
				// line: `INSERT INTO test_table (id, name) VALUES (1,"amazonriver");`,
				line: `INSERT INTO public."user" (id, name) VALUES ('7', 'sqlparser');`,
			},

			want: &event.Event{
				Schema: "public",
				Table:  "user",
				Action: "INSERT",
				Data:   map[string]interface{}{"id": "7", "name": "sqlparser"},
			},
		},
		{
			name: "test2",
			fields: fields{
				r: nil,
			},
			args: args{
				line: `DELETE FROM test.test_table WHERE id = 1;`,
			},
			want: nil,
		},
	}
	// parseSQL1
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser{
				r: tt.fields.r,
			}
			if got := p.parseSQL(tt.args.line); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("[parserSQL] got= %v, want %v", got, tt.want)
			}
		})
	}
}
