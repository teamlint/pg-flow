package dump

import (
	"bufio"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/event"
)

func TestParse(t *testing.T) {
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
			name: "insert",
			fields: fields{
				r: nil,
			},
			args: args{
				// line: `INSERT INTO test_table (id, name) VALUES (1,"amazonriver");`,
				line: `INSERT INTO public.user (id, name) VALUES ('7', 'sqlparser');`,
			},

			want: &event.Event{
				Schema: "public",
				Table:  "user",
				Action: "INSERT",
				Data:   map[string]interface{}{"id": "7", "name": "sqlparser"},
			},
		},
		{
			name: "delete",
			fields: fields{
				r: nil,
			},
			args: args{
				line: `DELETE FROM test.test_table WHERE id = 1;`,
			},
			want: nil,
		},
	}
	// parseSQL
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &sqlParser{
				r: tt.fields.r,
			}
			if got := p.parseSQL(tt.args.line); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("[parserSQL] got= %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseFile(t *testing.T) {
	fs, err := os.Open("./test.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	logrus.SetLevel(logrus.DebugLevel)
	p := newSQLParser(fs)
	rb := bufio.NewReaderSize(fs, 1024*16)
	i := 0
	for {
		line, err := rb.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				t.Log("file is end")
				break
			}
			t.Fatal(err)
		}
		t.Logf("sql.line = %v", line)
		evt := p.parseSQL(line)
		if evt == nil {
			t.Log("sql.parsing...")
			continue
		}
		// 成功解析sql语句
		i++
		t.Logf("sql.parsed[%v]= %+v\n", i, evt)
	}
	if i != 17 {
		t.Errorf("insert statement's count = 17, got %v", i)
	}

}
