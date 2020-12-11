package esbulk

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/dump/handler"
	"github.com/teamlint/pg-flow/event"
)

// ElasticBulkHandler Bulk JSON 导入文件 Handler
type ElasticBulkHandler struct {
	maxDocs int // 一次批处理最多文档条数
}

func New(max int) handler.Handler {
	return &ElasticBulkHandler{maxDocs: max}
}

// Register 注册事件Handler
func Register(cfg *config.Config) {
	// handler
	handler.RegisterHandler("esbulk", New(2000))
}

func (h *ElasticBulkHandler) Handle(evt *event.Event) error {
	logrus.WithField("evtID", evt.ID).
		WithField("table", evt.Table).
		WithField("action", evt.Action).
		Debugln("[event]")
	// bulk
	var buf bytes.Buffer
	opType := "index"
	if evt.Action == "DELETE" {
		opType = "delete"
	}
	// meta := []byte(fmt.Sprintf(`{ "%s" : { "_id" : "%s" } }%s`, opType, docID, "\n"))
	// meta := []byte(fmt.Sprintf(`{ "%s" : { "_index" : "%s", "_id" : "%s" } }%s`, opType, table, docID, "\n"))
	// create 测试
	// opType = "create"
	meta := []byte(fmt.Sprintf(`{ "%s" : { "_index" : "%s", "_id" : "%s" } }%s`, opType, evt.Table, evt.ID, "\n"))
	// TODO 删除不需要传送数据
	logrus.Debugf("%s\n", meta) // <-- Uncomment to see the payload
	dataBytes, _ := json.Marshal(evt.Data)
	// dataBytes := []byte(evt.Data)
	dataBytes = append(dataBytes, "\n"...) // <-- Comment out to trigger failure for batch
	buf.Grow(len(meta) + len(dataBytes))
	buf.Write(meta)
	buf.Write(dataBytes)
	logrus.WithField("doc", buf.String()).
		Debugln("bulk")

	return nil

}
