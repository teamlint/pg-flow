package esbulk

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/dump/handler"
	"github.com/teamlint/pg-flow/event"
	"github.com/teamlint/shard"
)

const (
	DefaultFileSize = uint32(1 * 1024 * 1024) // 1 MB
	// DefaultFileSize = uint32(512 * 1024) // 512 KB
)

// ElasticBulkHandler Bulk JSON 导入文件 Handler
type ElasticBulkHandler struct {
	writer *shard.Writer
}

func New(fileSize uint32) handler.Handler {
	return &ElasticBulkHandler{writer: shard.NewWriter("dump", shard.FileSize(fileSize), shard.Extension("json"))}
}

// Register 注册事件Handler
func Register(cfg *config.Config) {
	// handler
	handler.RegisterHandler("esbulk", New(DefaultFileSize))
}

func (h *ElasticBulkHandler) Handle(evt *event.Event) error {
	// event over
	if evt.IsOver() {
		// h.writer.WriteString("\n")
		logrus.Infoln("esbulk.handler event is over")
		return nil
	}
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
	meta := []byte(fmt.Sprintf(`{"%s":{"_index":"%s","_id":"%s"}}%s`, opType, evt.Table, evt.ID, "\n"))
	logrus.Debugf("%s\n", meta) // <-- Uncomment to see the payload
	if evt.Action != "DELETE" {
		dataBytes, _ := json.Marshal(evt.Data)
		// dataBytes := []byte(evt.Data)
		dataBytes = append(dataBytes, "\n"...) // <-- Comment out to trigger failure for batch
		buf.Grow(len(meta) + len(dataBytes))
		buf.Write(meta)
		buf.Write(dataBytes)
	}
	// logrus.WithField("doc", buf.String()).Debugln("bulk")
	h.writer.Write(buf.Bytes())

	return h.writer.Err()
}
