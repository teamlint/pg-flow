package clickhouse

import (
	"bytes"
	"encoding/json"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/dump/handler"
	chrepo "github.com/teamlint/pg-flow/dump/repository/clickhouse"
	"github.com/teamlint/pg-flow/event"
	"github.com/teamlint/shard"
)

const (
	DefaultFileSize = uint32(10 * 1024 * 1024)   // 10 MB
	MaxFileSize     = uint32(2048 * 1024 * 1024) // 20 GB

	DefaultRows = 8164  // 默认批处理行数
	MaxRows     = 50000 // 最大行数
)

// ClickhouseHandler Clickhouse Handler
type ClickhouseHandler struct {
	writer *shard.Writer
	conn   *pgx.Conn
}

func New(fileSize uint32) handler.Handler {
	return &ClickhouseHandler{writer: shard.NewWriter("dump", shard.FileSize(fileSize), shard.Extension("json"))}
}

// Register 注册事件Handler
func Register(cfg *config.Config) {
	// handler
	var filesize uint32
	fs := cfg.Dumper.FileSize
	switch {
	case fs > 0:
		filesize = uint32(fs)
	case fs < 0:
		filesize = MaxFileSize
	default:
		filesize = DefaultFileSize
	}
	handler.RegisterHandler("clickhouse", New(filesize))
}

func (h *ClickhouseHandler) Init(cfg *config.Config) error {
	ch, err := chrepo.New(cfg)
	if err != nil {
		logrus.WithError(err).Errorln("handler.Init")
		return err
	}
	defer ch.Close()
	return ch.CreateTables()
}

func (h *ClickhouseHandler) Handle(evt *event.Event) error {
	// event over
	if evt.IsOver() {
		h.writer.Close()
		logrus.Infoln("clickhouse.handler event is over")
		return nil
	}
	logrus.WithField("evtID", evt.ID).
		WithField("table", evt.Table).
		WithField("action", evt.Action).
		Debugln("[event]")

	var buf bytes.Buffer
	dataBytes, err := json.Marshal(evt.Data)
	// evtData, err := evt.MarshalJSON()
	if err != nil {
		logrus.WithError(err).Error("clickhouse.handler json.Marshal")
		return nil
	}
	// TODO 针对不同表处理

	// data := gjson.GetBytes(evtData, "data").Raw
	dataBytes = append(dataBytes, "\n"...)
	buf.Grow(len(dataBytes))
	buf.Write(dataBytes)
	// logrus.WithField("dumper.handler", "clickhouse").Debugln(buf.String())
	h.writer.Write(buf.Bytes())

	// logrus.WithField("dumper.handler", "clickhouse").Debugln(string(dataBytes))
	// logrus.WithField("dumper.handler", "clickhouse").Debugln(data)
	// h.writer.WriteString(data + "\n")
	h.writer.Write(dataBytes)
	// TODO 实现 clickhouse 数据导入

	return h.writer.Err()
}
