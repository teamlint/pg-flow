package dump

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/dump/handler"
	"github.com/teamlint/pg-flow/dump/handler/clickhouse"
	"github.com/teamlint/pg-flow/dump/handler/esbulk"
	"github.com/teamlint/pg-flow/dump/handler/event"
)

const (
	ActionKindInsert = "INSERT"
)

// Dumper dump database
type Dumper struct {
	pgDump  string
	handler handler.Handler
	conf    *config.Config
}

// New create a Dumper
func New(conf *config.Config) *Dumper {
	pgDump := conf.Dumper.Path
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	path, _ := exec.LookPath(pgDump)
	return &Dumper{pgDump: path, conf: conf}
}

// Dump database with snapshot, parse sql then write to handler
func (d *Dumper) Dump(snapshotID string) error {
	args := make([]string, 0, 16)

	// Common args
	args = append(args, fmt.Sprintf("--host=%s", d.conf.Database.Host))
	args = append(args, fmt.Sprintf("--port=%d", d.conf.Database.Port))

	args = append(args, fmt.Sprintf("--username=%s", d.conf.Database.User))

	args = append(args, d.conf.Database.Name)
	args = append(args, "--data-only")
	args = append(args, "--column-inserts")

	// --table 参数使用时, --schema 不起作用
	// if d.conf.Database.Schema != "" {
	// 	args = append(args, fmt.Sprintf("--schema=%s", d.conf.Database.Schema))
	// }
	for table, _ := range d.conf.Database.Filter.Tables {
		args = append(args, fmt.Sprintf(`--table=%s`, table))
	}
	args = append(args, fmt.Sprintf("--snapshot=%s", snapshotID))

	cmd := exec.Command(d.pgDump, args...)
	if d.conf.Database.Password != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", d.conf.Database.Password))
	}
	r, w := io.Pipe()

	cmd.Stdout = w
	cmd.Stderr = os.Stderr

	errCh := make(chan error)

	// handler
	event.Register(d.conf)
	esbulk.Register(d.conf)
	clickhouse.Register(d.conf)
	hdr, err := handler.GetHandler(d.conf.Dumper.Handler)
	if err != nil {
		logrus.WithError(err).WithField("dumper.handler", d.conf.Dumper.Handler).Error("handler.GetHandler")
		return err
	}
	err = hdr.Init(d.conf) // dumper handler 初始化,仅执行一次
	if err != nil {
		logrus.WithError(err).WithField("dumper.handler", d.conf.Dumper.Handler).Error("handler.Init error")
		return err
	}
	logrus.WithField("dumper.handler", d.conf.Dumper.Handler).Infoln("handler.GetHandler")
	// sql parser
	sqlParser := newSQLParser(r)
	go func() {
		err := sqlParser.Parse(hdr)
		errCh <- err
	}()

	logrus.Infof("pg_dump %s\n", cmd.Args)

	err = cmd.Run()
	w.CloseWithError(err)

	err = <-errCh
	return err
}
