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
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/event"
)

const (
	ActionKindInsert = "INSERT"
)

// Dumper dump database
type Dumper struct {
	pgDump string
	conf   *config.Config
}

// New create a Dumper
func New(conf *config.Config) *Dumper {
	pgDump := conf.Listener.DumpPath
	if pgDump == "" {
		pgDump = "pg_dump"
	}
	path, _ := exec.LookPath(pgDump)
	return &Dumper{pgDump: path, conf: conf}
}

// Dump database with snapshot, parse sql then write to handler
func (d *Dumper) Dump(snapshotID string, pub event.Publisher) error {
	args := make([]string, 0, 16)

	// Common args
	args = append(args, fmt.Sprintf("--host=%s", d.conf.Database.Host))
	args = append(args, fmt.Sprintf("--port=%d", d.conf.Database.Port))

	args = append(args, fmt.Sprintf("--username=%s", d.conf.Database.User))

	args = append(args, d.conf.Database.Name)
	args = append(args, "--data-only")
	args = append(args, "--column-inserts")

	// --table 参数使用时, --schema 不起作用
	if d.conf.Database.Schema != "" {
		args = append(args, fmt.Sprintf("--schema=%s", d.conf.Database.Schema))
	}
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
	parser := newParser(r)
	go func() {
		err := parser.parse(pub, d.conf.Publisher.TopicPrefix)
		errCh <- err
	}()

	logrus.Infof("pg_dump %s\n", cmd.Args)

	err := cmd.Run()
	w.CloseWithError(err)

	err = <-errCh
	return err
}
