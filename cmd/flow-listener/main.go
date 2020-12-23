package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/urfave/cli/v2"

	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/database"
	"github.com/teamlint/pg-flow/event"
	"github.com/teamlint/pg-flow/event/publisher/nats"
	"github.com/teamlint/pg-flow/listener"
	"github.com/teamlint/pg-flow/wal"
)

// go build -ldflags "-X main.version=1.0.1" main.go
var version = "0.0.1"

func main() {
	app := &cli.App{
		Name:    "flow-Listener",
		Usage:   "listen postgres events",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Value:   "config.yml",
				Aliases: []string{"c"},
				Usage:   "path to config file",
			},
		},
		Action: func(c *cli.Context) error {
			// config
			cfg, err := getConf(c.String("config"))
			if err != nil {
				logrus.WithError(err).Fatalln("getConf error")
			}
			if err = cfg.Validate(); err != nil {
				logrus.WithError(err).Fatalln("validate config error")
			}
			// logger
			initLogger(cfg.Logger)
			// database
			conn, rConn, err := initPgxConnections(cfg.Database)
			if err != nil {
				logrus.Fatal(err)
			}
			repo := database.New(conn)
			// publisher
			nats.Register(cfg)
			publisher, err := event.GetPublisher(cfg.Publisher.Type)
			if err != nil {
				logrus.Fatal(err)
			}
			// wal parser
			parser := wal.NewBinaryParser(binary.BigEndian)
			// listener
			l := listener.New(cfg, repo, rConn, publisher, parser)
			return l.Process()
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		logrus.Fatal(err)
	}
}

// getConf load config from file.
func getConf(path string) (*config.Config, error) {
	var cfg config.Config
	viper.SetConfigFile(path)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	err = viper.Unmarshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return &cfg, nil
}
