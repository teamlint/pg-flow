package database

import (
	"fmt"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
)

const (
	ErrMsgPostgresConnection    = "db connection error"
	ErrMsgReplicationConnection = "replication connection error"
)

// Variable with connection errors.
var (
	ErrReplConnectionIsLost = errors.New("replication connection to postgres is lost")
	ErrConnectionIsLost     = errors.New("db connection to postgres is lost")
)

// InitConnections initialise db and replication connections.
func InitConnections(cfg config.DatabaseCfg) (*pgx.Conn, *pgx.ReplicationConn, error) {
	pgxConf := pgx.ConnConfig{
		LogLevel: pgx.LogLevelInfo,
		Logger:   pgxLogger{},
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Name,
		User:     cfg.User,
		Password: cfg.Password,
	}
	pgConn, err := pgx.Connect(pgxConf)
	if err != nil {
		return nil, nil, errors.Wrap(err, ErrMsgPostgresConnection)
	}

	rConnection, err := pgx.ReplicationConnect(pgxConf)
	if err != nil {
		return nil, nil, fmt.Errorf("%v: %w", ErrMsgReplicationConnection, err)
	}
	return pgConn, rConnection, nil
}

type pgxLogger struct{}

func (l pgxLogger) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	logrus.Debugln(msg)
}
