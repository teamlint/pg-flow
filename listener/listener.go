package listener

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"

	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/dump"
	"github.com/teamlint/pg-flow/event"
	"github.com/teamlint/pg-flow/replicator"
	"github.com/teamlint/pg-flow/repository"
	"github.com/teamlint/pg-flow/wal"
)

const errorBufferSize = 100

// Logical decoding plugin.
const (
	pgOutputPlugin = "pgoutput"
)

// Service info message.
const (
	StartServiceMessage = "service was started"
	StopServiceMessage  = "service was stopped"
)

// Listener main service struct.
type Listener struct {
	mu         sync.RWMutex
	config     config.Config
	slotName   string
	publisher  event.Publisher
	replicator replicator.Replicator
	repository repository.Repository
	parser     wal.Parser
	lsn        uint64
	errChannel chan error
}

// New create and initialize new listener service instance.
func New(
	cfg *config.Config,
	repo repository.Repository,
	repl replicator.Replicator,
	publ event.Publisher,
	parser wal.Parser,
) *Listener {
	return &Listener{
		slotName:   fmt.Sprintf("%s_%s", cfg.Listener.SlotName, cfg.Database.Name),
		config:     *cfg,
		publisher:  publ,
		repository: repo,
		replicator: repl,
		parser:     parser,
		errChannel: make(chan error, errorBufferSize),
	}
}

func (l *Listener) readLSN() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lsn
}

func (l *Listener) setLSN(lsn uint64) {
	l.mu.Lock()
	l.lsn = lsn
	l.mu.Unlock()
}

// Process is main service entry point.
func (l *Listener) Process() error {
	var serviceErr *serviceErr
	logger := logrus.WithField("slot_name", l.slotName)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	logrus.WithField("logger_level", l.config.Logger.Level).Infoln(StartServiceMessage)

	// 发布检查
	pubExists, err := l.publicationIsExists()
	if err != nil {
		logger.WithError(err).Errorln("publicationIsExists() error")
		return err
	}
	if !pubExists {
		if err := l.repository.CreatePublication(l.getPublicationName()); err != nil {
			logger.WithError(err).Infoln("CreatePublication() error")
			return err
		}
		logger.Infof("create new publication[%s]\n", l.getPublicationName())
	} else {
		logger.Infof("publication[%s] already exists\n", l.getPublicationName())
	}
	// 复制槽检查
	slotIsExists, err := l.slotIsExists()
	if err != nil {
		logger.WithError(err).Errorln("slotIsExists() error")
		return err
	}
	if !slotIsExists {
		// consistentPoint: 复制槽开始流的最早位置
		// snapshotID: 复制槽名称
		consistentPoint, snapshotID, err := l.replicator.CreateReplicationSlotEx(l.slotName, pgOutputPlugin)
		if err != nil {
			logger.WithError(err).Infoln("CreateReplicationSlotEx() error")
			return err
		}
		lsn, err := pgx.ParseLSN(consistentPoint)
		if err != nil {
			logger.WithError(err).Errorln("slotIsExists() error")
			return err
		}
		l.setLSN(lsn)
		logger.Infof("create new slot[%s], snapshot[%s]\n", l.slotName, snapshotID)
		// dump 同步旧数据
		if l.config.Listener.DumpSnapshot {
			l.exportSnapshot(snapshotID)
		}
	} else {
		logger.Infof("slot[%s] already exists, LSN updated\n", l.slotName)
	}

	go l.Stream(ctx)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	refresh := time.NewTicker(l.config.Listener.RefreshConnection)
ProcessLoop:
	for {
		select {
		case <-refresh.C:
			if !l.replicator.IsAlive() {
				logrus.Fatalln(ErrReplConnectionIsLost)
			}
			if !l.repository.IsAlive() {
				logrus.Fatalln(ErrConnectionIsLost)
				l.errChannel <- ErrConnectionIsLost
			}
		case err := <-l.errChannel:
			if errors.As(err, &serviceErr) {
				cancelFunc()
				logrus.Fatalln(err)
			} else {
				logrus.Errorln(err)
			}

		case <-signalChan:
			err := l.Stop()
			if err != nil {
				logrus.WithError(err).Errorln("l.Stop() error")
			}
			break ProcessLoop
		}
	}
	return nil
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotIsExists() (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(l.slotName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			logrus.
				WithField("slot", l.slotName).
				Warningln("restart_lsn for slot not found")
			return false, nil
		}
		return false, err
	}
	if len(restartLSNStr) == 0 {
		return false, nil
	}
	lsn, err := pgx.ParseLSN(restartLSNStr)
	if err != nil {
		return false, err
	}
	l.setLSN(lsn)
	return true, nil
}

// getPublicationName 获取发布名称
func (l *Listener) getPublicationName() string {
	pubName := l.config.Listener.PubName
	if pubName == "" {
		pubName = "pgflow"
	}
	return pubName
}

// publicationIsExists 检查发布是否存在
func (l *Listener) publicationIsExists() (bool, error) {
	return l.repository.PublicationIsExists(l.getPublicationName())
}

func publicationNames(publication string) string {
	return fmt.Sprintf(`publication_names '%s'`, publication)
}

const protoVersion = "proto_version '1'"

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) {
	err := l.replicator.StartReplication(l.slotName, l.readLSN(), -1, protoVersion, publicationNames(l.getPublicationName()))
	if err != nil {
		l.errChannel <- newListenerError("StartReplication()", err)
		return
	}

	go l.SendPeriodicHeartbeats(ctx)
	tx := wal.NewTransaction()
	for {
		if ctx.Err() != nil {
			l.errChannel <- newListenerError("read msg", err)
			break
		}
		msg, err := l.replicator.WaitForReplicationMessage(ctx)
		if err != nil {
			l.errChannel <- newListenerError("WaitForReplicationMessage()", err)
			continue
		}

		if msg != nil {
			if msg.WalMessage != nil {
				logrus.WithField("wal", msg.WalMessage.WalStart).
					Debugln("receive wal message")
				err := l.parser.ParseMessage(msg.WalMessage.WalData, tx)
				if err != nil {
					logrus.WithError(err).Errorln("msg parse failed")
					l.errChannel <- fmt.Errorf("%v: %w", ErrUnmarshalMsg, err)
					continue
				}
				if tx.CommitTime != nil {
					events := tx.CreateEventsWithFilter(l.config.Database.Filter.Tables)
					for _, event := range events {
						subjectName := event.GetSubject(l.config.Publisher.TopicPrefix)
						if err = l.publisher.Publish(subjectName, event); err != nil {
							l.errChannel <- fmt.Errorf("%v: %w", ErrPublishEvent, err)
							continue
						} else {
							logrus.
								WithField("subject", subjectName).
								WithField("action", event.Action).
								WithField("lsn", l.readLSN()).
								Infoln("event was send")
						}
					}
					tx.Clear()
				}

				if msg.WalMessage.WalStart > l.readLSN() {
					err = l.AckWalMessage(msg.WalMessage.WalStart)
					if err != nil {
						l.errChannel <- fmt.Errorf("%v: %w", ErrAckWalMessage, err)
						continue
					} else {
						logrus.WithField("lsn", l.readLSN()).Debugln("ack wal msg")
					}
				}
			}
			if msg.ServerHeartbeat != nil {
				//FIXME panic if there have been no messages for a long time.
				logrus.WithFields(logrus.Fields{
					"server_wal_end": msg.ServerHeartbeat.ServerWalEnd,
					"server_time":    msg.ServerHeartbeat.ServerTime,
				}).
					Debugln("received server heartbeat")
				if msg.ServerHeartbeat.ReplyRequested == 1 {
					logrus.Debugln("status requested")
					err = l.SendStandbyStatus()
					if err != nil {
						l.errChannel <- fmt.Errorf("%v: %w", ErrSendStandbyStatus, err)
					}
				}
			}
		}
	}
}

// Stop is a finalizer function.
func (l *Listener) Stop() error {
	var err error
	err = l.publisher.Close()
	if err != nil {
		return err
	}
	err = l.repository.Close()
	if err != nil {
		return err
	}
	err = l.replicator.Close()
	if err != nil {
		return err
	}
	logrus.Infoln(StopServiceMessage)
	return nil
}

// SendPeriodicHeartbeats send periodic keep alive heartbeats to the server.
func (l *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logrus.WithField("func", "SendPeriodicHeartbeats").
				Infoln("context was canceled, stop sending heartbeats")
			return
		case <-time.NewTicker(l.config.Listener.HeartbeatInterval).C:
			{
				err := l.SendStandbyStatus()
				if err != nil {
					logrus.WithError(err).Errorln("failed to send status heartbeat")
					continue
				}
				logrus.Debugln("sending periodic status heartbeat")
			}
		}
	}
}

// SendStandbyStatus sends a `StandbyStatus` object with the current RestartLSN value to the server.
func (l *Listener) SendStandbyStatus() error {
	standbyStatus, err := pgx.NewStandbyStatus(l.readLSN())
	if err != nil {
		return fmt.Errorf("unable to create StandbyStatus object: %w", err)
	}
	standbyStatus.ReplyRequested = 0
	err = l.replicator.SendStandbyStatus(standbyStatus)
	if err != nil {
		return fmt.Errorf("unable to send StandbyStatus object: %w", err)
	}
	return nil
}

// AckWalMessage acknowledge received wal message.
func (l *Listener) AckWalMessage(lsn uint64) error {
	l.setLSN(lsn)
	err := l.SendStandbyStatus()
	if err != nil {
		return err
	}
	return nil
}

// exportSnapshot 导出快照数据
func (l *Listener) exportSnapshot(snapshotID string) error {
	// replication slot already exists
	if snapshotID == "" || !l.config.Listener.DumpSnapshot {
		return nil
	}
	dumper := dump.New(&l.config)
	return dumper.Dump(snapshotID, l.publisher)
}
