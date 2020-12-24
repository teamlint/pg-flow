package replicator

import (
	"context"

	"github.com/jackc/pgx"
)

// Replicator 复制器接口
type Replicator interface {
	// 创建逻辑复制槽
	CreateReplicationSlotEx(slotName, outputPlugin string) (consistentPoint string, snapshotName string, err error)
	// 删除复制槽
	DropReplicationSlot(slotName string) (err error)
	// 开始复制
	StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error)
	// 等待复制消息
	WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error)
	// 发送
	SendStandbyStatus(k *pgx.StandbyStatus) (err error)
	// 检查链接是否正常
	IsAlive() bool
	// 关闭链接
	Close() error
}

type DefaultReplicator struct {
	conn *pgx.ReplicationConn
}

func New(conn *pgx.ReplicationConn) Replicator {
	return &DefaultReplicator{conn: conn}
}

func (r *DefaultReplicator) CreateReplicationSlotEx(slotName, outputPlugin string) (consistentPoint string, snapshotName string, err error) {
	return r.conn.CreateReplicationSlotEx(slotName, outputPlugin)
}

func (r *DefaultReplicator) DropReplicationSlot(slotName string) (err error) {
	return r.conn.DropReplicationSlot(slotName)
}

func (r *DefaultReplicator) StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error) {
	return r.conn.StartReplication(slotName, startLsn, timeline, pluginArguments...)
}
func (r *DefaultReplicator) WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error) {
	return r.conn.WaitForReplicationMessage(ctx)
}

func (r *DefaultReplicator) SendStandbyStatus(k *pgx.StandbyStatus) (err error) {
	return r.conn.SendStandbyStatus(k)
}

func (r *DefaultReplicator) IsAlive() bool {
	return r.conn.IsAlive()
}

func (r *DefaultReplicator) Close() error {
	return r.conn.Close()
}
