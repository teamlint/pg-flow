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
