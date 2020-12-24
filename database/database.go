package database

import (
	"errors"

	"github.com/jackc/pgx"
)

// Database 数据库接口
type Database interface {
	// 获取复制槽的消费者要求的最旧WAL地址
	GetSlotLSN(slotName string) (string, error)
	// 检查发布是否存在
	PublicationIsExists(pubName string) (bool, error)
	// 创建发布
	CreatePublication(pubName string) error
	// 检查数据库连接是否正常
	IsAlive() bool
	// 关闭数据库链接
	Close() error
}

// DefaultDatabase service database.
type DefaultDatabase struct {
	conn *pgx.Conn
}

// New returns a new instance of the database.
func New(conn *pgx.Conn) *DefaultDatabase {
	return &DefaultDatabase{conn: conn}
}

// GetSlotLSN returns the value of the last offset for a specific slot.
func (r DefaultDatabase) GetSlotLSN(slotName string) (string, error) {
	var restartLSNStr string
	err := r.conn.QueryRow(
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;",
		slotName,
	).Scan(&restartLSNStr)
	return restartLSNStr, err
}

// PublicationIsExists 检查发布是否存在
func (r DefaultDatabase) PublicationIsExists(pubName string) (bool, error) {
	var name string
	err := r.conn.QueryRow(
		"SELECT pubname FROM pg_catalog.pg_publication WHERE pubname=$1;",
		pubName,
	).Scan(&name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, err
}

// CreatePublication 创建发布
func (r DefaultDatabase) CreatePublication(pubName string) error {
	sql := "CREATE PUBLICATION " + pubName + " FOR ALL TABLES;"
	_, err := r.conn.Exec(sql)
	// _, err := r.conn.Exec("CREATE PUBLICATION $1 FOR ALL TABLES;", pubName)
	return err
}

// IsAlive check database connection problems.
func (r DefaultDatabase) IsAlive() bool {
	return r.conn.IsAlive()
}

// Close database connection.
func (r DefaultDatabase) Close() error {
	return r.conn.Close()
}
