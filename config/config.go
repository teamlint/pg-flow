package config

import (
	"time"

	"github.com/asaskevich/govalidator"
)

// Config for pg-listener/
type Config struct {
	Listener  ListenerCfg  // 数据库监控器配置
	Database  DatabaseCfg  // 数据库连接配置
	Publisher PublisherCfg // 事件发布器配置
	Dumper    DumperCfg    // Dumper 配置
	Logger    LoggerCfg    // 日志配置
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string        `valid:"required"`
	PubName           string        // 发布名称
	Dump              bool          // 是否导出复制槽快照数据
	AckTimeout        time.Duration `valid:"required"`
	RefreshConnection time.Duration `valid:"required"`
	HeartbeatInterval time.Duration `valid:"required"`
}

type DumperCfg struct {
	Handler  string // Dump Handler: event,sql,es 默认event, 适合历史数据较少情况
	FileSize int    // Dump 文件大小字节: 0 使用默认 1 MB, -1 不限制文件大小,最大文件20GB
	Path     string // pg_dump 路径
}

// Table 目标仓库表配置信息
type Table map[string]string

// Repository 目标数据仓库
type Repository struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required"`
	Params   map[string]string
	Tables   map[string]Table
}

// PublisherCfg path of the evernt publisher config.
type PublisherCfg struct {
	Type        string `valid:"required"`
	Address     string `valid:"required"`
	ClusterID   string `valid:"required"`
	ClientID    string `valid:"required"`
	TopicPrefix string `valid:"required"`
}

// LoggerCfg path of the logger config.
type LoggerCfg struct {
	Caller        bool
	Level         string
	HumanReadable bool
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required"`
	Schema   string
	Filter   FilterStruct
}

// FilterStruct incoming WAL message filter.
type FilterStruct struct {
	Tables map[string][]string
}

// Validate config data.
func (c Config) Validate() error {
	_, err := govalidator.ValidateStruct(c)
	return err
}
