package database

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

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
	// GetTableColumns 获取表结构信息
	GetTableColumns(schema string, table string) ([]Column, error)
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
func (d DefaultDatabase) GetSlotLSN(slotName string) (string, error) {
	var restartLSNStr string
	err := d.conn.QueryRow(
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1;",
		slotName,
	).Scan(&restartLSNStr)
	return restartLSNStr, err
}

// PublicationIsExists 检查发布是否存在
func (d DefaultDatabase) PublicationIsExists(pubName string) (bool, error) {
	var name string
	err := d.conn.QueryRow(
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
func (d DefaultDatabase) CreatePublication(pubName string) error {
	sql := "CREATE PUBLICATION " + pubName + " FOR ALL TABLES;"
	_, err := d.conn.Exec(sql)
	// _, err := r.conn.Exec("CREATE PUBLICATION $1 FOR ALL TABLES;", pubName)
	return err
}

// IsAlive check database connection problems.
func (d DefaultDatabase) IsAlive() bool {
	return d.conn.IsAlive()
}

// GetTableColumns 获取表结构信息
func (d DefaultDatabase) GetTableColumns(schema string, table string) ([]Column, error) {
	pgColumns := make([]Column, 0)

	sql := `select
  a.attname,
  not a.attnotnull,
  a.atttypid::regtype::text,
  string_to_array(substring(format_type(a.atttypid, a.atttypmod) from '\((.*)\)'), ',') as ext,
  coalesce(ai.attnum, 0) as pk_attnum,
  a.atttypmod,
  a.atttypid
from pg_class c
  inner join pg_namespace n on n.oid = c.relnamespace
  inner join pg_attribute a on a.attrelid = c.oid
  left join pg_index i on i.indrelid = a.attrelid and i.indisprimary
  left join pg_attribute ai on ai.attrelid = i.indexrelid and ai.attname = a.attname and ai.attisdropped = false
where`
	if schema == "" {
		sql += ` (n.nspname = $1 OR true)`
	} else {
		sql += ` n.nspname = $1`
	}
	sql += ` and c.relname = $2
  and a.attnum > 0
  and a.attisdropped = false
order by
  a.attnum`

	ctx := context.Background()
	tx, err := d.conn.BeginEx(ctx, &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, errors.Wrapf(err, "could not start pg transaction")
	}
	rows, err := tx.Query(sql, schema, table)

	if err != nil {
		return nil, fmt.Errorf("could not query: %v", err)
	}

	for rows.Next() {
		var (
			pgColumn Column
			baseType string
			extStr   []string
		)

		if err := rows.Scan(&pgColumn.Name, &pgColumn.IsNullable, &baseType, &extStr, &pgColumn.PKNum, &pgColumn.ModifierType, &pgColumn.TypeID); err != nil {
			return nil, errors.Errorf("could not scan: %v", err)
		}
		// is pk
		pgColumn.IsKey = pgColumn.PKNum > 0

		if baseType[len(baseType)-2:] == "[]" {
			pgColumn.IsArray = true
			pgColumn.BaseType = baseType[:len(baseType)-2]
		} else {
			pgColumn.BaseType = baseType
		}

		if extStr != nil {
			pgColumn.Ext, err = strToIntArray(extStr)
			if err != nil {
				return nil, errors.Errorf("could not convert into int array: %v", err)
			}
		}

		pgColumns = append(pgColumns, pgColumn)
	}

	return pgColumns, nil
}

// Close database connection.
func (d DefaultDatabase) Close() error {
	return d.conn.Close()
}

func strToIntArray(str []string) ([]int, error) {
	var err error
	ints := make([]int, len(str))
	for i, strVal := range str {
		ints[i], err = strconv.Atoi(strVal)
		if err != nil {
			return nil, err
		}
	}

	return ints, nil
}
