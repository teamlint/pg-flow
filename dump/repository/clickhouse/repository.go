package clickhouse

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
)

type ClickhouseRepository struct {
	cfg    *config.Config
	pgconn *pgx.ReplicationConn
	ch     clickhouse.Clickhouse
}

// New
func New(cfg *config.Config) (*ClickhouseRepository, error) {
	repo := &ClickhouseRepository{
		cfg: cfg,
	}
	// postgres
	// pgconn
	pgconn, err := postgresConnect(cfg.Database)
	if err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.postgresConnect")
		return nil, err
	}
	repo.pgconn = pgconn
	// clickhouse
	ch, err := clickhouseConnect(cfg.Dumper.Repository)
	if err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.clickhouseConnect")
		return nil, err
	}
	repo.ch = ch
	return repo, nil
}

// postgresConnect 创建 Postgresql 数据库连接
func postgresConnect(cfg config.DatabaseCfg) (*pgx.ReplicationConn, error) {
	pgxConf := pgx.ConnConfig{
		// LogLevel: pgx.LogLevelInfo,
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Name,
		User:     cfg.User,
		Password: cfg.Password,
	}

	conn, err := pgx.ReplicationConnect(pgxConf)
	if err != nil {
		logrus.WithError(err).Error("clickhouse.PostgresConnect")
		return nil, err
	}

	return conn, nil
}

func clickhouseConnect(cfg config.RepositoryCfg) (clickhouse.Clickhouse, error) {
	conn, err := clickhouse.OpenDirect(connectionString(cfg))
	if err != nil {
		logrus.WithError(err).Error("clickhouse.clickhouseConnect")
		return nil, err
	}

	return conn, nil
}

func connectionString(c config.RepositoryCfg) string {
	connStr := url.Values{}

	connStr.Add("username", c.User)
	connStr.Add("password", c.Password)
	connStr.Add("database", c.Name)

	for param, value := range c.Params {
		connStr.Add(param, value)
	}

	return fmt.Sprintf("tcp://%s:%d?%s", c.Host, c.Port, connStr.Encode())
}

// GenerateDDL 生成DDL语句
func (r *ClickhouseRepository) generateDDL() (string, error) {
	// return "ddl statement", nil
	var tableDDL string

	ctx := context.Background()
	tx, err := r.pgconn.BeginEx(ctx, &pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", errors.Wrapf(err, "could not start pg transaction")
	}
	for tblName, _ := range r.cfg.Database.Filter.Tables {
		var (
			pkNum        int
			engineParams string
			orderBy      string
		)

		schema := "public"
		walColumns, pgColumns, err := TablePGColumns(tx, schema, tblName)
		_ = walColumns
		if err != nil {
			return "", errors.Wrapf(err, "could not get columns for %s.%s postgres table", schema, tblName)
		}

		// clickhouse 表配置
		tblCfg, tblOK := r.cfg.Dumper.Repository.Tables[tblName]
		chColumnDDLs := make([]string, 0)

		for name, pgCol := range pgColumns {
			// 如果有表配置
			// TODO
			// if tblOK {
			// 	// 如果有列配置
			// 	colsCfg, colsOK := tblCfg["columns"]
			// 	if colsOK {
			// 		// 如果不包含指定列, 跳过
			// 		if colsCfg[name] == "" {
			// 			continue
			// 		}
			// 	}
			// }
			// 没有表配置或没有列配置, 则全列复制
			// 转换pg列到ch列
			chColType, err := PG2CHType(pgCol)
			if err != nil {
				return "", errors.Wrapf(err, "could not get clickhouse column definition")
			}
			if pgCol.PKNum > 0 && pgCol.PKNum > pkNum {
				pkNum = pgCol.PKNum
			}

			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", name, chColType))
		}
		// 主键列
		pkColumns := make([]string, pkNum)

		for pgColName, pgCol := range pgColumns {
			if pgCol.PKNum < 1 {
				continue
			}

			pkColumns[pgCol.PKNum-1] = pgColName
		}

		param := tblCfg["generationColumn"]
		if param != "" {
			chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt32", param))
		}

		// 表引擎
		tableEngine := tblCfg["engine"]
		if tblOK {

			switch tableEngine {
			case MergeTree:
			case ReplacingMergeTree:
				engineParams = tblCfg["verColumn"]
				if engineParams != "" {
					chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt64", engineParams))
				}
				// chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt8", tblCfg["deletedColumn"])
			case CollapsingMergeTree:
				engineParams = tblCfg["signColumn"]
				chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s Int8", engineParams))
			case VersionedCollapsingMergeTree:
				engineParams = tblCfg["signColumn"]
				chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s Int8", engineParams))
				engineParams += tblCfg["verColumn"]
				chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt64", engineParams))
			}
		}
		// default table engine
		if tableEngine == "" {
			defaultTableEngine := r.cfg.Dumper.Repository.DefaultTableEngine
			if defaultTableEngine == "" {
				defaultTableEngine = ReplacingMergeTree
			}
			tableEngine = defaultTableEngine
		}
		tableDDL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) Engine = %s(%s)",
			tblName,
			strings.Join(chColumnDDLs, ",\n"),
			tableEngine, engineParams)

		if tblCfg["orderBy"] == "" {
			if len(pkColumns) > 0 {
				orderBy = fmt.Sprintf(" ORDER BY(%s)", strings.Join(pkColumns, ", "))
			}
		} else {
			orderBy = fmt.Sprintf(" ORDER BY(%s)", tblCfg["orderBy"])
		}
		tableDDL += orderBy + ";\n"
	}

	// 	fmt.Println(tableDDL)

	// 	if tblCfg.ChBufferTable != "" {
	// 		fmt.Println(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n) Engine = MergeTree()%s;",
	// 			tblCfg.ChBufferTable,
	// 			strings.Join(
	// 				append(chColumnDDLs, fmt.Sprintf("    %s UInt64", tblCfg.BufferTableRowIdColumn)), ",\n"),
	// 			orderBy))
	// 	}

	// }

	return tableDDL, nil
}
func (r *ClickhouseRepository) CreateTables() error {
	// generate DDL
	ddl, err := r.generateDDL()
	if err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.generateDDL")
		return err
	}
	// create table
	r.ch.Begin()
	stmt, _ := r.ch.Prepare(ddl)
	if _, err = stmt.Exec([]driver.Value{}); err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.stmt.Exec")
		return err
	}
	return r.ch.Commit()
}

func (r *ClickhouseRepository) InsertData(table string, json string) error {
	inputFormatSQL := "SET input_format_import_nested_json=1;"
	insertSQL := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow %s;", table, json)
	r.ch.Begin()
	stmt, _ := r.ch.Prepare(inputFormatSQL + insertSQL)
	if _, err := stmt.Exec([]driver.Value{}); err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.stmt.Exec")
		return err
	}
	return r.ch.Commit()
}
func (r *ClickhouseRepository) Close() error {
	return r.ch.Close()
}
