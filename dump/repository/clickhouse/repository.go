package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/database"
)

type ClickhouseRepository struct {
	cfg *config.Config
	db  database.Database
	ch  clickhouse.Clickhouse
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
	// repo.pgconn = pgconn
	db := database.New(pgconn)
	repo.db = db
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
func postgresConnect(cfg config.DatabaseCfg) (*pgx.Conn, error) {
	pgxConf := pgx.ConnConfig{
		// LogLevel: pgx.LogLevelInfo,
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Name,
		User:     cfg.User,
		Password: cfg.Password,
	}

	// conn, err := pgx.ReplicationConnect(pgxConf)
	conn, err := pgx.Connect(pgxConf)
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
func (r *ClickhouseRepository) generateDDL(databaseName string, schemaName string, tblName string) (string, error) {
	// for tblName, _ := range r.cfg.Database.Filter.Tables {
	var (
		pkNum        int
		engineParams string
		orderBy      string
	)

	pgColumns, err := r.db.GetTableColumns(schemaName, tblName)
	if err != nil {
		return "", errors.Wrapf(err, "could not get columns for %s.%s postgres table", schemaName, tblName)
	}

	if len(pgColumns) < 1 {
		return "", errors.Wrapf(err, "could not get columns for %s.%s postgres table", schemaName, tblName)
	}

	// clickhouse 表配置
	tblCfg, tblOK := r.cfg.Dumper.Repository.Tables[tblName]
	chColumnDDLs := make([]string, 0)

	for _, pgCol := range pgColumns {
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

		chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s %s", pgCol.Name, chColType))
	}
	// 主键列
	pkColumns := make([]string, pkNum)

	for _, pgCol := range pgColumns {
		if pgCol.PKNum < 1 {
			continue
		}

		pkColumns[pgCol.PKNum-1] = pgCol.Name
	}

	param := tblCfg["generationcolumn"]
	if param != "" {
		chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt32", param))
	}

	// 表引擎
	tableEngine := tblCfg["engine"]
	if tblOK {
		// chColumnDDLs 是否重复添加关键列?
		switch tableEngine {
		case MergeTree:
		case ReplacingMergeTree:
			engineParams = tblCfg["vercolumn"]
			// chColumnDDLs = append(chColumnDDLs, fmt.Sprintf("    %s UInt8", tblCfg["deletedcolumn"])
		case CollapsingMergeTree:
			engineParams = tblCfg["signcolumn"]
			if engineParams == "" {
				return "", errors.Errorf("%s engine: signColumn must be set", CollapsingMergeTree)
			}
		case VersionedCollapsingMergeTree:
			engineParams = tblCfg["signcolumn"]
			if engineParams == "" {
				return "", errors.Errorf("%s engine: signColumn must be set", VersionedCollapsingMergeTree)
			}
			verParams := tblCfg["vercolumn"]
			if verParams == "" {
				return "", errors.Errorf("%s engine: verColumn must be set", VersionedCollapsingMergeTree)
			}
			engineParams += verParams
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
	tableDDL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n%s\n)\nEngine = %s(%s)",
		databaseName,
		tblName,
		strings.Join(chColumnDDLs, ",\n"),
		tableEngine, engineParams)

	// PARTITION BY
	if partitionBy := tblCfg["partitionby"]; partitionBy != "" {
		tableDDL += fmt.Sprintf("\nPARTITION BY %s", partitionBy)
	}
	// ORDER BY
	if tblCfg["orderby"] == "" {
		if len(pkColumns) > 0 {
			orderBy = fmt.Sprintf("\nORDER BY (%s)", strings.Join(pkColumns, ", "))
		}
	} else {
		orderBy = fmt.Sprintf("\nORDER BY (%s)", tblCfg["orderby"])
	}
	tableDDL += orderBy
	// PRIMARY KEY
	if primaryKey := tblCfg["primarykey"]; primaryKey != "" {
		tableDDL += fmt.Sprintf("\nPRIMARY KEY %s", primaryKey)
	}
	// SAMPLE BY
	if sampleBy := tblCfg["sampleby"]; sampleBy != "" {
		tableDDL += fmt.Sprintf("\nSAMPLE BY %s", sampleBy)
	}
	// SETTINGS
	if settings := tblCfg["settings"]; settings != "" {
		tableDDL += fmt.Sprintf("\nSETTINGS %s", settings)
	}
	tableDDL += ";"
	// }

	return tableDDL, nil
}
func (r *ClickhouseRepository) CreateTables() error {
	databaseName := r.cfg.Dumper.Repository.Name
	if databaseName == "" {
		databaseName = "default"
	}
	schema := r.cfg.Database.Filter.Schema

	// 遍历表
	for tblName, _ := range r.cfg.Database.Filter.Tables {
		// generate DDL
		ddl, err := r.generateDDL(databaseName, schema, tblName)
		if err != nil {
			logrus.WithError(err).Errorln("ClickhouseRepository.generateDDL")
			return err
		}

		r.ch.Begin()
		stmt, _ := r.ch.Prepare(ddl)
		if _, err = stmt.Exec([]driver.Value{}); err != nil {
			logrus.WithError(err).Errorln("ClickhouseRepository.stmt.Exec")
			return err
		}
	}
	if err := r.ch.Commit(); err != nil {
		logrus.WithField("database", databaseName).WithError(err).Error("tables create error")
		return err
	}
	logrus.WithField("database", databaseName).Info("tables is created")
	return nil
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
	err := errors.Wrap(r.db.Close(), "database close()")
	err = errors.Wrap(r.ch.Close(), "clickhouse close()")
	return err
}
