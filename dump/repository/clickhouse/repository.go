package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/teamlint/pg-flow/config"
	"github.com/teamlint/pg-flow/database"
)

var (
	// "2006-01-02 15:04:05",
	TimestampRegexp = regexp.MustCompile(`\d+-\d+-\d+ \d+:\d+:\d+(,\d+)?`)
	// SQL Timestamp
	DBTimestamps = []string{"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999",
		"2006-01-02",
	}
)

const (
	DBNull = "NULL"
)

type ClickhouseRepository struct {
	cfg  *config.Config
	db   database.Database
	ch   clickhouse.Clickhouse
	chdb *sql.DB
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
	// sql.DB
	chdb, err := clickhouseDBConnect(cfg.Dumper.Repository)
	if err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.clickhouseDBConnect")
		return nil, err
	}

	if err := chdb.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logrus.WithField("coce", exception.Code).WithField("stack", exception.StackTrace).Error(exception.Message)
			return nil, err
		} else {
			logrus.WithError(err).Error("clickhouseRepository.clickhouseDBConnect.ping")
			return nil, err
		}
	}
	repo.chdb = chdb

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

func clickhouseDBConnect(cfg config.RepositoryCfg) (*sql.DB, error) {
	conn, err := sql.Open("clickhouse", connectionString(cfg))
	if err != nil {
		logrus.WithError(err).Error("clickhouse.clickhouseDBConnect")
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
			logrus.WithError(err).Errorln("ClickhouseRepository.CreateTables")
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

func (r *ClickhouseRepository) SetInputFormat() error {
	// input format setting
	inputFormatSQL := "SET input_format_import_nested_json=1;"
	r.ch.Begin()
	stmt, _ := r.ch.Prepare(inputFormatSQL)
	if _, err := stmt.Exec([]driver.Value{}); err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.SetInputFormat")
		return err
	}
	return r.ch.Commit()
}
func (r *ClickhouseRepository) InsertData(table string, data map[string]interface{}) error {
	// insert data
	// insertSQL := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow %s;", table, json)
	// sql.DB
	// tx, err := r.chdb.Begin()
	// if err != nil {
	// 	logrus.WithError(err).Errorln("ClickhouseRepository.InsertData")
	// 	return err
	// }
	// if _, err := tx.Exec(insertSQL); err != nil {
	// 	logrus.WithError(err).Errorln("ClickhouseRepository.InsertData")
	// 	return err
	// }
	// return tx.Commit()

	// clickhouse.DB
	var fields []string
	var values []driver.Value
	for k, v := range data {
		// fields
		fields = append(fields, k)
		// values
		// null
		// if v == nil || v.(string) == DBNull {
		logrus.Debugf("%s(%T) field is %v", k, v, v)
		if v == nil {
			v = nil
		} else {
			// datetime
			switch t := v.(type) {
			case string:
				if found := TimestampRegexp.FindString(v.(string)); len(found) > 0 {
					// v = fmt.Sprintf("parseDateTimeBestEffortOrNull('%s')", v)
					logrus.Debugf("TimestampRegexp found=%v", found)
					t, err := parseTimestamp(v.(string))
					if err != nil {
						logrus.WithError(err).Error("timestamp parse")
					}
					v = t
				}
			default:
				v = t
			}
		}
		values = append(values, v)
	}
	// insert statement
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(fields, ", "))
	sb.WriteString(")")
	sb.WriteString(" VALUES (")
	l := len(fields)
	for i := range fields {
		if i == l-1 {
			sb.WriteString("?")
		} else {
			sb.WriteString("?, ")
		}
	}
	sb.WriteString(")")

	r.ch.Begin()
	stmt, _ := r.ch.Prepare(sb.String())
	// TODO 批处理插入数据
	if _, err := stmt.Exec(values); err != nil {
		logrus.WithError(err).Errorln("ClickhouseRepository.InsertData")
		return err
	}
	return r.ch.Commit()
}
func (r *ClickhouseRepository) Close() error {
	err := errors.Wrap(r.db.Close(), "database close()")
	err = errors.Wrap(r.ch.Close(), "clickhouse close()")
	return err
}
func parseTimestamp(s string) (time.Time, error) {
	var t time.Time
	var err error
	for _, layout := range DBTimestamps {
		t, err = time.Parse(layout, s)
		if err == nil {
			break
		}
	}
	if err != nil {
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), err
	}
	return t, err
}
