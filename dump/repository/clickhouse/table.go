package clickhouse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/teamlint/pg-flow/database"
	"github.com/teamlint/pg-flow/wal"
)

// TablePGColumns returns postgresql table's columns structure
func TablePGColumns(tx *pgx.Tx, schema string, table string) ([]wal.RelationColumn, map[string]database.Column, error) {
	walColumns := make([]wal.RelationColumn, 0)
	pgColumns := make(map[string]database.Column)

	rows, err := tx.Query(`select
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
where
  n.nspname = $1
  and c.relname = $2
  and a.attnum > 0
  and a.attisdropped = false
order by
  a.attnum`, schema, table)

	if err != nil {
		return nil, nil, fmt.Errorf("could not query: %v", err)
	}

	for rows.Next() {
		var (
			colName   string
			baseType  string
			pgColumn  database.Column
			extStr    []string
			attTypMod int32
			attOID    int32
		)

		if err := rows.Scan(&colName, &pgColumn.IsNullable, &baseType, &extStr, &pgColumn.PKNum, &attTypMod, &attOID); err != nil {
			return nil, nil, errors.Errorf("could not scan: %v", err)
		}

		if baseType[len(baseType)-2:] == "[]" {
			pgColumn.IsArray = true
			pgColumn.BaseType = baseType[:len(baseType)-2]
		} else {
			pgColumn.BaseType = baseType
		}

		if extStr != nil {
			pgColumn.Ext, err = strToIntArray(extStr)
			if err != nil {
				return nil, nil, errors.Errorf("could not convert into int array: %v", err)
			}
		}

		walColumns = append(walColumns, wal.RelationColumn{
			Key:          pgColumn.PKNum > 0,
			Name:         colName,
			TypeID:       attOID,
			ModifierType: attTypMod,
		})
		pgColumns[colName] = pgColumn
	}

	return walColumns, pgColumns, nil
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

func parseCHType(chType string) (col database.ColumnAttr) {
	if strings.HasPrefix(chType, "LowCardinality(") {
		chType = chType[15 : len(chType)-1]
	}

	col = database.ColumnAttr{BaseType: chType, IsArray: false, IsNullable: false}

	if ln := len(chType); ln >= 7 {
		if strings.HasPrefix(chType, "Array(Nullable(") {
			col = database.ColumnAttr{BaseType: chType[15 : ln-2], IsArray: true, IsNullable: true}
		} else if strings.HasPrefix(chType, "Array(") {
			col = database.ColumnAttr{BaseType: chType[6 : ln-1], IsArray: true, IsNullable: false}
		} else if strings.HasPrefix(chType, "Nullable(") {
			col = database.ColumnAttr{BaseType: chType[9 : ln-1], IsArray: false, IsNullable: true}
		}
	}

	if strings.HasPrefix(col.BaseType, "FixedString(") {
		col.BaseType = "FixedString"
	}

	if strings.HasPrefix(col.BaseType, "Decimal(") {
		col.BaseType = "Decimal"
	}

	return
}
