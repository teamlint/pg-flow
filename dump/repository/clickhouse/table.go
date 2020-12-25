package clickhouse

import (
	"strings"

	"github.com/teamlint/pg-flow/database"
)

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
