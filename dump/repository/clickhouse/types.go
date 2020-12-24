package clickhouse

import (
	"fmt"

	"github.com/teamlint/pg-flow/database"
)

const (
	Int8        = "Int8"
	Int16       = "Int16"
	Int32       = "Int32"
	Int64       = "Int64"
	UInt8       = "UInt8"
	UInt16      = "UInt16"
	Uint32      = "UInt32"
	Uint64      = "UInt64"
	Float32     = "Float32"
	Float64     = "Float64"
	FixedString = "FixedString"
	String      = "String"
	Date        = "Date"
	DateTime    = "DateTime"
	DateTime64  = "DateTime64"
	Decimal     = "Decimal"
	UUID        = "UUID"
	UInt8Array  = "Array(UInt8)"
)

var PG2CHMap = map[string]string{
	database.SmallInt:                 Int16,
	database.Integer:                  Int32,
	database.BigInt:                   Int64,
	database.CharacterVarying:         String,
	database.Varchar:                  String,
	database.Text:                     String,
	database.Real:                     Float32,
	database.DoublePrecision:          Float64,
	database.Interval:                 Int32,
	database.Boolean:                  UInt8,
	database.Decimal:                  Decimal,
	database.Numeric:                  Decimal,
	database.Character:                FixedString,
	database.Char:                     FixedString,
	database.JSONB:                    String,
	database.JSON:                     String,
	database.UUID:                     UUID,
	database.Bytea:                    UInt8Array,
	database.Inet:                     Int64,
	database.Timestamp:                DateTime64,
	database.TimestampWithTimeZone:    DateTime64,
	database.TimestampWithoutTimeZone: DateTime64,
	database.Date:                     Date,
	database.Time:                     Uint32,
	database.TimeWithoutTimeZone:      Uint32,
	database.TimeWithTimeZone:         Uint32,
}

// PG2CHType converts pg type into clickhouse type
func PG2CHType(pgColumn database.Column) (string, error) {
	chType, ok := PG2CHMap[pgColumn.BaseType]
	if !ok {
		chType = String
	}

	switch pgColumn.BaseType {
	case database.Decimal:
		fallthrough
	case database.Numeric:
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("precision must be specified for the numeric type")
		}
		chType = fmt.Sprintf("%s(%d, %d)", chType, pgColumn.Ext[0], pgColumn.Ext[1])
	case database.Character:
		fallthrough
	case database.Char:
		if pgColumn.Ext == nil {
			return "", fmt.Errorf("length must be specified for character type")
		}
		chType = fmt.Sprintf("%s(%d)", chType, pgColumn.Ext[0])
	}

	if pgColumn.IsArray {
		chType = fmt.Sprintf("Array(%s)", chType)
	}

	if pgColumn.IsNullable && !pgColumn.IsArray {
		chType = fmt.Sprintf("Nullable(%s)", chType)
	}

	return chType, nil
}
