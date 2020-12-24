package database

type ColumnAttr struct {
	BaseType   string
	IsArray    bool
	IsNullable bool
	Ext        []int
}

type Column struct {
	ColumnAttr
	PKNum int // 主键数量
}
