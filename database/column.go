package database

// ColumnAttr 列属性
type ColumnAttr struct {
	Name         string // 列名
	BaseType     string // 数据类型
	IsKey        bool   // 是否主键
	IsArray      bool   // 是否数组
	IsNullable   bool   // 是否可空
	Ext          []int  // 精度设置
	TypeID       int32  // 列类型ID
	ModifierType int32
}

type Column struct {
	ColumnAttr
	PKNum int // 主键数量
}
