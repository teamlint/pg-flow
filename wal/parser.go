package wal

// Parser WAL 消息解析器接口
type Parser interface {
	ParseMessage([]byte, *Transaction) error
}
