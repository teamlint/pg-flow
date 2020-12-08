package wal

import "errors"

var (
	ErrMessageLost        = errors.New("messages are lost")
	ErrEmptyMessage       = errors.New("empty WAL message")
	ErrUnknownMessageType = errors.New("unknown message type")
)
