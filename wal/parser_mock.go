package wal

import (
	"time"

	"github.com/stretchr/testify/mock"
)

type parserMock struct {
	mock.Mock
}

func (p *parserMock) ParseMessage(msg []byte, tx *Transaction) error {
	args := p.Called(msg, tx)
	now := time.Now()
	tx.BeginTime = &now
	tx.CommitTime = &now
	tx.Actions = []ActionData{
		{
			Schema: "public",
			Table:  "users",
			Kind:   "INSERT",
			Columns: []Column{
				{
					name:      "id",
					value:     1,
					valueType: 23,
					isKey:     true,
				},
			},
		},
	}
	return args.Error(0)
}
