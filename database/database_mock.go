package database

import "github.com/stretchr/testify/mock"

type databaseMock struct {
	mock.Mock
}

func (r *databaseMock) GetSlotLSN(slotName string) (string, error) {
	args := r.Called(slotName)
	return args.Get(0).(string), args.Error(1)
}

func (r *databaseMock) IsAlive() bool {
	return r.Called().Bool(0)
}

func (r *databaseMock) Close() error {
	return r.Called().Error(0)
}
