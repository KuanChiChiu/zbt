package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestTask bool

func (gts TestTask) enable() bool {
	return bool(gts)
}

func (gts TestTask) register(tasks *map[string]interface{}) {
	(*tasks)["testtask"] = "test"
}

func (tt TestTask) do() {
	return
}

func (tt TestTask) registerTask(tasks *map[string]interface{}) {
	(*tasks)["Test"] = tt.do
}

func Test_GetRegisterTask(t *testing.T) {
	taskMap := GetRegister(TestTask(true))
	assert.Equal(t, 1, len(*taskMap), "must equal")
}
