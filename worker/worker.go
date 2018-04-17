package worker

import (
	"dforcepro.com/resource"
)

type Worker interface {
	register(tasks *map[string]interface{})
	enable() bool
}

var (
	WsDi *resource.Di
)

func GetRegister(workers ...Worker) *map[string]interface{} {
	taskMap := make(map[string]interface{})
	for _, t := range workers {
		if t.enable() {
			t.register(&taskMap)
		}
	}
	return &taskMap
}
