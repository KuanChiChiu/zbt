package worker

import (
	"fmt"

	"zbt.dforcepro.com/doc"
	"zbt.dforcepro.com/worker/task"
)

type SolarCreatedTask interface {
	Process(wd *doc.RTDoc) error
	Enable() bool
}

type SolarCreatedWorker bool

var solarCreatedTasks []SolarCreatedTask

func SetSolarCreatedTasks(tasks ...SolarCreatedTask) {
	solarCreatedTasks = tasks
}

func SolarCreated(objectId ...string) (bool, error) {
	fmt.Println("SolarCreated")
	if len(solarCreatedTasks) == 0 {
		return true, nil
	}
	rtDoc := doc.RTDoc{}

	for _, ID := range objectId {
		//solarDoc := doc.SolarDoc{}

		sc, err := rtDoc.GetByIdRT(ID)

		if sc == nil {
			return false, err
		}

		task.SetDi(WsDi)

		for _, ict := range solarCreatedTasks {
			if !ict.Enable() {
				continue
			}
			err := ict.Process(sc)
			if err != nil {
				WsDi.Log.Err(err.Error())
			}
		}
	}
	return true, nil
}

func (icw SolarCreatedWorker) register(tasks *map[string]interface{}) {
	WsDi.Log.Debug(fmt.Sprintf("start registerTask: %s", doc.TaskSolarCreated))
	(*tasks)[doc.TaskSolarCreated] = SolarCreated
}

func (icw SolarCreatedWorker) enable() bool {
	return bool(icw)
}
