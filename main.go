package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"dforcepro.com/alert"
	"dforcepro.com/api"
	"dforcepro.com/api/middle"
	"dforcepro.com/common"
	"dforcepro.com/cron"
	"dforcepro.com/resource"
	"dforcepro.com/resource/queue"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/betacraft/yaag/yaag"
	"github.com/gorilla/mux"
	mycron "zbt.dforcepro.com/cron"
	"zbt.dforcepro.com/doc"
	"zbt.dforcepro.com/report"
	"zbt.dforcepro.com/search"
	"zbt.dforcepro.com/worker"
	"zbt.dforcepro.com/worker/task"
)

func main() {
	args := os.Args[1:]
	fmt.Println(args)

	if count := len(args); count == 0 {
		fmt.Println("parameters error.")
		return
	}
	configPath := args[1]

	filename, _ := filepath.Abs(configPath + "config.yml")
	resource.IniConf(filename)
	resource.ConfPath = configPath
	di, err := resource.GetDI()
	if err != nil {
		panic(err)
	}
	defer resource.Close()

	di.Log.StartLog()
	doc.SetDI(di)
	search.Init(di)
	iniTaskServer(di)

	switch usage := args[0]; usage {
	case "api":
		runAPI(di)
	case "job":
		runJob(di)
	case "worker":
		runWorker(di)
	case "check":
		check()
	case "send":
		runSend()
	case "timer":
		runTimer(di)
	case "cron":
		runCron(di)
	default:
		fmt.Println("parameters error. only api or job")
	}

}

func runAPI(di *resource.Di) {
	router := mux.NewRouter()

	yaag.Init(&yaag.Config{On: true, DocTitle: "Gorilla Mux", DocPath: "apidoc.html"})
	middleConf := di.APIConf.Middle
	middle.SetDI(di)
	middlewares := middle.GetMiddlewares(
		middle.DebugMiddle(middleConf.Debug),
		middle.GenDocMiddle(middleConf.GenDoc),
		middle.AuthMiddle(middleConf.Auth),
		middle.LogMiddle(middleConf.Log),
	)
	apiConf := &api.APIconf{Router: router, MiddleWares: middlewares}

	common.SetDI(di)
	report.SetDI(di)
	api.InitAPI(apiConf, common.UserAPI(true), doc.SolarAPI(true), doc.SolarConfAPI(true), doc.StationAPI(true), doc.MaintainAPI(true), doc.EventAPI(true), report.SolarReportAPI(true))

	alert.InitTpl(resource.ConfPath + "alert_conf_tpl.yml")

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", di.APIConf.Port), router))
}

func runJob(di *resource.Di) {
	fmt.Println("runJob")
}

func iniTaskServer(di *resource.Di) {
	// Register tasks
	di.Log.Debug("start iniTaskServer")
	worker.WsDi = di
	worker.SetSolarCreatedTasks(task.GoToSearchForSolar(true), task.AlertForSolar(true))
	tasks := worker.GetRegister(worker.SolarCreatedWorker(true))
	err := queue.GetTaskServer(di).RegisterTasks(*tasks)
	if err != nil {
		fmt.Println(err.Error())
	}

}

func runWorker(di *resource.Di) {
	server := queue.GetTaskServer(di)
	worker := server.NewWorker("zbt_worker", 10)
	err := worker.Launch()
	if err != nil {
		// do something with the error
		fmt.Println(err.Error())
	}
	// common.SetDI(di)
	// mycron.SetDI(di)
	//mycron.Update_ACPRT_T0072801()
	//mycron.Update_ACPRT_T0510104()
	//mycron.Update_ACPRT_T0510801()
	//mycron.Update_ACPRT_PT000001()
	// mycron.Update_ACPRT_T0531702()
	// mycron.Update_ACPRT_T0510602()
	// mycron.Update_ACPRT_T0510604()

	//mycron.UpdatePreData_T0072801()
	//mycron.UpdatePreData_PT000001()
	///mycron.UpdatePreData_T0510104()
	//mycron.UpdatePreData_T0510801()
	// mycron.UpdatePreData_T0531702()
	// mycron.UpdatePreData_T0510602()
	// mycron.UpdatePreData_T0510604()
}

func check() {
	fmt.Println(queue.TaskServer.GetRegisteredTaskNames())
}

func runSend() {

	args := []tasks.Arg{
		{
			Type:  "string",
			Value: "aaaa",
		},
	}

	queue.SendTask(doc.TaskSolarCreated, args)

}

func runTimer(di *resource.Di) {

	common.SetDI(di)
	mycron.SetDI(di)
	report.SetDI(di)
	//var cron mycron.ReportCron
	//mycron.UpdateIrrRealTime()
	//cron.StationDailyReport()
	// fmt.Println("Timer")
	// cron := mycron.GesCreateCron
	// err := cron.RefreshOverview()
	//report.AddDatetime()
	// err := rr.DailyReport("T0510602")
	// fmt.Println("err", err)
}

func runCron(di *resource.Di) {
	fmt.Println("ronCron")
	mycron.SetDI(di)
	cron.StartCronJob(mycron.GesCreateCron(true), mycron.ReportCron(true), mycron.Weather(true))
}

func runCronReport(di *resource.Di) {
	common.SetDI(di)
	report.SetDI(di)
	doc.SetDI(di)
	err := mycron.FormReportCSV()
	if err != nil {
		fmt.Println(err)
	}
}
