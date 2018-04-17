package report

import (
	"encoding/json"
	"net/http"
	"time"

	"dforcepro.com/api"
	"dforcepro.com/util"
	"zbt.dforcepro.com/search"
)

type SolarReportAPI bool

const (
	SolarTimeLayout = "2006-01-02T15:04:05"
)

func (sra SolarReportAPI) GetAPIs() *[]*api.APIHandler {
	return &[]*api.APIHandler{
		&api.APIHandler{Path: "/v1/pwr/report", Next: sra.reportEndpoint, Method: "GET", Auth: false},
		//&api.APIHandler{Path: "/v1/pwr/overview", Next: sra.overviewEndpoint, Method: "GET", Auth: false},
		//&api.APIHandler{Path: "/v1/pwr/inv", Next: sra.invEndpoint, Method: "GET", Auth: false},
	}
}

func (sra SolarReportAPI) Enable() bool {
	return bool(sra)
}

func (sra SolarReportAPI) reportEndpoint(w http.ResponseWriter, req *http.Request) {
	queryMap := util.GetQueryValue(req, []string{"stationid", "startTime", "endTime"}, true)
	//reportType := (*queryMap)["type"].(string)
	stationID := (*queryMap)["stationid"].(string)
	startTime := (*queryMap)["startTime"].(string)
	endTime := (*queryMap)["endTime"].(string)
	/*if !util.IsStrInList(reportType, []string{"day", "month", "year", "life"}) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}*/
	if stationID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if startTime == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if endTime == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var err error
	var searchResult *[]search.SearchResult
	ss := search.SolarSearch{}
	/*switch reportType {
	case "day":
		searchResult, err = ws.StaticDay(gwID, startTime)
	case "month":
		searchResult, err = ws.StaticMonth(gwID, startTime)
	case "year":
		searchResult, err = ws.StaticYear(gwID, startTime)
	case "life":
		searchResult, err = ws.StaticLife(gwID, startTime)
	}*/

	startT, err := time.Parse(SolarTimeLayout, startTime)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	endT, err := time.Parse(SolarTimeLayout, endTime)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	searchResult, err = ss.StaticHour(stationID, startT.Unix(), endT.Unix())
	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	jsonByte, _ := json.Marshal(searchResult)
	w.Write(jsonByte)
}

//FIXME: 永旺總攬需要完成
/*
func (sra SolarReportAPI) overviewEndpoint(w http.ResponseWriter, req *http.Request) {
	queryMap := util.GetQueryValue(req, []string{"stationid", "startTime", "endTime"})
	//reportType := (*queryMap)["type"].(string)
	stationID := (*queryMap)["stationid"].(string)
	startTime := (*queryMap)["startTime"].(string)
	endTime := (*queryMap)["endTime"].(string)

	if stationID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if startTime == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if endTime == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var err error
	var searchResult *[]search.SearchResult
	ss := search.SolarSearch{}

	startT, err := time.Parse(SolarTimeLayout, startTime)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	endT, err := time.Parse(SolarTimeLayout, endTime)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	searchResult, err = ss.StaticHour(stationID, startT.Unix(), endT.Unix())
	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	jsonByte, _ := json.Marshal(searchResult)
	w.Write(jsonByte)
}
*/

//FIXME: 永旺inv需要完成
/*
func (sra SolarReportAPI) invEndpoint(w http.ResponseWriter, req *http.Request) {
	queryMap := util.GetQueryValue(req, []string{"stationid"})
	//reportType := (*queryMap)["type"].(string)
	stationID := (*queryMap)["stationid"].(string)

	if stationID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var err error
	var searchResult *[]search.SearchResult
	ss := search.SolarSearch{}

	searchResult, err = ss.RTInverter(stationID)
	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	jsonByte, _ := json.Marshal(searchResult)
	w.Write(jsonByte)
}
*/
