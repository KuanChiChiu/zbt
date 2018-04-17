package report

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"
	"zbt.dforcepro.com/doc"
	"zbt.dforcepro.com/search"
)

type RegularReport struct {
	doc *interface{}
}

const (
	zbtDb      = "zbt"
	StationS   = "Station"
	RT         = "RealTime"
	HourTbl    = "Hour"
	DailyTbl   = "Day"
	MonthlyTbl = "Month"
)

func (rr RegularReport) HourlyReport(stationID string) error {

	var err error
	//var hResult *[]search.SearchResult
	//var hResult []interface{}
	ss := search.SolarSearch{}
	RTDoc := doc.RTDoc{}
	hourlyDoc := search.HourlyDoc{}
	mongo := doc.GetMongo()

	//-timestamp 取最新時間資料  ,timestamp 取最舊時間資料
	err = mongo.DB(zbtDb).C(HourTbl).Find(bson.M{"stationid": stationID}).Sort("-timestamp").Limit(1).One(&hourlyDoc)
	var Hours = 1
	var startTime int64
	var endTime time.Time
	if err != nil {
		_di.Log.Err(err.Error())
		err = getMongo().DB(zbtDb).C(RT).Find(bson.M{"stationid": stationID}).Sort("timestamp").Limit(1).One(&RTDoc)
		fmt.Println("RTDoc", RTDoc)
		fmt.Println(err)
		if err != nil {
			_di.Log.Err(err.Error())
			return err
		}
		startTime = RTDoc.Timestamp
		//var Hours = 1
		endTime = time.Unix(RTDoc.Timestamp, 0).Add(time.Hour * time.Duration(Hours))
		fmt.Println("startTime1", startTime)
		fmt.Println("endTime1", endTime)
		fmt.Println("endTime1 Unix", endTime.Unix())
	} else {
		startTime = hourlyDoc.Timestamp
		//var Hours = 1
		endTime = time.Unix(hourlyDoc.Timestamp, 0).Add(time.Hour * time.Duration(Hours))
		fmt.Println("startTime2", startTime)
		fmt.Println("endTime2", endTime)
		fmt.Println("endTime2 Unix", endTime.Unix())
	}

	hResult, err := ss.StaticHourReport(stationID, startTime, endTime.Unix())
	if err != nil {
		_di.Log.Err(err.Error())

		return err
	}
	//fmt.Println("*hResult value", *hResult)
	fmt.Println("hResult addr", hResult)
	//fmt.Println("hdata.ID", hdata.ID)
	for _, hdata := range *hResult {
		fmt.Println("hdata.ID", hdata.ID)
		err = getMongo().DB(zbtDb).C(HourTbl).Upsert(bson.M{"_id": hdata.ID}, hdata)
		if err != nil {
			_di.Log.Err(err.Error())

			return err
		}
	}
	return nil
}

/*
func (rr RegularReport) DailyReport(stationID string) error {

	var err error
	//var hResult *[]search.SearchResult
	//var hResult []interface{}
	ss := search.SolarSearch{}
	RTDoc := doc.RTDoc{}
	hourlyDoc := search.HourlyDoc{}
	mongo := doc.GetMongo()

	//-timestamp 取最新時間資料  ,timestamp 取最舊時間資料
	err = mongo.DB(zbtDb).C(HourTbl).Find(bson.M{"stationid": stationID}).Sort("-timestamp").Limit(1).One(&hourlyDoc)
	var Hours = 1
	var startTime int64
	var endTime time.Time
	if err != nil {
		_di.Log.Err(err.Error())
		err = getMongo().DB(zbtDb).C(RT).Find(bson.M{"stationid": stationID}).Sort("timestamp").Limit(1).One(&RTDoc)
		fmt.Println("RTDoc", RTDoc)
		fmt.Println(err)
		if err != nil {
			_di.Log.Err(err.Error())
			return err
		}
		startTime = RTDoc.Timestamp
		//var Hours = 1
		endTime = time.Unix(RTDoc.Timestamp, 0).Add(time.Hour * time.Duration(Hours))
		fmt.Println("startTime1", startTime)
		fmt.Println("endTime1", endTime)
		fmt.Println("endTime1 Unix", endTime.Unix())
	} else {
		startTime = hourlyDoc.Timestamp
		//var Hours = 1
		endTime = time.Unix(hourlyDoc.Timestamp, 0).Add(time.Hour * time.Duration(Hours))
		fmt.Println("startTime2", startTime)
		fmt.Println("endTime2", endTime)
		fmt.Println("endTime2 Unix", endTime.Unix())
	}

	dResult, err := ss.StaticDailyReport(stationID, startTime, endTime.Unix())
	if err != nil {
		_di.Log.Err(err.Error())

		return err
	}
	fmt.Println("*hResult value", *dResult)
	fmt.Println("hResult addr", dResult)

	for _, ddata := range *dResult {
		err = getMongo().DB(zbtDb).C(DailyTbl).Upsert(ddata.ID, ddata)
		if err != nil {
			_di.Log.Err(err.Error())

			return err
		}
	}
	return nil
}
*/
