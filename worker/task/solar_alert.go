package task

import (
	"fmt"
	"time"

	"dforcepro.com/alert"
	"dforcepro.com/resource/db"
	"zbt.dforcepro.com/doc"
)

type AlertForSolar bool

func (afi AlertForSolar) Enable() bool {
	return bool(afi)
}

func (afi AlertForSolar) Process(rd *doc.RTDoc) error {
	//_di.Log.Debug("AlertForSolar Process")
	//fmt.Println("AlertForSolar Process")
	objID := doc.GetStationDocObjectId(rd.StationID)
	//fmt.Println(objID)
	invAlertConf, err := doc.FindInvAlertConfByID(objID)
	//fmt.Println(invAlertConf, err)

	if err != nil {
		_di.Log.Debug(rd.StationID)
		_di.Log.Debug(err.Error())
		return err
	}
	//return alertList []alert.Alert
	alerts := returnnil()
	//TODO: Add TimeZone UTC +8
	time_ := time.Unix(rd.Timestamp, int64(8))
	hour_ := time_.Hour()
	if *rd.AvgIrr > 0.1 && hour_ > 8 && hour_ < 17 {
		alerts = invAlertConf.Validate(rd)
	}

	//alerts := invAlertConf.Validate(rd)
	if alerts == nil || len(*alerts) < 1 {
		return nil
	}

	rd.HasAlert = true
	_di.Log.Debug("event")
	var notifyAlert []alert.Alert

	for _, alert := range *alerts {

		//&alert ->alert config ,string(objID)->此Station_ID

		fmt.Println(alert)
		if isAlert := countInRedis(&alert, rd.StationID, rd.InverterID); isAlert {

			notifyAlert = append(notifyAlert, alert)
		}
	}

	if len(notifyAlert) == 0 {
		return nil
	}
	_, err = rd.CreateEvent(&notifyAlert)

	return err
	//return nil
}

func countInRedis(al *alert.Alert, StationID string, InverterID int) bool {
	redis := _di.Redis.DB(db.RedisDB_Alert).GetClient()
	//"[%d]:%s", al.AlertType, al.Title
	key := fmt.Sprintf("%s:%d:%s", StationID, InverterID, al.ToKeyStr())
	//Incr 針對key加一 ,Decr 針對key減一
	value, err := redis.Incr(key).Result()
	if err != nil {
		_di.Log.Err(err.Error())

	}
	_di.Log.Debug(fmt.Sprintf("Alert key: %s . Count is %d", key, value))
	//判斷Minute時間裡超過Times的次數 ,時間超過Minute Duration ,key自動被清除
	redis.Expire(key, time.Duration(al.Minute)*time.Minute)
	//次數到達Times 回傳true ,反之回傳false
	return uint8(value) == al.Times
}

func returnnil() *[]alert.Alert {
	return nil

}
