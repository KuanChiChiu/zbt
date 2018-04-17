package cron

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"time"

	"dforcepro.com/cron"
	"gopkg.in/mgo.v2/bson"
	"zbt.dforcepro.com/doc"
	//"golang.org/x/net/context/ctxhttp"
)

type ReportCron bool

func (mycron ReportCron) Enable() bool {
	return bool(mycron)
}

func (mycron ReportCron) GetJobs() []cron.JobSpec {
	return []cron.JobSpec{
		cron.JobSpec{
			Spec: "0 0 * * * *",
			Job:  mycron.StationHourlyReport,
		},
		cron.JobSpec{
			Spec: "0 0 * * * *",
			Job:  mycron.StationDailyReport,
		},
		// cron.JobSpec{
		// 	Spec: "0 0 0 * * *",
		// 	Job:  mycron.StationMonthlyReport,
		// },
		// cron.JobSpec{
		// 	Spec: "0 0 0 * * *",
		// 	Job:  mycron.StationYearlyReport,
		// },
	}
}

const (
	HourTbl             = "Hour"
	DailyTbl            = "Day"
	MonthlyTbl          = "Month"
	YearlyTbl           = "Year"
	RedisDB_StationConf = 5
)

type HourlyDoc struct {
	//Timestamp  int64     `json:"Timestamp,omitempty"` // 數據採集時間
	ID                 bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Datetime           time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	Datestr            string        `json:"Datestr,omitempty"`       // 數據採集時間
	Hour               int           `json:"Hour,omitempty"`
	StationID          string        `json:"Station_ID,omitempty"` //案場編號
	InverterID         int           `json:"InverterID,omitempty"` // 逆變器編號
	DCP                *float64      `json:"DCP,omitempty"`
	DCPCheckNil        *float64      `json:"DCPCheckNil,omitempty"`
	ACP                *float64      `json:"ACP,omitempty"`
	ACPCheckNil        *float64      `json:"ACPCheckNil ,omitempty"`
	ACPHours           *float64      `json:"ACP_Hours,omitempty"`
	ACPHoursCheckNil   *float64      `json:"ACP_Hours_CheckNil,omitempty"`
	ACPDaily           *float64      `json:"ACP_Daily,omitempty"`
	ACPDailyCheckNil   *float64      `json:"ACP_DailyCheckNil,omitempty"`
	ACPLife            *float64      `json:"ACP_Life,omitempty"`
	ACPLifeCheckNil    *float64      `json:"ACP_LifeCheckNil,omitempty"`
	Freq               *float64      `json:"Freq,omitempty"`
	FreqCheckNil       *float64      `json:"FreqCheckNil,omitempty"`
	InvTemp            *float64      `json:"InvTemp,omitempty"`
	InvTempCheckNil    *float64      `json:"InvTempCheckNil,omitempty"`
	PVTemp             *float64      `json:"PVTemp,omitempty"`
	PVTempCheckNil     *float64      `json:"PVTempCheckNil,omitempty"`
	Irr                *float64      `json:"Irr,omitempty"`
	IrrCheckNil        *float64      `json:"IrrCheckNil,omitempty"`
	InvEff             *float64      `json:"Inv_Eff,omitempty"`
	PR                 *float64      `json:"PR,omitempty"`
	IrrDurTimeCheckNil *float64      `json:"IrrDurTimeCheckNil,omitempty"`
	IrrDurTime         *float64      `json:"IrrDurTime,omitempty"`
	ACPDurTimeCheckNil *float64      `json:"ACPDurTimeCheckNil,omitempty"`
	ACPDurTime         *float64      `json:"ACPDurTime,omitempty"`
	CapCheckNil        *float64      `json:"CapacityCheckNil,omitempty"`
	Cap                *float64      `json:"Capacity,omitempty"`
	IrrHoursArea       *float64      `json:"IrrHoursArea,omitempty"` //日射面積
	IrrHours           *float64      `json:"IrrHours,omitempty"`     //日射面積除時間
	//HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type HourlyDoc_ struct {
	//Timestamp  int64     `json:"Timestamp,omitempty"` // 數據採集時間
	ID           bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Datetime     time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	Datestr      string        `json:"Datestr,omitempty"`       // 數據採集時間
	Hour         int           `json:"Hour,omitempty"`
	StationID    string        `json:"Station_ID,omitempty"` //案場編號
	InverterID   int           `json:"InverterID,omitempty"` // 逆變器編號
	DCP          *float64      `json:"DCP,omitempty"`
	ACP          *float64      `json:"ACP,omitempty"`
	ACPHours     *float64      `json:"ACP_Hours,omitempty"`
	ACPDaily     *float64      `json:"ACP_Daily,omitempty"`
	ACPLife      *float64      `json:"ACP_Life,omitempty"`
	Freq         *float64      `json:"Freq,omitempty"`
	InvTemp      *float64      `json:"InvTemp,omitempty"`
	PVTemp       *float64      `json:"PVTemp,omitempty"`
	Irr          *float64      `json:"Irr,omitempty"`
	InvEff       *float64      `json:"Inv_Eff,omitempty"`
	PR           *float64      `json:"PR,omitempty"`
	IrrDurTime   *float64      `json:"IrrDurTime,omitempty"`
	ACPDurTime   *float64      `json:"ACPDurTime,omitempty"`
	Cap          *float64      `json:"Capacity,omitempty"`
	IrrHours     *float64      `json:"IrrHours,omitempty"`
	IrrHoursArea *float64      `json:"IrrHoursArea,omitempty"` //日射面積
	//HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type DailyDoc struct {
	ID                 bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Datetime           time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	Datestr            string        `json:"Datestr,omitempty"`       // 數據採集時間
	Day                int           `json:"Day,omitempty"`
	StationID          string        `json:"Station_ID,omitempty"` //案場編號
	InverterID         int           `json:"InverterID,omitempty"` // 逆變器編號
	ACPDaily           *float64      `json:"ACP_Daily,omitempty"`
	ACPLife            *float64      `json:"ACP_Life,omitempty"`
	Freq               *float64      `json:"Freq,omitempty"`
	FreqCheckNil       *float64      `json:"Freq,omitempty"`
	InvTemp            *float64      `json:"InvTemp,omitempty"`
	InvTempCheckNil    *float64      `json:"InvTemp,omitempty"`
	PVTemp             *float64      `json:"PVTemp,omitempty"`
	PVTempCheckNil     *float64      `json:"PVTemp,omitempty"`
	Irr                *float64      `json:"Irr,omitempty"`
	IrrCheckNil        *float64      `json:"Irr,omitempty"`
	InvEff             *float64      `json:"Inv_Eff,omitempty"`
	InvEffCheckNil     *float64      `json:"Inv_Eff,omitempty"`
	PR                 *float64      `json:"PR,omitempty"`
	APR                *float64      `json:"APR,omitempty"`
	KKp                *float64      `json:"kWh_kWp,omitempty"`
	IrrDurTime         *float64      `json:"IrrDurTime,omitempty"`
	IrrDurTimeCheckNil *float64      `json:"IrrDurTime,omitempty"`
	ACPDurTime         *float64      `json:"ACPDurTime,omitempty"`
	ACPDurTimeCheckNil *float64      `json:"ACPDurTime,omitempty"`
	IrrACPTime         *float64      `json:"IrrACPTime,omitempty"`
	Cap                *float64      `json:"Capacity,omitempty"`
	CapCheckNil        *float64      `json:"Capacity,omitempty"`
	IrrDailyArea       *float64      `json:"IrrDailyArea,omitempty"` //日射面積
	IrrDaily           *float64      `json:"IrrDaily,omitempty"`     //日射面積除時間
	//HasAlert           bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type DailyDoc_ struct {
	ID           bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Datetime     time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	Datestr      string        `json:"Datestr,omitempty"`       // 數據採集時間
	Day          int           `json:"Day,omitempty"`
	StationID    string        `json:"Station_ID,omitempty"` //案場編號
	InverterID   int           `json:"InverterID,omitempty"` // 逆變器編號
	ACPDaily     *float64      `json:"ACP_Daily,omitempty"`
	ACPLife      *float64      `json:"ACP_Life,omitempty"`
	Freq         *float64      `json:"Freq,omitempty"`
	InvTemp      *float64      `json:"InvTemp,omitempty"`
	PVTemp       *float64      `json:"PVTemp,omitempty"`
	Irr          *float64      `json:"Irr,omitempty"`
	InvEff       *float64      `json:"Inv_Eff,omitempty"`
	PR           *float64      `json:"PR,omitempty"`
	APR          *float64      `json:"APR,omitempty"`
	KKp          *float64      `json:"kWh_kWp,omitempty"`
	IrrDurTime   *float64      `json:"IrrDurTime,omitempty"`
	ACPDurTime   *float64      `json:"ACPDurTime,omitempty"`
	IrrACPTime   *float64      `json:"IrrACPTime,omitempty"`
	Cap          *float64      `json:"Capacity,omitempty"`
	IrrDailyArea *float64      `json:"IrrDailyArea,omitempty"` //日射面積
	IrrDaily     *float64      `json:"IrrDaily,omitempty"`     //日射面積除時間
	//HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type MonthlyDoc struct {
	ID         bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Datetime   time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	Datestr    string        `json:"Datestr,omitempty"`       // 數據採集時間
	Month      int           `json:"Month,omitempty"`
	StationID  string        `json:"Station_ID,omitempty"` //案場編號
	InverterID int           `json:"InverterID,omitempty"` // 逆變器編號
	ACPMonth   *float64      `json:"ACP_Month,omitempty"`
	ACPLife    *float64      `json:"ACP_Life,omitempty"`
	Freq       *float64      `json:"Freq,omitempty"`
	InvTemp    *float64      `json:"InvTemp,omitempty"`
	PVTemp     *float64      `json:"PVTemp,omitempty"`
	Irr        *float64      `json:"Irr,omitempty"`
	InvEff     *float64      `json:"Inv_Eff,omitempty"`
	PR         *float64      `json:"PR,omitempty"`
	APR        *float64      `json:"APR,omitempty"`
	KKp        *float64      `json:"kWh_kWp,omitempty"`
	Cap        *float64      `json:"Capacity,omitempty"`
	HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type YearlyDoc struct {
	ID         bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Datetime   time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	Datestr    string        `json:"Datestr,omitempty"`       // 數據採集時間
	Year       int           `json:"Year,omitempty"`
	StationID  string        `json:"Station_ID,omitempty"` //案場編號
	InverterID int           `json:"InverterID,omitempty"` // 逆變器編號
	ACPYear    *float64      `json:"ACP_Year,omitempty"`
	ACPLife    *float64      `json:"ACP_Life,omitempty"`
	Freq       *float64      `json:"Freq,omitempty"`
	InvTemp    *float64      `json:"InvTemp,omitempty"`
	PVTemp     *float64      `json:"PVTemp,omitempty"`
	Irr        *float64      `json:"Irr,omitempty"`
	InvEff     *float64      `json:"Inv_Eff,omitempty"`
	PR         *float64      `json:"PR,omitempty"`
	APR        *float64      `json:"APR,omitempty"`
	KKp        *float64      `json:"kWh_kWp,omitempty"`
	Cap        *float64      `json:"Capacity,omitempty"`
	HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

// func UpdateDatetime() {
// 	fmt.Println("UpdateDatetime")
// 	rtdoc := []doc.RTDoc{}
// 	doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"uploadtimestamp": bson.M{
// 		"$lt": 1517565041,
// 	}}).All(&rtdoc)
// 	//fmt.Println("rtdoc", rtdoc)
// 	fmt.Println("aa")
// 	for _, data := range rtdoc {
// 		fmt.Println("data", data.StationID, data.InverterID, data.UploadTimestamp)
// 		err := doc.GetMongo().DB(zbtDb).C(doc.RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"datetime": time.Unix(data.Timestamp, 0).In(time.FixedZone("GMT", 8*3600))}})
// 		if err != nil {
// 			fmt.Println(err)
// 			//_di.Log.Info(err.Error())
// 		}
// 	}

// }

func (w *HourlyDoc) GenObjectId() {
	if bson.ObjectId("") == w.ID {

		w.ID = getRTDocObjectId(w.StationID, w.InverterID, w.Datetime)
	}
}

func getRTDocObjectId(StationID string, InverterID int, Timestamp time.Time) bson.ObjectId {
	var b [12]byte
	// Timestamp, 4 bytes, big endian
	//timestamp := time.Unix(Timestamp, 0)
	binary.BigEndian.PutUint32(b[:], uint32(Timestamp.Unix()))

	var sum [8]byte
	id := sum[:]
	hw := md5.New()
	hw.Write([]byte(StationID))
	copy(id, hw.Sum(nil))
	//ex T0510602 ->id[1]=0 to id[7]=2
	b[4] = id[1]
	b[5] = id[2]
	b[6] = id[3]
	b[7] = id[4]
	b[8] = id[5]
	b[9] = id[6]
	b[10] = id[7]
	b[11] = uint8(InverterID)
	return bson.ObjectId(b[:])
}

func (mycron ReportCron) StationHourlyReport() {
	ovrt := []doc.OverviewRTDoc{}
	err := doc.GetMongo().DB(zbtDb).C(doc.RTOverview).Find(bson.M{}).All(&ovrt)
	if err == nil {
		for _, data := range ovrt {
			fmt.Println("StationID", data.StationID)
			_err := mycron.HourlyReport(data.StationID)
			if _err != nil {
				fmt.Println("Line 267", _err)
				//_di.Log.Err(_err.Error())
			}
		}
	}
}

func (mycron ReportCron) StationDailyReport() {
	ovrt := []doc.OverviewRTDoc{}
	err := doc.GetMongo().DB(zbtDb).C(doc.RTOverview).Find(bson.M{}).All(&ovrt)
	if err == nil {
		for _, data := range ovrt {
			fmt.Println(data.StationID)
			_err := mycron.DailyReport(data.StationID)
			if _err != nil {
				//fmt.Println("line256:", _err)
				//_di.Log.Err(_err.Error())
				_di.Log.Info("line256:" + _err.Error())
			}
		}
	}
}

func (mycron ReportCron) StationMonthlyReport() {
	ovrt := []doc.OverviewRTDoc{}
	err := doc.GetMongo().DB(zbtDb).C(doc.RTOverview).Find(bson.M{}).All(&ovrt)
	if err == nil {
		for _, data := range ovrt {
			_err := mycron.MonthlyReport(data.StationID)
			if _err != nil {
				//fmt.Println("line270:", _err)
				_di.Log.Info("line270:" + _err.Error())
			}
		}
	}
}

func (mycron ReportCron) StationYearlyReport() {
	ovrt := []doc.OverviewRTDoc{}
	err := doc.GetMongo().DB(zbtDb).C(doc.RTOverview).Find(bson.M{}).All(&ovrt)
	if err == nil {
		for _, data := range ovrt {
			_err := mycron.YearlyReport(data.StationID)
			if _err != nil {
				//fmt.Println("line284:", _err)
				_di.Log.Info("line284:" + _err.Error())
			}
		}
	}
}

func (mycron ReportCron) HourlyReport(stationID string) error {

	//var hResult *[]search.SearchResult
	//var hResult []interface{}
	// fmt.Println("HourlyReport")
	RTDoc := doc.RTDoc{}
	hourlyDoc := HourlyDoc{}
	mongo := doc.GetMongo()

	//-timestamp 取最新時間資料  ,timestamp 取最舊時間資料
	err := mongo.DB(zbtDb).C(HourTbl).Find(bson.M{"stationid": stationID}).Sort("-datetime").Limit(1).One(&hourlyDoc)
	//fmt.Println(err)
	//fmt.Println("hourlyDoc", hourlyDoc)
	//var Hours = 720
	var startTime time.Time
	var endTime time.Time
	if err != nil {
		fmt.Println("line308:", err)
		//_di.Log.Err(err.Error())
		err = mongo.DB(zbtDb).C(RT).Find(bson.M{"stationid": stationID}).Sort("datetime").Limit(1).One(&RTDoc)

		fmt.Println("line312:", err)
		if err != nil {
			//_di.Log.Err(err.Error())
			return err
		}
		//startTime = RTDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		startTime = time.Unix(1517414400, 0).In(time.FixedZone("GMT", 8*3600)) //2018-02-02
		//startTime = RTDoc.Datetime.In(time.FixedZone("GMT", 8*3600))

		fmt.Println("H_fir start", startTime)
		endTime = time.Unix(1517500800, 0).In(time.FixedZone("GMT", 8*3600)) //2018-02-02
		//endTime = time.Now().In(time.FixedZone("GMT", 8*3600))
		//endTime = time.Unix(1516942090, 0).In(time.FixedZone("GMT", 8*3600))
		//endTime = startTime.Add(time.Hour * time.Duration(24))
		//endTime := time.Now().In(time.FixedZone("GMT", 8*3600))
		fmt.Println("H_fir end", endTime)
	} else {
		startTime = hourlyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		//startTime = time.Unix(1522252800, 0).In(time.FixedZone("GMT", 8*3600)) //2018-02-02
		fmt.Println("H_ start", startTime)
		//endTime = time.Unix(1522425600, 0).In(time.FixedZone("GMT", 8*3600)) //2018-02-02
		endTime = time.Now().In(time.FixedZone("GMT", 8*3600))
		////endTime = startTime.Add(time.Hour * time.Duration(Hours))
		//endTime = time.Unix(1521201564, 0).In(time.FixedZone("GMT", 8*3600)) //2018-02-02
		fmt.Println("H_ end", endTime)
	}
	//fmt.Println("a")
	hResult, err := mycron.AggHourlyReport(stationID, startTime, endTime)
	//fmt.Println("b")
	if err != nil {
		//_di.Log.Err(err.Error())
		fmt.Println("line368:", err)
		return err
	}
	//fmt.Println("hResult value", hResult)
	//fmt.Println("hdata.ID", hdata.ID)
	for _, data := range *hResult {
		fmt.Println("data", data.Datestr, data.StationID, data.InverterID)
		if data.ACPHoursCheckNil == nil {
			data.ACPHours = nil
			//fmt.Println("acphourschecknil", data.ACPHoursCheckNil)
		}
		if data.DCPCheckNil == nil {
			data.DCP = nil
		}

		if data.ACPCheckNil == nil {
			data.ACP = nil
			//fmt.Println("acphourschecknil", data.ACPHoursCheckNil)
		}
		if data.ACPDailyCheckNil == nil {
			data.ACPDaily = nil
		}

		if data.ACPDurTimeCheckNil == nil {
			data.ACPDurTime = nil
			//fmt.Println("acphourschecknil", data.ACPHoursCheckNil)
		}
		if data.ACPLifeCheckNil == nil {
			data.ACPLife = nil
		}

		if data.CapCheckNil == nil {
			data.Cap = nil
			//fmt.Println("acphourschecknil", data.ACPHoursCheckNil)
		}
		if data.FreqCheckNil == nil {
			data.Freq = nil
		}

		if data.InvTempCheckNil == nil {
			data.InvTemp = nil
			//fmt.Println("acphourschecknil", data.ACPHoursCheckNil)
		}
		if data.IrrCheckNil == nil {
			data.Irr = nil
		}

		if data.IrrDurTimeCheckNil == nil {
			data.IrrDurTime = nil
			//fmt.Println("acphourschecknil", data.ACPHoursCheckNil)
		}
		if data.PVTempCheckNil == nil {
			data.PVTemp = nil
		}

		//fmt.Println("acphourschecknil", data.ACPHoursCheckNil, *data.ACPHoursCheckNil)
		// fmt.Println("StationID", data.StationID)
		// fmt.Println("ACPHours", data.ACPHours, *data.ACPHours)
		if bson.ObjectId("") == data.ID {

			data.ID = getRTDocObjectId(data.StationID, data.InverterID, data.Datetime)
		}
		// fmt.Println("data", data.StationID, data.Datestr)
		if data.Hour == 0 && data.ACPDaily != nil && data.ACPHours != nil {
			*data.ACPDaily = 0
			*data.ACPHours = 0

		}
		if data.DCP != nil && data.ACP != nil {
			data.InvEff = new(float64)
			*data.InvEff = 0.0
			if *data.DCP > 0.0 {

				*data.InvEff = *data.ACP / *data.DCP * 100
			}

		} else {
			data.InvEff = nil
		}

		if data.IrrHoursArea != nil && data.IrrDurTime != nil {
			if *data.IrrDurTime != 0 {
				data.IrrHours = new(float64)
				*data.IrrHours = *data.IrrHoursArea / *data.IrrDurTime
			} else {
				data.IrrHours = new(float64)
				*data.IrrHours = 0
			}
		} else {
			data.IrrHoursArea = new(float64)
			*data.IrrHoursArea = 0
			data.IrrHours = new(float64)
			*data.IrrHours = 0
		}

		if data.IrrHours != nil && data.Cap != nil && data.ACPHours != nil {

			if *data.IrrHours > 0.0 && *data.Cap > 0.0 {
				//fmt.Println("ACPHours", data.ACPHours)
				//fmt.Println("Irr", data.Irr)
				//fmt.Println("Cap", data.Cap)
				if data.PR == nil {
					data.PR = new(float64)
					*data.PR = 0.0
				}
				*data.PR = (*data.ACPHours / (*data.IrrHours * *data.Cap)) * 100
			} else {
				data.PR = new(float64)
				*data.PR = 0.0
			}
		} else {
			data.PR = nil
		}

		data_final := HourlyDoc_{}
		data_final.ACP = data.ACP
		data_final.ACPDaily = data.ACPDaily
		data_final.ACPDurTime = data.ACPDurTime
		data_final.ACPHours = data.ACPHours
		data_final.ACPLife = data.ACPLife
		data_final.Cap = data.Cap
		data_final.Datestr = data.Datestr
		data_final.Datetime = data.Datetime
		data_final.DCP = data.DCP
		data_final.Freq = data.Freq
		data_final.Hour = data.Hour
		data_final.ID = data.ID
		data_final.InvEff = data.InvEff
		data_final.InverterID = data.InverterID
		data_final.InvTemp = data.InvTemp
		data_final.Irr = data.Irr
		data_final.IrrDurTime = data.IrrDurTime
		data_final.PR = data.PR
		data_final.PVTemp = data.PVTemp
		data_final.IrrHoursArea = data.IrrHoursArea
		data_final.IrrHours = data.IrrHours
		data_final.StationID = data.StationID
		// fmt.Println("data_final", data_final)
		err_Ins := doc.GetMongo().DB(zbtDb).C(HourTbl).Insert(data_final)
		if err_Ins != nil {
			err_Ups := doc.GetMongo().DB(zbtDb).C(HourTbl).Upsert(bson.M{"_id": data_final.ID}, data_final)
			if err_Ups != nil {
				//_di.Log.Err(err.Error())
				fmt.Println("line489:", err_Ups)
				return err_Ups

			}
		}

	}
	return nil
}

func (mycron ReportCron) AggHourlyReport(stationID string, start time.Time, end time.Time) (*[]HourlyDoc, error) {

	c := doc.GetMongo().DB(zbtDb).C(RT)
	pipe := c.Pipe(MakeHourPipeline(stationID, start, end))
	//fmt.Println(MakeHourPipeline())
	resp := []HourlyDoc{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return nil, err
	} else {
		return &resp, nil
	}

}

func MakeHourPipeline(stationid string, start time.Time, end time.Time) []bson.M {

	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid":  "$stationid",
				"inverterid": "$inverterid",
				"hour": bson.M{
					"$hour": bson.M{
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
				"datestr": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y-%m-%dT%H:00:00",
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
			},
			"stationid":          bson.M{"$first": "$stationid"},
			"inverterid":         bson.M{"$first": "$inverterid"},
			"cap":                bson.M{"$first": "$cap"},
			"capchecknil":        bson.M{"$min": "$cap"},
			"acp":                bson.M{"$sum": "$acp"},
			"acpchecknil":        bson.M{"$min": "$acp"},
			"dcp":                bson.M{"$sum": "$dcp"},
			"dcpchecknil":        bson.M{"$min": "$dcp"},
			"acphours":           bson.M{"$sum": "$acprealtime"},
			"acphourschecknil":   bson.M{"$min": "$acprealtime"},
			"acpdaily":           bson.M{"$max": "$acpdaily"},
			"acpdailychecknil":   bson.M{"$min": "$acpdaily"},
			"acplife":            bson.M{"$max": "$acplife"},
			"acplifechecknil":    bson.M{"$min": "$acplife"},
			"freq":               bson.M{"$avg": "$freq"},
			"freqchecknil":       bson.M{"$min": "$freq"},
			"invtemp":            bson.M{"$avg": "$invtemp"},
			"invtempchecknil":    bson.M{"$min": "$invtemp"},
			"pvtemp":             bson.M{"$avg": "$avgpvtemp"},
			"pvtempchecknil":     bson.M{"$min": "$avgpvtemp"},
			"irr":                bson.M{"$avg": "$avgirr"},
			"irrchecknil":        bson.M{"$min": "$avgirr"},
			"irrdurtime":         bson.M{"$sum": "$irrdurtime"},
			"irrdurtimechecknil": bson.M{"$min": "$irrdurtime"},
			"acpdurtime":         bson.M{"$sum": "$acpdurtime"},
			"acpdurtimechecknil": bson.M{"$min": "$acpdurtime"},
			"irrhoursarea":       bson.M{"$sum": "$irrrealtime"},
		},
	})

	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":          1,
			"inverterid":         1,
			"datetime":           bson.M{"$dateFromString": bson.M{"dateString": "$_id.datestr", "timezone": "+08:00"}},
			"hour":               "$_id.hour",
			"datestr":            "$_id.datestr",
			"cap":                1,
			"capchecknil":        1,
			"acp":                1,
			"acpchecknil":        1,
			"dcp":                1,
			"dcpchecknil":        1,
			"acphourschecknil":   1,
			"acphours":           1,
			"acpdaily":           1,
			"acpdailychecknil":   1,
			"acplife":            1,
			"acplifechecknil":    1,
			"freq":               1,
			"freqchecknil":       1,
			"invtemp":            1,
			"invtempchecknil":    1,
			"pvtemp":             1,
			"pvtempchecknil":     1,
			"irr":                1,
			"irrchecknil":        1,
			"irrdurtime":         1,
			"irrdurtimechecknil": 1,
			"acpdurtime":         1,
			"acpdurtimechecknil": 1,
			"irrhoursarea":       1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

// "irrdurtime":         bson.M{"$sum": bson.M{"$cond": []interface{}{bson.M{"$gte": []interface{}{"$avgirr", 0.1}, 1, 0}}}},
// 			"irrdurtimechecknil": bson.M{"$min": "$irrdurtime"},
// 			"acpdurtime":         bson.M{"$sum": bson.M{"$cond": []interface{}{bson.M{"$gt": []interface{}{"$acp", 0.0}, 1, 0}}}},
// 			"acpdurtimechecknil": bson.M{"$min": "$acpdurtime"},
func (mycron ReportCron) DailyReport(stationID string) error {

	//var hResult *[]search.SearchResult
	//var hResult []interface{}
	hourlyDoc := HourlyDoc{}
	dailyDoc := DailyDoc{}
	fmt.Println("DailyReport")
	mongo := doc.GetMongo()
	//-timestamp 取最新時間資料  ,timestamp 取最舊時間資料
	err := mongo.DB(zbtDb).C(DailyTbl).Find(bson.M{"stationid": stationID}).Sort("-datetime").Limit(1).One(&dailyDoc)
	//fmt.Println("hourlyDoc", hourlyDoc)
	var Days = 5
	var startTime time.Time
	var endTime time.Time
	if err != nil {
		fmt.Println("line481:", err)
		//_di.Log.Err(err.Error())
		err = mongo.DB(zbtDb).C(HourTbl).Find(bson.M{"stationid": stationID}).Sort("datetime").Limit(1).One(&hourlyDoc)

		fmt.Println("line485:", err)
		if err != nil {
			//_di.Log.Err(err.Error())
			return err
		}
		startTime = hourlyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		//startTime = time.Unix(1517414400, 0).In(time.FixedZone("GMT", 8*3600))
		fmt.Println("D_fir start", startTime)
		endTime = time.Now().In(time.FixedZone("GMT", 8*3600))
		//endTime = startTime.Add(time.Hour * time.Duration(24))
		fmt.Println("D_fir end", endTime)
	} else {
		startTime = dailyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		//startTime = time.Unix(1521043200, 0).In(time.FixedZone("GMT", 8*3600))
		fmt.Println("D_ start", startTime)
		//endTime = time.Unix(1521993600, 0).In(time.FixedZone("GMT", 8*3600))
		//endTime = time.Now().In(time.FixedZone("GMT", 8*3600))
		endTime = startTime.AddDate(0, 0, Days)
		fmt.Println("D_ end", endTime)
	}

	dResult, err := mycron.AggDailyReport(stationID, startTime, endTime)
	_dResult, _err := mycron.AggDailyReportIrr(stationID, startTime, endTime) //data need consider time and irr
	_dResult_ad, _err := mycron.AggDailyReportACPDaily(stationID, startTime, endTime)
	fmt.Println(dResult)

	if err != nil {
		_di.Log.Err(err.Error())

		return err
	}
	if _err != nil {
		_di.Log.Err(_err.Error())

		return _err
	}
	//fmt.Println("*hResult value", *hResult)
	//fmt.Println("hdata.ID", hdata.ID)
	for _, data := range *dResult {
		fmt.Println(data.StationID, data.Datetime)
		for _, _data_ad := range *_dResult_ad {
			if _data_ad.Datetime == data.Datetime && _data_ad.StationID == data.StationID && _data_ad.InverterID == data.InverterID {

				data.Irr = _data_ad.Irr
				data.ACPDaily = _data_ad.ACPDaily
				if data.IrrDurTime != nil && _data_ad.IrrDailyArea != nil {
					data.IrrDaily = new(float64)
					data.IrrDailyArea = new(float64)
					*data.IrrDailyArea = *_data_ad.IrrDailyArea
					*data.IrrDaily = *_data_ad.IrrDailyArea / *data.IrrDurTime
				}

			}
		}

		fmt.Println("data", data.StationID)
		// if data.IrrCheckNil == nil {
		// 	data.Irr = nil
		// }

		if data.CapCheckNil == nil {
			data.Cap = nil
		}

		if data.ACPDurTimeCheckNil == nil {
			data.ACPDurTime = nil
		}

		if data.IrrDurTimeCheckNil == nil {
			data.IrrDurTime = nil
		}

		if bson.ObjectId("") == data.ID {

			data.ID = getRTDocObjectId(data.StationID, data.InverterID, data.Datetime)
		}

		if data.Irr != nil && data.Cap != nil && data.ACPDaily != nil {

			if *data.Irr > 0.0 && *data.Cap > 0.0 {
				//fmt.Println("ACPHours", data.ACPHours)
				fmt.Println("Irr", data.Irr)
				fmt.Println("Cap", data.Cap)
				if data.PR == nil {
					data.PR = new(float64)
					*data.PR = 0.0
				}
				if data.KKp == nil {
					data.KKp = new(float64)
					*data.KKp = 0.0
				}
				*data.PR = (*data.ACPDaily / (*data.Irr * *data.Cap)) * 100
				*data.KKp = *data.ACPDaily / *data.Cap
			} else {
				data.PR = new(float64)
				*data.PR = 0.0
				data.KKp = new(float64)
				*data.KKp = 0.0
			}
		} else {
			data.PR = nil
			data.KKp = nil
		}

		/*if *data.Irr > 0.0 && *data.Cap > 0.0 {
			fmt.Println("Irr", data.Irr)
			fmt.Println("Cap", data.Cap)
			*data.PR = (*data.ACPDaily / (*data.Irr * *data.Cap)) * 100
			*data.KKp = *data.ACPDaily / *data.Cap
		} else {
			*data.PR = 0.0
			*data.KKp = 0.0
		}*/

		if data.IrrDurTime != nil && data.ACPDurTime != nil {
			data.IrrACPTime = new(float64)
			*data.IrrACPTime = 0.0
			if *data.IrrDurTime > 0.0 {

				*data.IrrACPTime = (*data.ACPDurTime / *data.IrrDurTime) * 100
			}

		} else {
			data.IrrACPTime = nil
		}

		for _, _data := range *_dResult {

			if _data.Datetime == data.Datetime && _data.StationID == data.StationID && _data.InverterID == data.InverterID {

				if _data.FreqCheckNil == nil {
					_data.Freq = nil
				}

				if _data.InvEffCheckNil == nil {
					_data.InvEff = nil
				}

				if _data.InvTempCheckNil == nil {
					_data.InvTemp = nil
				}

				if _data.PVTempCheckNil == nil {
					_data.PVTemp = nil
				}

				data.Freq = _data.Freq
				data.InvEff = _data.InvEff
				data.InvTemp = _data.InvTemp
				data.PVTemp = _data.PVTemp
			}
		}

		ovrt := doc.OverviewRTDoc{}
		err_ovrt := mongo.DB(zbtDb).C(doc.RTOverview).Find(bson.M{"stationid": stationID}).One(&ovrt)
		if data.KKp != nil && ovrt.APRTar != nil {
			if err_ovrt == nil && *ovrt.APRTar != 0.0 {
				*data.APR = (*data.KKp / *ovrt.APRTar) * 100
			} else {
				*data.APR = 0.0
			}
		} else {

			data.APR = nil
		}

		data_final := DailyDoc_{}
		data_final.ACPDaily = data.ACPDaily
		data_final.ACPDurTime = data.ACPDurTime
		data_final.ACPLife = data.ACPLife
		data_final.APR = data.APR
		data_final.Cap = data.Cap
		data_final.Datestr = data.Datestr
		data_final.Datetime = data.Datetime
		data_final.Day = data.Day
		data_final.Freq = data.Freq
		//data_final.HasAlert = data.HasAlert
		data_final.ID = data.ID
		data_final.InvEff = data.InvEff
		data_final.InverterID = data.InverterID
		data_final.InvTemp = data.InvTemp
		data_final.Irr = data.Irr
		data_final.IrrACPTime = data.IrrACPTime
		data_final.IrrDurTime = data.IrrDurTime
		data_final.KKp = data.KKp
		data_final.PR = data.PR
		data_final.PVTemp = data.PVTemp
		data_final.IrrDaily = data.IrrDaily
		data_final.IrrDailyArea = data.IrrDailyArea
		data_final.StationID = data.StationID
		err_Ins := doc.GetMongo().DB(zbtDb).C(DailyTbl).Insert(data_final)
		if err_Ins != nil {
			err_Ups := doc.GetMongo().DB(zbtDb).C(DailyTbl).Upsert(bson.M{"_id": data.ID}, data_final)
			if err_Ups != nil {
				_di.Log.Err(err.Error())
				return err_Ups

			}
		}

	}
	return nil
}

func (mycron ReportCron) AggDailyReport(stationID string, start time.Time, end time.Time) (*[]DailyDoc, error) {

	c := doc.GetMongo().DB(zbtDb).C(HourTbl)
	pipe := c.Pipe(MakeDailyPipeline(stationID, start, end))
	//fmt.Println(MakeHourPipeline())
	resp := []DailyDoc{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return nil, err
	} else {
		return &resp, nil
	}

}

func (mycron ReportCron) AggDailyReportIrr(stationID string, start time.Time, end time.Time) (*[]DailyDoc, error) {

	c := doc.GetMongo().DB(zbtDb).C(HourTbl)
	pipe := c.Pipe(MakeDailyPipelineIrr(stationID, start, end))
	//fmt.Println(MakeHourPipeline())
	resp := []DailyDoc{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return nil, err
	} else {
		return &resp, nil
	}

}

func (mycron ReportCron) AggDailyReportACPDaily(stationID string, start time.Time, end time.Time) (*[]DailyDoc, error) {

	c := doc.GetMongo().DB(zbtDb).C(HourTbl)
	pipe := c.Pipe(MakeDailyPipelineACPDaily(stationID, start, end))
	//fmt.Println(MakeHourPipeline())
	resp := []DailyDoc{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return nil, err
	} else {
		return &resp, nil
	}

}

func MakeDailyPipeline(stationid string, start time.Time, end time.Time) []bson.M {
	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid":  "$stationid",
				"inverterid": "$inverterid",
				"day": bson.M{
					"$dayOfMonth": bson.M{
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
				"datestr": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y-%m-%dT00:00:00",
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
			},
			"stationid":          bson.M{"$first": "$stationid"},
			"inverterid":         bson.M{"$first": "$inverterid"},
			"cap":                bson.M{"$first": "$cap"},
			"capchecknil":        bson.M{"$min": "$cap"},
			"acplife":            bson.M{"$max": "$acplife"},
			"irrdurtime":         bson.M{"$sum": "$irrdurtime"},
			"irrdurtimechecknil": bson.M{"$min": "$irrdurtime"},
			"acpdurtime":         bson.M{"$sum": "$acpdurtime"},
			"acpdurtimechecknil": bson.M{"$min": "$acpdurtime"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":          1,
			"inverterid":         1,
			"datetime":           bson.M{"$dateFromString": bson.M{"dateString": "$_id.datestr", "timezone": "+08:00"}},
			"day":                "$_id.day",
			"datestr":            "$_id.datestr",
			"cap":                1,
			"capchecknil":        1,
			"acplife":            1,
			"irrdurtime":         1,
			"irrdurtimechecknil": 1,
			"acpdurtime":         1,
			"acpdurtimechecknil": 1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

// bson.M{"$match": bson.M{"irr": bson.M{
// 	"$gt": 0,
// }}},
func MakeDailyPipelineACPDaily(stationid string, start time.Time, end time.Time) []bson.M {
	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid":  "$stationid",
				"inverterid": "$inverterid",
				"day": bson.M{
					"$dayOfMonth": bson.M{
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
				"datestr": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y-%m-%dT00:00:00",
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
			},
			"stationid":    bson.M{"$first": "$stationid"},
			"inverterid":   bson.M{"$first": "$inverterid"},
			"acpdaily":     bson.M{"$max": "$acpdaily"},
			"irrdailyarea": bson.M{"$sum": "$irrhoursarea"},
			"irr":          bson.M{"$sum": "$irrhours"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":    1,
			"inverterid":   1,
			"datetime":     bson.M{"$dateFromString": bson.M{"dateString": "$_id.datestr", "timezone": "+08:00"}},
			"day":          "$_id.day",
			"datestr":      "$_id.datestr",
			"acpdaily":     1,
			"irrdailyarea": 1,
			"irr":          1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

func MakeDailyPipelineIrr(stationid string, start time.Time, end time.Time) []bson.M {
	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"irrhours": bson.M{
			"$gte": 0.1,
		}}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid":  "$stationid",
				"inverterid": "$inverterid",
				"day": bson.M{
					"$dayOfMonth": bson.M{
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
				"datestr": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y-%m-%dT00:00:00",
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
			},
			"stationid":       bson.M{"$first": "$stationid"},
			"inverterid":      bson.M{"$first": "$inverterid"},
			"freq":            bson.M{"$avg": "$freq"},
			"inveff":          bson.M{"$avg": "$inveff"},
			"invtemp":         bson.M{"$avg": "$invtemp"},
			"pvtemp":          bson.M{"$avg": "$pvtemp"},
			"freqchecknil":    bson.M{"$min": "$freq"},
			"inveffchecknil":  bson.M{"$min": "$inveff"},
			"invtempchecknil": bson.M{"$min": "$invtemp"},
			"pvtempchecknil":  bson.M{"$min": "$pvtemp"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":       1,
			"inverterid":      1,
			"datetime":        bson.M{"$dateFromString": bson.M{"dateString": "$_id.datestr", "timezone": "+08:00"}},
			"day":             "$_id.day",
			"datestr":         "$_id.datestr",
			"freq":            1,
			"inveff":          1,
			"invtemp":         1,
			"pvtemp":          1,
			"freqchecknil":    1,
			"inveffchecknil":  1,
			"invtempchecknil": 1,
			"pvtempchecknil":  1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

func (mycron ReportCron) MonthlyReport(stationID string) error {

	dailyDoc := DailyDoc{}
	monthlyDoc := MonthlyDoc{}
	fmt.Println("MonthlyReport")
	mongo := doc.GetMongo()
	//-timestamp 取最新時間資料  ,timestamp 取最舊時間資料
	err := mongo.DB(zbtDb).C(MonthlyTbl).Find(bson.M{"stationid": stationID}).Sort("-datetime").Limit(1).One(&monthlyDoc)
	//fmt.Println("hourlyDoc", hourlyDoc)
	var Months = 1
	var startTime time.Time
	var endTime time.Time
	if err != nil {
		fmt.Println("line728:", err)
		//_di.Log.Err(err.Error())
		err = mongo.DB(zbtDb).C(DailyTbl).Find(bson.M{"stationid": stationID}).Sort("datetime").Limit(1).One(&dailyDoc)

		fmt.Println("line732:", err)
		if err != nil {
			//_di.Log.Err(err.Error())
			return err
		}
		startTime = dailyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		//startTime = time.Unix(1517414400, 0).In(time.FixedZone("GMT", 8*3600))
		fmt.Println("M_fir start", startTime)
		endTime = time.Now().In(time.FixedZone("GMT", 8*3600))
		//endTime = startTime.Add(time.Hour * time.Duration(24))
		fmt.Println("M_fir end", endTime)
	} else {
		startTime = monthlyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		fmt.Println("M_ start", startTime)
		endTime = startTime.AddDate(0, Months, 0)
		fmt.Println("M_ end", endTime)
	}

	mResult, err := mycron.AggMonthlyReport(stationID, startTime, endTime)

	if err != nil {
		_di.Log.Err(err.Error())

		return err
	}

	for _, month := range *mResult {

		if bson.ObjectId("") == month.ID {

			month.ID = getRTDocObjectId(month.StationID, month.InverterID, month.Datetime)
		}

		if *month.Irr > 0.0 && *month.Cap > 0.0 {
			fmt.Println("Irr", month.Irr)
			fmt.Println("Cap", month.Cap)
			*month.PR = (*month.ACPMonth / (*month.Irr * *month.Cap)) * 100
			*month.KKp = *month.ACPMonth / *month.Cap
		} else {
			*month.PR = 0.0
			*month.KKp = 0.0
		}
		err_Ins := doc.GetMongo().DB(zbtDb).C(MonthlyTbl).Insert(month)
		if err_Ins != nil {
			err_Ups := doc.GetMongo().DB(zbtDb).C(MonthlyTbl).Upsert(bson.M{"_id": month.ID}, month)
			if err_Ups != nil {
				_di.Log.Err(err.Error())
				return err_Ups

			}
		}

	}
	return nil
}

func (mycron ReportCron) AggMonthlyReport(stationID string, start time.Time, end time.Time) (*[]MonthlyDoc, error) {

	c := doc.GetMongo().DB(zbtDb).C(DailyTbl)
	pipe := c.Pipe(MakeMonthlyPipeline(stationID, start, end))
	//fmt.Println(MakeHourPipeline())
	resp := []MonthlyDoc{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return nil, err
	} else {
		return &resp, nil
	}

}
func MakeMonthlyPipeline(stationid string, start time.Time, end time.Time) []bson.M {
	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid":  "$stationid",
				"inverterid": "$inverterid",
				"month": bson.M{
					"$month": bson.M{
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
				"datestr": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y-%m-01T00:00:00",
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
			},
			"stationid":  bson.M{"$first": "$stationid"},
			"inverterid": bson.M{"$first": "$inverterid"},
			"cap":        bson.M{"$first": "$cap"},
			"acpmonth":   bson.M{"$sum": "$acpdaily"},
			"acplife":    bson.M{"$max": "$acplife"},
			"irr":        bson.M{"$sum": "$irr"},
			"apr":        bson.M{"$avg": "$apr"},
			"freq":       bson.M{"$avg": "$freq"},
			"inveff":     bson.M{"$avg": "$inveff"},
			"invtemp":    bson.M{"$avg": "$invtemp"},
			"pvtemp":     bson.M{"$avg": "$pvtemp"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":  1,
			"inverterid": 1,
			"datetime":   bson.M{"$dateFromString": bson.M{"dateString": "$_id.datestr", "timezone": "+08:00"}},
			"month":      "$_id.month",
			"datestr":    "$_id.datestr",
			"cap":        1,
			"acpmonth":   1,
			"acplife":    1,
			"irr":        1,
			"apr":        1,
			"freq":       1,
			"inveff":     1,
			"invtemp":    1,
			"pvtemp":     1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

func (mycron ReportCron) YearlyReport(stationID string) error {

	monthlyDoc := MonthlyDoc{}
	yearlyDoc := YearlyDoc{}
	fmt.Println("YearlyReport")
	mongo := doc.GetMongo()
	//-timestamp 取最新時間資料  ,timestamp 取最舊時間資料
	err := mongo.DB(zbtDb).C(YearlyTbl).Find(bson.M{"stationid": stationID}).Sort("-datetime").Limit(1).One(&yearlyDoc)
	//fmt.Println("hourlyDoc", hourlyDoc)
	var Years = 1
	var startTime time.Time
	var endTime time.Time
	if err != nil {
		fmt.Println("line878:", err)
		//_di.Log.Err(err.Error())
		err = mongo.DB(zbtDb).C(MonthlyTbl).Find(bson.M{"stationid": stationID}).Sort("datetime").Limit(1).One(&monthlyDoc)

		fmt.Println("line882:", err)
		if err != nil {
			//_di.Log.Err(err.Error())
			return err
		}
		startTime = monthlyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		//startTime = time.Unix(1517414400, 0).In(time.FixedZone("GMT", 8*3600))
		fmt.Println("Y_fir start", startTime)
		endTime = time.Now().In(time.FixedZone("GMT", 8*3600))
		//endTime = startTime.Add(time.Hour * time.Duration(24))
		fmt.Println("Y_fir end", endTime)
	} else {
		startTime = yearlyDoc.Datetime.In(time.FixedZone("GMT", 8*3600))
		fmt.Println("Y_ start", startTime)
		endTime = startTime.AddDate(Years, 0, 0)
		fmt.Println("Y_ end", endTime)
	}

	yResult, err := mycron.AggYearlyReport(stationID, startTime, endTime)

	if err != nil {
		_di.Log.Err(err.Error())

		return err
	}

	for _, year := range *yResult {

		if bson.ObjectId("") == year.ID {

			year.ID = getRTDocObjectId(year.StationID, year.InverterID, year.Datetime)
		}

		if *year.Irr > 0.0 && *year.Cap > 0.0 {

			*year.PR = (*year.ACPYear / (*year.Irr * *year.Cap)) * 100
			*year.KKp = *year.ACPYear / *year.Cap
		} else {
			*year.PR = 0.0
			*year.KKp = 0.0
		}

		err_Ins := doc.GetMongo().DB(zbtDb).C(YearlyTbl).Insert(year)
		if err_Ins != nil {
			err_Ups := doc.GetMongo().DB(zbtDb).C(YearlyTbl).Upsert(bson.M{"_id": year.ID}, year)
			if err_Ups != nil {
				_di.Log.Err(err.Error())
				return err_Ups

			}
		}

	}
	return nil
}

func (mycron ReportCron) AggYearlyReport(stationID string, start time.Time, end time.Time) (*[]YearlyDoc, error) {

	c := doc.GetMongo().DB(zbtDb).C(MonthlyTbl)
	pipe := c.Pipe(MakeYearlyPipeline(stationID, start, end))
	//fmt.Println(MakeHourPipeline())
	resp := []YearlyDoc{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return nil, err
	} else {
		return &resp, nil
	}

}
func MakeYearlyPipeline(stationid string, start time.Time, end time.Time) []bson.M {
	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid":  "$stationid",
				"inverterid": "$inverterid",
				"year": bson.M{
					"$year": bson.M{
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
				"datestr": bson.M{
					"$dateToString": bson.M{
						"format":   "%Y-01-01T00:00:00",
						"date":     "$datetime",
						"timezone": "+0800",
					},
				},
			},
			"stationid":  bson.M{"$first": "$stationid"},
			"inverterid": bson.M{"$first": "$inverterid"},
			"cap":        bson.M{"$first": "$cap"},
			"acpyear":    bson.M{"$sum": "$acpmonth"},
			"acplife":    bson.M{"$max": "$acplife"},
			"irr":        bson.M{"$sum": "$irr"},
			"apr":        bson.M{"$avg": "$apr"},
			"freq":       bson.M{"$avg": "$freq"},
			"inveff":     bson.M{"$avg": "$inveff"},
			"invtemp":    bson.M{"$avg": "$invtemp"},
			"pvtemp":     bson.M{"$avg": "$pvtemp"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":  1,
			"inverterid": 1,
			"datetime":   bson.M{"$dateFromString": bson.M{"dateString": "$_id.datestr", "timezone": "+08:00"}},
			"year":       "$_id.year",
			"datestr":    "$_id.datestr",
			"cap":        1,
			"acpyear":    1,
			"acplife":    1,
			"irr":        1,
			"apr":        1,
			"freq":       1,
			"inveff":     1,
			"invtemp":    1,
			"pvtemp":     1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

// func UpdatePreData() {
// 	fmt.Println("UpdatePreData")
// 	stationlist := []Station{}
// 	stationlist = append(stationlist, Station{StationID: "PT000001"})
// 	stationlist = append(stationlist, Station{StationID: "T0072801"})
// 	stationlist = append(stationlist, Station{StationID: "T0510104"})
// 	stationlist = append(stationlist, Station{StationID: "T0510801"})

// 	for _, station := range stationlist {
// 		rtdoc := []doc.RTDoc{}
// 		doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"uploadtimestamp": bson.M{
// 			"$lte": 1520295613,
// 			"$gte": 1517328000,
// 		}, "stationid": station.StationID}).All(&rtdoc)
// 		for _, data := range rtdoc {
// 			if data.PreDataID == nil {
// 				preRT, err_ := data.GetPrevRecV2()
// 				if err_ != nil {
// 					fmt.Println("line1295", err_)
// 				} else {
// 					if preRT.AvgIrr != nil && data.AvgIrr != nil {
// 						//IrrDurTime
// 						if data.IrrDurTime == nil {
// 							data.IrrDurTime = new(float64)
// 							*data.IrrDurTime = 0.0
// 						}
// 						if *preRT.AvgIrr > 0.1 && *data.AvgIrr > 0.1 {
// 							*data.IrrDurTime = float64(data.Timestamp - preRT.Timestamp)
// 						}

// 					}
// 					if preRT.ACP != nil && data.ACP != nil {
// 						//ACPDurTime
// 						if data.ACPDurTime == nil {
// 							data.ACPDurTime = new(float64)
// 							*data.ACPDurTime = 0.0
// 						}

// 						if *preRT.ACP > 0.0 && *data.ACP > 0.0 {
// 							*data.ACPDurTime = float64(data.Timestamp - preRT.Timestamp)
// 						}
// 					}

// 					err := doc.GetMongo().DB(zbtDb).C(doc.RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"predataid": preRT.ID, "irrdurtime": data.IrrDurTime, "acpdurtime": data.ACPDurTime}})
// 					if err != nil {
// 						fmt.Println("line1322", err)
// 					} else {
// 						fmt.Println(data.StationID, data.Timestamp, "success")
// 					}
// 				}
// 			} else {
// 				preRT := doc.RTDoc{}
// 				err := doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"_id": data.PreDataID}).One(&preRT)
// 				if preRT.AvgIrr != nil && data.AvgIrr != nil {
// 					//IrrDurTime
// 					if data.IrrDurTime == nil {
// 						data.IrrDurTime = new(float64)
// 						*data.IrrDurTime = 0.0
// 					}
// 					if *preRT.AvgIrr > 0.1 && *data.AvgIrr > 0.1 {
// 						*data.IrrDurTime = float64(data.Timestamp - preRT.Timestamp)
// 					}

// 				}
// 				if preRT.ACP != nil && data.ACP != nil {
// 					//ACPDurTime
// 					if data.ACPDurTime == nil {
// 						data.ACPDurTime = new(float64)
// 						*data.ACPDurTime = 0.0
// 					}

// 					if *preRT.ACP > 0.0 && *data.ACP > 0.0 {
// 						*data.ACPDurTime = float64(data.Timestamp - preRT.Timestamp)
// 					}
// 				}

// 				err = doc.GetMongo().DB(zbtDb).C(doc.RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"irrdurtime": data.IrrDurTime, "acpdurtime": data.ACPDurTime}})
// 				if err != nil {
// 					fmt.Println("line1353", err)
// 				} else {
// 					fmt.Println(data.StationID, data.Timestamp, "success")
// 				}

// 			}

// 		}
// 	}
// }
