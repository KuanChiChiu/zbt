package cron

import (
	"fmt"
	"time"

	"dforcepro.com/cron"
	"dforcepro.com/resource/queue"
	"github.com/RichardKnop/machinery/v1/tasks"
	"gopkg.in/mgo.v2/bson"
	"zbt.dforcepro.com/doc"
	//"golang.org/x/net/context/ctxhttp"
)

type ParseDataCron bool

func (mycron ParseDataCron) Enable() bool {
	return bool(mycron)
}

func (mycron ParseDataCron) GetJobs() []cron.JobSpec {
	return []cron.JobSpec{
		cron.JobSpec{
			Spec: "* */5 * * * *",
			Job:  mycron.ParseData,
		},
	}
}

func (mycron ParseDataCron) ParseData() {
	ovrtlist := []doc.OverviewRTDoc{}
	err := doc.GetMongo().DB(zbtDb).C(doc.RTOverview).Find(bson.M{}).All(&ovrtlist)
	if err == nil {
		for _, station := range ovrtlist {
			fmt.Println("StationID", station.StationID)
			_err := mycron.ParseRawdata(station.StationID)
			if _err != nil {
				fmt.Println("Line 35", _err)
				//_di.Log.Err(_err.Error())
			}
		}
	}

}

func (mycron ParseDataCron) ParseRawdata(stationid string) error {
	rawdatadoclist := []doc.RTDoc{}
	// if timeint == 0 {
	// 	timeint = time.Now().Unix()
	// }
	// end := timeint - 120 //1m
	end := time.Now().Unix() - 600     //1hr before
	start := time.Now().Unix() - 43200 //1hr before
	//start := timeint - 43200         //1m
	//end := 1523289600   //20180410
	//err := doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"stationid": stationid, "timestamp": bson.M{"$gte": start, "$lte": end}, "_parse": bson.M{"$exists": false}}).Sort("timestamp").All(&rawdatadoclist)

	err := doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"stationid": stationid, "timestamp": bson.M{"$gte": start, "$lte": end}, "_parse": bson.M{"$exists": false}}).Sort("timestamp").All(&rawdatadoclist)
	if err != nil {
		return err
	} else {
		args := []tasks.Arg{}
		for _, rawdata := range rawdatadoclist {

			fmt.Println("rawdata", rawdata.StationID, rawdata.Datetime)

			rtdata := rawdata.GetRTRec()

			//fmt.Println("timedef", time.Now())
			err_save := doc.GetMongo().DB(zbtDb).C(RT).Upsert(bson.M{"_id": rtdata.ID}, rtdata)

			//err_save := rtdata.UpdateRT(bson.M{"$set": bson.M{"_parse": time.Now().Unix()}})
			if err_save != nil {

				fmt.Println("Line 56", err_save)
				return err_save
				//_di.Log.Err(err_save.Error())
			}

			_di.Log.Info("processAlert Start")
			fmt.Println("id", rtdata.StationID)
			err = rtdata.ProcessAlert()
			if err != nil {
				fmt.Println("Line 63", err)
				//_di.Log.Err(err.Error())
			}

			//_di.Log.Info("processAlert End")

			rtdata.UpdateRT(bson.M{"$set": bson.M{"_parse": time.Now().Unix()}})

			args = append(args, tasks.Arg{Type: "string", Value: rawdata.ID.Hex()})

		}

		result, err := queue.SendTask(doc.TaskSolarCreated, args)
		if err != nil {
			//_di.Log.Err(err.Error())
		} else {
			_di.Log.Info(fmt.Sprintf("Add Queue. UUID is %s. State is %s", result.TaskUUID, result.State))
		}
	}
	return nil
}

//get predoc RT doc for RealTime table
// func (mycron ParseDataCron) ParsePreDataRT() {
// 	stationlist := []doc.Station{}
// 	err := doc.GetMongo().DB(zbtDb).C(doc.StationS).Find(bson.M{}).All(&stationlist)
// 	if err == nil {
// 		for _, station := range stationlist {
// 			fmt.Println("StationID", station.StationID)
// 			_err := mycron.paresPreData(station.StationID)
// 			if _err != nil {
// 				fmt.Println("Line 35", _err)
// 				//_di.Log.Err(_err.Error())
// 			}
// 		}
// 	}

// }

// func (mycron ParseDataCron) paresPreData(stationid string) error {
// 	rawdatadoclist := []doc.RTDoc{}
// 	end := time.Now().Unix() - 60     //1m
// 	start := time.Now().Unix() - 3600 //1hr before
// 	err := doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"stationid": stationid, "timestamp": bson.M{"$gte": start, "$lte": end}, "_parse": bson.M{"$exists": false}}).Sort("timestamp").All(&rawdatadoclist)
// 	if err != nil {
// 		return err
// 	} else {
// 		args := []tasks.Arg{}
// 		for _, rawdata := range rawdatadoclist {
// 			//fmt.Println("rawdata", rawdata.Timestamp, rawdata.StationID)

// 			rtdata, errpredata := getPredata(rawdata)

// 			//fmt.Println("timedef", time.Now())
// 			if errpredata != nil {
// 				err_save := rtdata.Save()
// 				if err_save != nil {

// 					fmt.Println("Line 129", err_save)
// 					return err_save
// 					//_di.Log.Err(err_save.Error())
// 				}

// 			} else {
// 				return errpredata
// 			}
// 			//err_save := rtdata.Save()

// 			//_di.Log.Info("processAlert Start")
// 			// fmt.Println("id", rtdata.StationID)
// 			// err = rtdata.ProcessAlert()
// 			// if err != nil {
// 			// 	fmt.Println("Line 63", err)
// 			// 	//_di.Log.Err(err.Error())
// 			// }

// 			//_di.Log.Info("processAlert End")

// 			rawdata.UpdateRawdata(bson.M{"$set": bson.M{"_parse": time.Now().Unix()}})

// 			args = append(args, tasks.Arg{Type: "string", Value: rawdata.ID.Hex()})

// 		}

// 		result, err := queue.SendTask(doc.TaskSolarCreated, args)
// 		if err != nil {
// 			//_di.Log.Err(err.Error())
// 		} else {
// 			_di.Log.Info(fmt.Sprintf("Add Queue. UUID is %s. State is %s", result.TaskUUID, result.State))
// 		}
// 	}
// 	return nil
// }

// func getPredata(rd doc.RTDoc) (*doc.RTDoc, error) {

// 	IrrLimit := 0.1
// 	ACPLimit := 0.0
// 	info, err := doc.GetStationInRedis(rd.StationID, rd.InverterID)

// 	fmt.Println("line167 err", err)
// 	fmt.Println("line168 info", info)
// 	rd.Cap = new(float64)
// 	*rd.Cap = 0.0

// 	if len(info) == 3 && err == nil {
// 		rd.Cap = info[0]
// 		IrrLimit = *info[1]
// 		ACPLimit = *info[2]
// 		preRTDoc, _ := rd.GetPrevRecV2()

// 		if preRTDoc != nil {
// 			if rd.ACPLife != nil && preRTDoc.ACPLife != nil {
// 				//ACPRealTime
// 				if rd.ACPRealTime == nil {
// 					rd.ACPRealTime = new(float64)
// 					*rd.ACPRealTime = 0.0
// 				}
// 				rd.ACPRealTime = round(*rd.ACPLife-*preRTDoc.ACPLife, 4)
// 			} else if rd.ACP != nil {
// 				if rd.ACP != nil {
// 					//ACPRealTime
// 					if rd.ACPRealTime == nil {
// 						rd.ACPRealTime = new(float64)
// 						*rd.ACPRealTime = 0.0
// 					}
// 					*rd.ACPRealTime = *rd.ACP * 0.001
// 				}
// 			}
// 			rd.PreDataID = &preRTDoc.ID

// 			if preRTDoc.AvgIrr != nil && rd.AvgIrr != nil {
// 				//IrrDurTime
// 				if rd.IrrDurTime == nil {
// 					rd.IrrDurTime = new(float64)
// 					*rd.IrrDurTime = 0.0
// 				}
// 				if rd.IrrRealTime == nil {
// 					rd.IrrRealTime = new(float64)
// 					*rd.IrrRealTime = 0.0
// 				}

// 				if *preRTDoc.AvgIrr > IrrLimit && *rd.AvgIrr > IrrLimit {
// 					*rd.IrrDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
// 				}

// 				if *preRTDoc.AvgIrr > 0.0 && *rd.AvgIrr > 0.0 {

// 					*rd.IrrRealTime = ((*rd.AvgIrr + *preRTDoc.AvgIrr) * *rd.IrrDurTime) / 2
// 				} else {
// 					*rd.IrrRealTime = 0
// 				}

// 			}
// 			if preRTDoc.ACP != nil && rd.ACP != nil {
// 				//ACPDurTime
// 				if rd.ACPDurTime == nil {
// 					rd.ACPDurTime = new(float64)
// 					*rd.ACPDurTime = 0.0
// 				}
// 				if *preRTDoc.ACP > ACPLimit && *rd.ACP > ACPLimit {
// 					*rd.ACPDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
// 				}
// 			}
// 			if rd.AvgIrr == nil || rd.Cap == nil || rd.DCP == nil {
// 				rd.RA = nil
// 			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
// 				rd.RA = new(float64)
// 				*rd.RA = 0.0
// 			} else {
// 				//RA
// 				if rd.RA == nil {
// 					rd.RA = new(float64)
// 					*rd.RA = 0.0
// 				}
// 				rd.RA = round((*rd.DCP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
// 			}
// 			if rd.AvgIrr == nil || rd.Cap == nil || rd.ACP == nil {
// 				rd.PR = nil
// 			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
// 				rd.PR = new(float64)
// 				*rd.PR = 0.0
// 			} else {
// 				//PR
// 				if rd.PR == nil {
// 					rd.PR = new(float64)
// 					*rd.PR = 0.0
// 				}
// 				rd.PR = round((*rd.ACP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
// 			}
// 			return &rd, nil
// 		} else {
// 			if rd.ACP != nil {
// 				//ACPRealTime
// 				if rd.ACPRealTime == nil {
// 					rd.ACPRealTime = new(float64)
// 					*rd.ACPRealTime = 0.0
// 				}
// 				*rd.ACPRealTime = *rd.ACP * 0.001
// 			}

// 			rd.PreDataID = nil
// 			rd.IrrDurTime = nil
// 			rd.ACPDurTime = nil
// 			rd.IrrRealTime = nil
// 			if rd.AvgIrr == nil || rd.Cap == nil || rd.DCP == nil {
// 				rd.RA = nil
// 			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
// 				rd.RA = new(float64)
// 				*rd.RA = 0.0
// 			} else {
// 				//RA
// 				if rd.RA == nil {
// 					rd.RA = new(float64)
// 					*rd.RA = 0.0
// 				}
// 				rd.RA = round((*rd.DCP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
// 			}
// 			if rd.AvgIrr == nil || rd.Cap == nil || rd.ACP == nil {
// 				rd.PR = nil
// 			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
// 				rd.PR = new(float64)
// 				*rd.PR = 0.0
// 			} else {
// 				//PR
// 				if rd.PR == nil {
// 					rd.PR = new(float64)
// 					*rd.PR = 0.0
// 				}
// 				rd.PR = round((*rd.ACP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
// 			}

// 		}
// 		return &rd, nil
// 	} else {

// 		rd.Cap = nil
// 		rd.PR = nil
// 		rd.RA = nil
// 		preRTDoc, _ := rd.GetPrevRecV2()
// 		if preRTDoc != nil {
// 			if rd.ACPLife != nil && preRTDoc.ACPLife != nil {
// 				//ACPRealTime
// 				if rd.ACPRealTime == nil {
// 					rd.ACPRealTime = new(float64)
// 					*rd.ACPRealTime = 0.0
// 				}
// 				rd.ACPRealTime = round(*rd.ACPLife-*preRTDoc.ACPLife, 2)
// 			}
// 			rd.PreDataID = &preRTDoc.ID

// 			if preRTDoc.AvgIrr != nil && rd.AvgIrr != nil {
// 				//IrrDurTime
// 				if rd.IrrDurTime == nil {
// 					rd.IrrDurTime = new(float64)
// 					*rd.IrrDurTime = 0.0
// 				}
// 				if rd.IrrRealTime == nil {
// 					rd.IrrRealTime = new(float64)
// 					*rd.IrrRealTime = 0.0
// 				}

// 				if *preRTDoc.AvgIrr > IrrLimit && *rd.AvgIrr > IrrLimit {
// 					*rd.IrrDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
// 				}

// 				if *preRTDoc.AvgIrr > 0.0 && *rd.AvgIrr > 0.0 {
// 					*rd.IrrRealTime = ((*rd.AvgIrr + *preRTDoc.AvgIrr) * *rd.IrrDurTime) / 2
// 				} else {
// 					*rd.IrrRealTime = 0
// 				}
// 			}
// 			if preRTDoc.ACP != nil && rd.ACP != nil {
// 				//ACPDurTime
// 				if rd.ACPDurTime == nil {
// 					rd.ACPDurTime = new(float64)
// 					*rd.ACPDurTime = 0.0
// 				}

// 				if *preRTDoc.ACP > ACPLimit && *rd.ACP > ACPLimit {
// 					*rd.ACPDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
// 				}
// 			}
// 			return &rd, nil
// 		} else {
// 			rd.ACPRealTime = nil
// 			rd.PreDataID = nil
// 			rd.IrrDurTime = nil
// 			rd.ACPDurTime = nil
// 			rd.IrrRealTime = nil
// 		}
// 		return &rd, nil

// 	}
// 	return nil, err
// }
