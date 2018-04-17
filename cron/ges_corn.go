package cron

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"dforcepro.com/cron"
	"gopkg.in/mgo.v2/bson"
	"zbt.dforcepro.com/doc"
	//"golang.org/x/net/context/ctxhttp"
)

type GesCreateCron bool

func (mycron GesCreateCron) Enable() bool {
	return bool(mycron)
}

func (mycron GesCreateCron) GetJobs() []cron.JobSpec {
	return []cron.JobSpec{
		cron.JobSpec{
			Spec: "0 */1 * * * *",
			Job:  mycron.RefreshInvEndpoint,
		},
		cron.JobSpec{
			Spec: "0 */2 * * * *",
			Job:  mycron.RefreshOverview,
		},
		cron.JobSpec{
			Spec: "0 0 0 * * *",
			Job:  mycron.RefreshAPRTar,
		},
		cron.JobSpec{
			Spec: "0 */30 * * * *",
			Job:  mycron.RefreshRTUploadTimeStatus,
		},
	}
}

const (
	RTInv      = "RTInv"
	RTOverview = "RTOverview"
	RTStatus   = "RTStatus"
	zbtDb      = "zbt"
	StationS   = "Station"
	RT         = "RealTime"
	TimeFormat = "2006-01-02 15:04:05"
)

func (mycron GesCreateCron) Job() {
	now := time.Now()

	fmt.Println("cron run:", now)

}

func (mycron GesCreateCron) Job2() {
	now := time.Now()
	fmt.Println("cron run2:", now)

}

func (mycron GesCreateCron) RefreshAPRTar() {
	//fmt.Println("RefreshAPRTar")
	now := time.Now().Unix()
	month := time.Now().Month()
	var i int = int(month) //Jan 1,Feb 2...
	weight_M := 0.0

	ovrt := []doc.OverviewRTDoc{}
	_ = doc.GetMongo().DB(zbtDb).C(RTOverview).Find(bson.M{}).All(&ovrt)

	for _, ov := range ovrt {
		station := doc.Station{}
		err := doc.GetMongo().DB(zbtDb).C(StationS).Find(bson.M{"stationid": ov.StationID}).One(&station)
		if err != nil {
			_di.Log.Info(err.Error())
		} else {
			OnlineNum := float64((now - station.OnlineDate) / 86400)

			for _, marr := range station.Conf.APRMon {
				if marr.Mon == i {
					weight_M = marr.Value
				}
			}

			acptar := station.Conf.EnsureACP * (1 - (OnlineNum/365.0)*station.Conf.Decay*0.01) * weight_M
			err := doc.GetMongo().DB(zbtDb).C(RTOverview).Update(bson.M{"_id": station.ID}, bson.M{"$set": bson.M{"aprtar": acptar}})
			if err != nil {
				_di.Log.Info(err.Error())
			}
		}

	}

	return
}

func (mycron GesCreateCron) RefreshInvEndpoint() {

	//fmt.Println("RefreshInvEndpoint")
	station := []doc.Station{}
	err_S := doc.GetMongo().DB(zbtDb).C(StationS).Find(bson.M{}).All(&station)

	if err_S != nil {
		_di.Log.Info("line 623: invEndpoint(),Station_ID info not found.")
		_di.Log.Info(err_S.Error())
		return
	} else {
		for _, sta := range station {

			rtdoc := []doc.RTDoc{}

			invrow := len(sta.Inverter)

			err := doc.GetMongo().DB(zbtDb).C(RT).Find(bson.M{"stationid": sta.StationID}).Sort("-uploadtimestamp").Limit(invrow).All(&rtdoc)

			if err != nil {
				_di.Log.Info("line 638: invEndpoint(),realtime data  not found.")
				return

			} else {
				for _, rt := range rtdoc {
					invdoc := doc.InvRTDoc{}
					invdoc.City = sta.City
					invdoc.Town = sta.Town
					invdoc.Name = sta.Name
					//Capacity
					if invdoc.Capacity == nil {
						invdoc.Capacity = new(float64)
						*invdoc.Capacity = 0.0
					}
					//*invdoc.Capacity = 0
					invdoc.StationID = rt.StationID
					if rt.ACA != nil {
						invdoc.ACA = round(*rt.ACA, 2)
					}

					invdoc.No = rt.InverterID
					if rt.DC1DCA != nil {
						invdoc.DC1DCA = round(*rt.DC1DCA, 2)
					}
					if rt.ACP != nil {
						invdoc.ACP = round(*rt.ACP, 2)
					}
					if rt.ACV != nil {
						invdoc.ACV = round(*rt.ACV, 2)
					}
					if rt.ACPDaily != nil {
						invdoc.ACPDaily = round(*rt.ACPDaily, 2)
					}
					if rt.PR != nil {
						pr := round(*rt.PR, 2)
						if math.IsNaN(*pr) {
							*invdoc.PR = 0
						} else {
							invdoc.PR = pr
						}
					}
					if rt.Freq != nil {
						invdoc.Freq = round(*rt.Freq, 2)
					}
					invdoc.RS485ID = rt.InverterID
					if rt.InvEff != nil {
						invdoc.InvEff = round(*rt.InvEff, 2)
					}
					invdoc.InverterID = rt.InverterID
					invdoc.InverterName = "#1-" + strconv.Itoa(rt.InverterID)
					if rt.InvTemp != nil {
						invtmp := round(*rt.InvTemp, 2)
						if math.IsNaN(*invtmp) {
							*invdoc.InvTemp = 0
						} else {
							invdoc.InvTemp = invtmp
						}
					}
					//TODO: 修改Inv_Status
					invdoc.Inv_Status = getInvStatus(invdoc.StationID, invdoc.InverterID, rt.Timestamp)
					if rt.DC2DCA != nil {
						invdoc.DC2DCA = round(*rt.DC2DCA, 2)
					}
					if rt.DC2DCV != nil {
						invdoc.DC2DCV = round(*rt.DC2DCV, 2)
					}
					if rt.DC2DCP != nil {
						invdoc.DC2DCP = round(*rt.DC2DCP, 2)
					}
					if rt.ACPLife != nil {
						invdoc.ACPLife = round(*rt.ACPLife, 2)
					}
					invdoc.Timestamp = time.Unix(rt.Timestamp, 0).In(time.FixedZone("GMT", sta.TimeZone*3600)).Format(TimeFormat)
					if rt.DC1DCP != nil {
						invdoc.DC1DCP = round(*rt.DC1DCP, 2)
					}
					if rt.DC1DCV != nil {
						invdoc.DC1DCV = round(*rt.DC1DCV, 2)
					}
					for _, inv := range sta.Inverter {

						if inv.ID == rt.InverterID {
							invdoc.Capacity = &inv.ModuleCap

						}
					}
					err := doc.GetMongo().DB(zbtDb).C(RTInv).Upsert(bson.M{"stationid": invdoc.StationID, "inverterid": invdoc.InverterID}, invdoc)
					if err != nil {
						_di.Log.Err(err.Error())
						return

					}

					//result = append(result, invdoc)
				}

			}
		}

	}
	return
}

func (mycron GesCreateCron) RefreshOverview() {
	//fmt.Println("RefreshOverview")
	mongo := doc.GetMongo()
	c := mongo.DB(zbtDb).C(RTInv)
	pipe := c.Pipe(mycron.MakeOverviewPipeline())
	resp := []doc.OverviewRTDoc{}
	//var resp []interface{}
	err := pipe.All(&resp)
	if err != nil {
		//handle error
		return
	} else {
		for _, ovrt := range resp {
			// fmt.Println("ovrt", ovrt)
			// fmt.Println("ovrt", ovrt.DC1DCA, *ovrt.DC1DCA)
			ovrt.ID = doc.GetStationDocObjectId(ovrt.StationID)
			//DCA
			if ovrt.DC1DCA != nil && ovrt.DC2DCA != nil {
				ovrt.DCA = round(*ovrt.DC1DCA+*ovrt.DC2DCA, 2)
			} else if ovrt.DC1DCA != nil {
				ovrt.DCA = round(*ovrt.DC1DCA, 2)
			} else if ovrt.DC2DCA != nil {
				ovrt.DCA = round(*ovrt.DC2DCA, 2)
			} else {
				ovrt.DCA = nil
			}
			//DCV
			if ovrt.DC1DCV != nil && ovrt.DC2DCV != nil {
				ovrt.DCV = round((*ovrt.DC1DCV+*ovrt.DC2DCV)/2.0, 2)
			} else if ovrt.DC1DCV != nil {
				ovrt.DCV = round(*ovrt.DC1DCV, 2)
			} else if ovrt.DC2DCV != nil {
				ovrt.DCV = round(*ovrt.DC2DCV, 2)
			} else {
				ovrt.DCV = nil
			}
			//DCP
			if ovrt.DC1DCP != nil && ovrt.DC2DCP != nil {
				ovrt.DCP = round(*ovrt.DC1DCP+*ovrt.DC2DCP, 2)
			} else if ovrt.DC1DCV != nil {
				ovrt.DCP = round(*ovrt.DC1DCP, 2)
			} else if ovrt.DC2DCV != nil {
				ovrt.DCP = round(*ovrt.DC2DCP, 2)
			} else {
				ovrt.DCP = nil
			}
			//ACPDailyPerCap
			if ovrt.ACPDaily != nil && ovrt.Capacity != nil {
				ovrt.ACPDailyPerCap = round(*ovrt.ACPDaily / *ovrt.Capacity, 2)
			} else {
				ovrt.ACPDailyPerCap = nil
			}
			//ACPPerCap
			if ovrt.ACP != nil && ovrt.Capacity != nil {
				ovrt.ACPPerCap = round(*ovrt.ACP / *ovrt.Capacity, 2)
			} else {
				ovrt.ACPPerCap = nil
			}
			//ACV
			if ovrt.ACV != nil {
				ovrt.ACV = round(*ovrt.ACV, 2)
			} else {
				ovrt.ACV = nil
			}
			//ACA
			if ovrt.ACA != nil {
				ovrt.ACA = round(*ovrt.ACA, 2)
			} else {
				ovrt.ACA = nil
			}
			//ACP
			if ovrt.ACP != nil {
				ovrt.ACP = round(*ovrt.ACP, 2)
			} else {
				ovrt.ACP = nil
			}
			//ACPDaily
			if ovrt.ACPDaily != nil {
				ovrt.ACPDaily = round(*ovrt.ACPDaily, 2)
			} else {
				ovrt.ACPDaily = nil
			}
			//ACPLife
			if ovrt.ACPLife != nil {
				ovrt.ACPLife = round(*ovrt.ACPLife, 2)
			} else {
				ovrt.ACPLife = nil
			}
			//Capacity
			if ovrt.Capacity != nil {
				ovrt.Capacity = round(*ovrt.Capacity, 2)
			} else {
				ovrt.Capacity = nil
			}
			//PR
			if ovrt.PR != nil {
				ovrt.PR = round(*ovrt.PR, 2)
			} else {
				ovrt.PR = nil
			}
			err := mongo.DB(zbtDb).C(RTOverview).Upsert(bson.M{"_id": ovrt.ID}, ovrt)
			if err != nil {
				_di.Log.Err(err.Error())
				return

			}

		}
		return
	}

}

func (mycron GesCreateCron) MakeOverviewPipeline() []bson.M {
	pipiline := []bson.M{}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid": "$stationid",
			},
			"timestamp": bson.M{"$first": "$timestamp"},
			"stationid": bson.M{"$first": "$stationid"},
			"name":      bson.M{"$first": "$name"},
			"city":      bson.M{"$first": "$city"},
			"town":      bson.M{"$first": "$town"},
			"capacity":  bson.M{"$sum": "$capacity"},
			"dc1dca":    bson.M{"$sum": "$dc1dca"},
			"dc2dca":    bson.M{"$sum": "$dc2dca"},
			"dc1dcv":    bson.M{"$avg": "$dc1dcv"},
			"dc2dcv":    bson.M{"$avg": "$dc2dcv"},
			"dc1dcp":    bson.M{"$sum": "$dc1dcp"},
			"dc2dcp":    bson.M{"$sum": "$dc2dcp"},
			"acv":       bson.M{"$avg": "$acv"},
			"aca":       bson.M{"$sum": "$aca"},
			"acp":       bson.M{"$sum": "$acp"},
			"acpdaily":  bson.M{"$sum": "$acpdaily"},
			"acplife":   bson.M{"$sum": "$acplife"},
			"status":    bson.M{"$min": "$inv_status"},
			"pr":        bson.M{"$avg": "$pr"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"timestamp": 1,
			"stationid": 1,
			"name":      1,
			"city":      1,
			"town":      1,
			"capacity":  1,
			"dc1dca":    1,
			"dc2dca":    1,
			"dc1dcv":    1,
			"dc2dcv":    1,
			"dc1dcp":    1,
			"dc2dcp":    1,
			"acv":       1,
			"aca":       1,
			"acp":       1,
			"acpdaily":  1,
			"acplife":   1,
			"status":    1,
			"pr":        1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	return pipiline
}

func (mycron GesCreateCron) RefreshRTUploadTimeStatus() {
	//fmt.Println("RefreshRTUploadTimeStatus")

	station := []doc.Station{}
	err := doc.GetMongo().DB(zbtDb).C(StationS).Find(bson.M{}).All(&station)
	if err == nil {
		for _, sta := range station {
			rts := doc.RTStatus{}
			err_rts := doc.GetMongo().DB(zbtDb).C(RTStatus).Find(bson.M{"stationid": sta.StationID}).One(&rts)

			if err_rts != nil {
				rtdoc := doc.RTDoc{}
				err_rt := doc.GetMongo().DB(zbtDb).C(RT).Find(bson.M{"stationid": sta.StationID}).Sort("-uploadtimestamp").Limit(1).One(&rtdoc)
				if err_rt == nil {

					rts.StationID = rtdoc.StationID
					rts.Status = 1
					rts.LastUploadTime = rtdoc.UploadTimestamp
					_ = doc.GetMongo().DB(zbtDb).C(RTStatus).Upsert(bson.M{"stationid": rts.StationID}, rts)

				}

			} else {
				rtdoc := doc.RTDoc{}
				err_rtins := doc.GetMongo().DB(zbtDb).C(RT).Find(bson.M{"stationid": sta.StationID}).Sort("-uploadtimestamp").Limit(1).One(&rtdoc)

				if err_rtins == nil {

					if rts.LastUploadTime == rtdoc.UploadTimestamp {
						rts.Status = -1
					} else {
						rts.Status = 1
						rts.LastUploadTime = rtdoc.UploadTimestamp
					}
					_ = doc.GetMongo().DB(zbtDb).C(RTStatus).Upsert(bson.M{"stationid": rts.StationID}, rts)

				}
			}
		}
	}
}

//return 1:normal 0:yellowalert -1:redalert
func getInvStatus(StationID string, InvID int, time int64) int {

	rts := doc.RTStatus{}
	err_rts := doc.GetMongo().DB(zbtDb).C(RTStatus).Find(bson.M{"stationid": StationID}).One(&rts)
	if err_rts != nil {
		return -1
	} else {
		if rts.Status == -1 {
			return -1
		} else {
			err_alert := doc.GetMongo().DB(zbtDb).C(doc.EventC).Find(bson.M{"stationid": StationID, "inverterid": InvID, "alerttype": 1, "eventtimeunix": bson.M{"$gte": time - 3600, "$lte": time}}).One(&rts)
			if err_alert == nil {
				return -1
			} else {
				err_warning := doc.GetMongo().DB(zbtDb).C(doc.EventC).Find(bson.M{"stationid": StationID, "inverterid": InvID, "alerttype": 2, "eventtimeunix": bson.M{"$gte": time - 3600, "$lte": time}}).One(&rts)
				if err_warning == nil {
					return 0
				} else {
					return 1
				}
			}
		}
	}
}

func round(v float64, decimals int) *float64 {
	if math.IsNaN(v) {
		return nil
	} else {
		var pow float64 = 1
		for i := 0; i < decimals; i++ {
			pow *= 10
		}
		result := float64(int((v*pow)+0.5)) / pow
		return &result
	}
}
