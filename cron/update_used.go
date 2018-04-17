package cron

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
	"zbt.dforcepro.com/doc"
	//"golang.org/x/net/context/ctxhttp"
)

type Station struct {
	StationID string `json:"Station_ID,omitempty"` //案場編號

}

func UpdatePreData() {
	fmt.Println("UpdatePreData")
	stationlist := []Station{}
	stationlist = append(stationlist, Station{StationID: "PT000001"})
	stationlist = append(stationlist, Station{StationID: "T0072801"})
	stationlist = append(stationlist, Station{StationID: "T0510104"})
	stationlist = append(stationlist, Station{StationID: "T0510801"})

	for _, station := range stationlist {
		rtdoc := []doc.RTDoc{}
		doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"uploadtimestamp": bson.M{
			"$lte": 1520295613,
			"$gte": 1517328000,
		}, "stationid": station.StationID}).All(&rtdoc)
		for _, data := range rtdoc {
			if data.PreDataID == nil {
				preRT, err_ := data.GetPrevRecV2()
				if err_ != nil {
					fmt.Println("line1295", err_)
				} else {
					if preRT.AvgIrr != nil && data.AvgIrr != nil {
						//IrrDurTime
						if data.IrrDurTime == nil {
							data.IrrDurTime = new(float64)
							*data.IrrDurTime = 0.0
						}
						if *preRT.AvgIrr > 0.1 && *data.AvgIrr > 0.1 {
							*data.IrrDurTime = float64(data.Timestamp - preRT.Timestamp)
						}

					}
					if preRT.ACP != nil && data.ACP != nil {
						//ACPDurTime
						if data.ACPDurTime == nil {
							data.ACPDurTime = new(float64)
							*data.ACPDurTime = 0.0
						}

						if *preRT.ACP > 0.0 && *data.ACP > 0.0 {
							*data.ACPDurTime = float64(data.Timestamp - preRT.Timestamp)
						}
					}

					err := doc.GetMongo().DB(zbtDb).C(doc.RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"predataid": preRT.ID, "irrdurtime": data.IrrDurTime, "acpdurtime": data.ACPDurTime}})
					if err != nil {
						fmt.Println("line1322", err)
					} else {
						fmt.Println(data.StationID, data.Timestamp, "success")
					}
				}
			} else {
				preRT := doc.RTDoc{}
				err := doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"_id": data.PreDataID}).One(&preRT)
				if preRT.AvgIrr != nil && data.AvgIrr != nil {
					//IrrDurTime
					if data.IrrDurTime == nil {
						data.IrrDurTime = new(float64)
						*data.IrrDurTime = 0.0
					}
					if *preRT.AvgIrr > 0.1 && *data.AvgIrr > 0.1 {
						*data.IrrDurTime = float64(data.Timestamp - preRT.Timestamp)
					}

				}
				if preRT.ACP != nil && data.ACP != nil {
					//ACPDurTime
					if data.ACPDurTime == nil {
						data.ACPDurTime = new(float64)
						*data.ACPDurTime = 0.0
					}

					if *preRT.ACP > 0.0 && *data.ACP > 0.0 {
						*data.ACPDurTime = float64(data.Timestamp - preRT.Timestamp)
					}
				}

				err = doc.GetMongo().DB(zbtDb).C(doc.RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"irrdurtime": data.IrrDurTime, "acpdurtime": data.ACPDurTime}})
				if err != nil {
					fmt.Println("line1353", err)
				} else {
					fmt.Println(data.StationID, data.Timestamp, "success")
				}

			}

		}
	}
}

func UpdateIrrRealTime() {
	fmt.Println("UpdateIrrRealTime")

	stationdlist := []doc.Station{}
	err := doc.GetMongo().DB(zbtDb).C(doc.StationS).Find(bson.M{}).All(&stationdlist)
	if err != nil {
		fmt.Println("line110", err)
	} else {
		for _, station := range stationdlist {
			fmt.Println("data1", station.StationID)
			rtdoc := []doc.RTDoc{}
			doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"timestamp": bson.M{
				"$gte": 1522252800,
				"$lte": 1522425600,
			}, "stationid": station.StationID, "irrrealtime": bson.M{"$exists": false}}).All(&rtdoc)
			fmt.Println(rtdoc)
			for _, data := range rtdoc {
				fmt.Println("data", data.StationID)
				if data.PreDataID != nil {
					preRT := doc.RTDoc{}
					err := doc.GetMongo().DB(zbtDb).C(doc.RT).Find(bson.M{"_id": data.PreDataID}).One(&preRT)
					if err == nil {
						if preRT.AvgIrr != nil && data.AvgIrr != nil && data.IrrDurTime != nil {
							//IrrDurTime
							if *preRT.AvgIrr > 0.0 && *data.AvgIrr > 0.0 {
								if data.IrrRealTime == nil {
									data.IrrRealTime = new(float64)
									*data.IrrRealTime = 0.0
								}
								*data.IrrRealTime = ((*data.AvgIrr + *preRT.AvgIrr) * *data.IrrDurTime) / 2
							} else {
								if data.IrrRealTime == nil {
									data.IrrRealTime = new(float64)
									*data.IrrRealTime = 0.0
								}
							}

						}

					}

				} else {
					if data.IrrRealTime == nil {
						data.IrrRealTime = new(float64)
						*data.IrrRealTime = 0.0
					}
				}
				err = doc.GetMongo().DB(zbtDb).C(doc.RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"irrrealtime": data.IrrRealTime}})
				if err != nil {
					fmt.Println("line143", err)
				} else {
					fmt.Println(data.StationID, data.Timestamp, "success")
				}

			}

		}
	}

}

func UpdateRTTest() {

}
