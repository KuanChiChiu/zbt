package search

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"dforcepro.com/resource/db"
	"gopkg.in/mgo.v2/bson"
	search "gopkg.in/olivere/elastic.v5"
	"zbt.dforcepro.com/doc"
)

//Comment
const (
	SolarFormIndex   = "zbt"
	SolarFormType    = "solar"
	SolarFormMapping = `{"settings":{"number_of_shards":1,"number_of_replicas":0},
	"mappings":{"solar":{
					"_all":{"enabled":false},
					"properties":{
						"ID":{"type":"keyword"},
						"InverterID":{"type":"long"},
						"Station_ID":{"type":"keyword"},
						"DCA":{"type":"float"},
						"DCV":{"type":"float"},
						"DCP":{"type":"float"},
						"ACA":{"type":"float"},
						"ACV":{"type":"float"},
						"ACP":{"type":"float"},
						"Freq":{"type":"float"},
						"ACP_RealTime":{"type":"float"},
						"ACP_Daily":{"type":"float"},
						"ACP_Life":{"type":"float"},
						"Inv_Eff":{"type":"float"},
						"Inverter_Temp":{"type":"float"},						
						"AvgPVTemp":{"type":"float"},
						"PR":{"type":"float"},						
						"RA":{"type":"float"},
						"Capacity":{"type":"long"},
						"AvgIrr":{"type":"float"},
						"IrrDurTime":{"type":"long"},
						"ACPDurTime":{"type":"long"},
						"Inverter_Status":{"type":"long"},
						"Timestamp":{"format":"epoch_second","type":"date"},
						"Upload_Timestamp":{"format":"epoch_second","type":"date"}
									}
								}}}`
	zbtDb             = "zbt"
	StationS          = "Station"
	SolarSearchLayout = "2006-01-02T15:04:05"
	Sec               = 60
	Min               = 60
	Hour              = 24
)



type SolarSearch struct {
	*doc.RTDoc
	Search SearchMeta `json:"_search,omitempty"`
}
type HourlyDoc struct {
	ID         bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Timestamp  int64         `json:"Timestamp,omitempty"`     // 數據採集時間
	Datetime   time.Time     `json:"Datetime,omitempty"`      // 數據採集時間
	StationID  string        `json:"Station_ID,omitempty"`    //案場編號
	InverterID uint8         `json:"InverterID,omitempty"`    // 逆變器編號
	DCP        float64       `json:"DCP,omitempty"`
	ACP        float64       `json:"ACP,omitempty"`
	ACPHours   float64       `json:"ACP_Hours,omitempty"`
	ACPDaily   float64       `json:"ACP_Daily,omitempty"`
	ACPLife    float64       `json:"ACP_Life,omitempty"`
	Freq       float64       `json:"Freq,omitempty"`
	InvTemp    float64       `json:"InvTemp,omitempty"`
	PVTemp     float64       `json:"PVTemp,omitempty"`
	Irr        float64       `json:"Irr,omitempty"`
	InvEff     float64       `json:"Inv_Eff,omitempty"`
	PR         float64       `json:"PR,omitempty"`
	IrrDurTime float64       `json:"IrrDurTime,omitempty"`
	ACPDurTime float64       `json:"ACPDurTime,omitempty"`
	Capacity   float64       `json:"Capacity,omitempty"`
	HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type DailyDoc struct {
	ID         bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Timestamp  int64         `json:"Timestamp,omitempty"`     // 數據採集時間
	StationID  string        `json:"Station_ID,omitempty"`    //案場編號
	InverterID uint8         `json:"InverterID,omitempty"`    // 逆變器編號
	ACPDaily   float64       `json:"ACP_Daily,omitempty"`
	ACPLife    float64       `json:"ACP_Life,omitempty"`
	Freq       float64       `json:"Freq,omitempty"`
	InvTemp    float64       `json:"InvTemp,omitempty"`
	PVTemp     float64       `json:"PVTemp,omitempty"`
	Irr        float64       `json:"Irr,omitempty"`
	InvEff     float64       `json:"Inv_Eff,omitempty"`
	PR         float64       `json:"PR,omitempty"`
	APR        float64       `json:"APR,omitempty"`
	ACPkkp     float64       `json:"kWh_kWp,omitempty"`
	IrrDurTime float64       `json:"IrrDurTime,omitempty"`
	ACPDurTime float64       `json:"ACPDurTime,omitempty"`
	IrrACPTime float64       `json:"IrrACPTime,omitempty"`
	Capacity   float64       `json:"Capacity,omitempty"`
	HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type MonthlyDoc struct {
	ID         bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Timestamp  int64         `json:"Timestamp,omitempty"`     // 數據採集時間
	StationID  string        `json:"Station_ID,omitempty"`    //案場編號
	InverterID uint8         `json:"InverterID,omitempty"`    // 逆變器編號
	ACPMonth   float64       `json:"ACP_Month,omitempty"`
	ACPLife    float64       `json:"ACP_Life,omitempty"`
	Freq       float64       `json:"Freq,omitempty"`
	InvTemp    float64       `json:"InvTemp,omitempty"`
	PVTemp     float64       `json:"PVTemp,omitempty"`
	Irr        float64       `json:"Irr,omitempty"`
	InvEff     float64       `json:"Inv_Eff,omitempty"`
	PR         float64       `json:"PR,omitempty"`
	APR        float64       `json:"APR,omitempty"`
	ACPkkp     float64       `json:"kWh_kWp,omitempty"`
	IrrDurTime float64       `json:"IrrDurTime,omitempty"`
	ACPDurTime float64       `json:"ACPDurTime,omitempty"`
	IrrACPTime float64       `json:"IrrACPTime,omitempty"`
	Capacity   float64       `json:"Capacity,omitempty"`
	HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

type YearlyDoc struct {
	ID         bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Timestamp  int64         `json:"Timestamp,omitempty"`     // 數據採集時間
	StationID  string        `json:"Station_ID,omitempty"`    //案場編號
	InverterID uint8         `json:"InverterID,omitempty"`    // 逆變器編號
	ACPYear    float64       `json:"ACP_Year,omitempty"`
	ACPLife    float64       `json:"ACP_Life,omitempty"`
	Freq       float64       `json:"Freq,omitempty"`
	InvTemp    float64       `json:"InvTemp,omitempty"`
	PVTemp     float64       `json:"PVTemp,omitempty"`
	Irr        float64       `json:"Irr,omitempty"`
	InvEff     float64       `json:"Inv_Eff,omitempty"`
	PR         float64       `json:"PR,omitempty"`
	APR        float64       `json:"APR,omitempty"`
	ACPkkp     float64       `json:"kWh_kWp,omitempty"`
	IrrDurTime float64       `json:"IrrDurTime,omitempty"`
	ACPDurTime float64       `json:"ACPDurTime,omitempty"`
	IrrACPTime float64       `json:"IrrACPTime,omitempty"`
	Capacity   float64       `json:"Capacity,omitempty"`
	HasAlert   bool          `json:"HasAlert" bson:"-"` // 是否有告警
}

func getMappings(refresh bool) string {
	if !refresh && SolarFormMapping != "" {
		return SolarFormMapping
	}
	mappings := db.GetMapping(SolarFormType)
	ss := SolarSearch{}
	ss.getMapping(mappings[SolarFormType])
	index := db.GetIndex(1, 0, &mappings)
	indexJSON, _ := json.Marshal(index)
	return string(indexJSON)
}

func (s SolarSearch) getMapping(props *db.Properties) {
	props.AddProperty("ID", db.GetTypeProperty("keyword")).
		AddProperty("InverterID", db.GetTypeProperty("long")).
		AddProperty("Station_ID", db.GetTypeProperty("keyword")).
		AddProperty("DCA", db.GetTypeProperty("float")).
		AddProperty("DCV", db.GetTypeProperty("float")).
		AddProperty("DCP", db.GetTypeProperty("float")).
		AddProperty("ACA", db.GetTypeProperty("float")).
		AddProperty("ACV", db.GetTypeProperty("float")).
		AddProperty("ACP", db.GetTypeProperty("float")).
		AddProperty("Freq", db.GetTypeProperty("float")).
		AddProperty("ACP_RealTime", db.GetTypeProperty("float")).
		AddProperty("ACP_Daily", db.GetTypeProperty("float")).
		AddProperty("ACP_Life", db.GetTypeProperty("float")).
		AddProperty("Inv_Eff", db.GetTypeProperty("float")).
		AddProperty("Inverter_Temp", db.GetTypeProperty("float")).
		AddProperty("AvgPVTemp", db.GetTypeProperty("float")).
		AddProperty("PR", db.GetTypeProperty("float")).
		AddProperty("PA", db.GetTypeProperty("float")).
		AddProperty("Capacity", db.GetTypeProperty("float")).
		AddProperty("AvgIrr", db.GetTypeProperty("float")).
		AddProperty("IrrDurTime", db.GetTypeProperty("long")).
		AddProperty("ACPDurTime", db.GetTypeProperty("long")).
		AddProperty("Inverter_Status", db.GetTypeProperty("long")).
		//AddProperty("Environment", db.GetTypeProperty("nested")).
		//AddProperty("Alarm", db.GetTypeProperty("nested")).
		AddProperty("Timestamp", db.GetTypeProperty("date"), &db.Property{Key: "format", Value: "epoch_second"}).
		AddProperty("Upload_Timestamp", db.GetTypeProperty("date"), &db.Property{Key: "format", Value: "epoch_second"})

}

func (s SolarSearch) Put() (bool, error) {
	//fmt.Println(wsDi.Elastic)

	_, err := wsDi.Elastic.Put(SolarFormIndex, SolarFormType, s.ID.Hex(), s.ToJsonStr())
	if err != nil {
		wsDi.Log.Err(err.Error())
		return false, err
	}
	return true, nil
}

func (s SolarSearch) Delete() bool {
	ok, err := wsDi.Elastic.Delete(SolarFormIndex, SolarFormType, s.ID.Hex())
	if err != nil {
		wsDi.Log.Err(err.Error())
	}
	return ok
}

func getDocObjectId(StationID string, InverterID uint8, Timestamp int64) bson.ObjectId {
	var b [12]byte
	// Timestamp, 4 bytes, big endian
	timestamp := time.Unix(Timestamp, 0)
	binary.BigEndian.PutUint32(b[:], uint32(timestamp.Unix()))

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
	b[11] = InverterID
	return bson.ObjectId(b[:])
}

func (s SolarSearch) StaticHour(stationID string, startT int64, endT int64) (*[]SearchResult, error) {

	//aggs = aggs.NewTermsAggregation().Field("InverterID").OrderByAggregation("InverterID", false)
	fmt.Println(startT)
	fmt.Println(endT)
	//aggs = aggs.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	aggInvID := search.NewTermsAggregation().Field("InverterID").OrderByTermAsc()
	aggInvID = aggInvID.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	aggInvID = aggInvID.SubAggregation("Avg_ACP", search.NewAvgAggregation().Field("ACP"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Eff", search.NewAvgAggregation().Field("Inv_Eff"))
	aggInvID = aggInvID.SubAggregation("Avg_Irr", search.NewAvgAggregation().Field("Avg_Irr"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPRT", search.NewSumAggregation().Field("ACP_RealTime"))
	aggInvID = aggInvID.SubAggregation("Avg_ACPDaily", search.NewMaxAggregation().Field("ACP_Daily"))
	aggInvID = aggInvID.SubAggregation("Avg_ACPLife", search.NewMaxAggregation().Field("ACP_Life"))
	aggInvID = aggInvID.SubAggregation("Avg_Freq", search.NewAvgAggregation().Field("Freq"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Temp", search.NewAvgAggregation().Field("InvTemp"))
	aggInvID = aggInvID.SubAggregation("Avg_PVTemp", search.NewAvgAggregation().Field("PVTemp"))
	aggInvID = aggInvID.SubAggregation("Sum_IrrDurTime", search.NewSumAggregation().Field("IrrDurTime"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPDurTime", search.NewSumAggregation().Field("ACPDurTime"))
	histogram := search.NewDateHistogramAggregation().Field("Timestamp").Interval("hour").Format("yyyy-MM-dd HH:00:00")
	aggs := histogram.SubAggregation("aggInvID", aggInvID)

	matchQuery := search.NewMatchQuery("Station_ID", stationID)

	filter := search.NewRangeQuery("Timestamp").Gte(startT).Lt(endT)

	client := wsDi.Elastic.GetClient()
	result, err := client.Search().
		Index(SolarFormIndex).Type(SolarFormType).Size(0).
		Query(search.NewBoolQuery().Must(matchQuery).Filter(filter)).
		Aggregation("static", aggs).
		Do(context.Background())

	if err != nil {
		return nil, err
	}
	staticHour, _ := result.Aggregations.DateHistogram("static") //buckets 桶
	var returnResult []SearchResult
	//returnResult = append(returnResult, string(staticHour))
	mongo := doc.GetMongo()

	station := doc.Station{}
	err = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	if err != nil {
		return nil, err
	}
	for _, bucket := range staticHour.Buckets {
		aggInv, err := bucket.Aggregations.Terms("aggInvID")
		if !err {
			//fmt.Println(aggInv)
			//fmt.Println(err)
			return nil, errors.New("Newterms InverterID not found")
		}
		for _, Invbucket := range aggInv.Buckets {
			staticResult := make(map[string]interface{})
			//staticResult["Timestamp"] = bucket.KeyAsString
			staticResult["Timestamp"] = int64(bucket.Key) / 1000 // int64 Timestamp
			staticResult["InverterID"] = Invbucket.Key
			staticResult["DocCount"] = Invbucket.DocCount
			DCP, _ := Invbucket.Aggregations.Avg("Avg_DCP")
			staticResult["DCP"] = *DCP.Value
			ACP, _ := Invbucket.Aggregations.Avg("Avg_ACP")
			staticResult["ACP"] = *ACP.Value
			InvEff, _ := Invbucket.Aggregations.Avg("Avg_Inv_Eff")
			staticResult["Inv_Eff"] = *InvEff.Value
			Irr, _ := Invbucket.Aggregations.Avg("Avg_Irr")
			staticResult["Irr"] = *Irr.Value
			ACPHour, _ := Invbucket.Aggregations.Sum("Sum_ACPRT")
			staticResult["ACP_Hours"] = *ACPHour.Value
			ACPDaily, _ := Invbucket.Aggregations.Max("Avg_ACPDaily")
			staticResult["ACP_Daily"] = *ACPDaily.Value
			ACPLife, _ := Invbucket.Aggregations.Max("Avg_ACPLife")
			staticResult["ACP_Life"] = *ACPLife.Value
			Freq, _ := Invbucket.Aggregations.Avg("Avg_Freq")
			staticResult["Freq"] = *Freq.Value
			InvTemp, _ := Invbucket.Aggregations.Avg("Avg_Inv_Temp")
			staticResult["InvTemp"] = *InvTemp.Value
			PVTemp, _ := Invbucket.Aggregations.Avg("Avg_PVTemp")
			staticResult["PVTemp"] = *PVTemp.Value
			IrrDurTime, _ := Invbucket.Aggregations.Sum("Sum_IrrDurTime")
			staticResult["IrrDurTime"] = *IrrDurTime.Value
			ACPDurTime, _ := Invbucket.Aggregations.Sum("Sum_ACPDurTime")
			staticResult["ACPDurTime"] = *ACPDurTime.Value
			cap := 0.0

			//invID, ok := Invbucket.Key.(int)
			//fmt.Println("invID", Invbucket.Key.(float64))
			/*if !ok {
				return nil, errors.New("InverterID is not a number")
			}*/
			for _, invarr := range station.Inverter {
				if float64(invarr.ID) == Invbucket.Key.(float64) {
					cap = invarr.ModuleCap
					fmt.Println(cap)
				}
			}
			//fmt.Println(cap)
			if cap == 0.0 {
				staticResult["PR"] = 0

			} else {
				staticResult["PR"] = (*ACPHour.Value / (*Irr.Value * cap)) * 100
			}

			staticResult["Capacity"] = cap
			returnResult = append(returnResult, SearchResult(staticResult))
		}
		//fmt.Println(bucket)
		//staticResult["OnlineRate"] = float64(bucket.DocCount) / DataCountPerHour
		/*DCP, _ := bucket.Aggregations.Avg("Avg_DCP")
		staticResult["DCP"] = *DCP.Value

		returnResult = append(returnResult, SearchResult(staticResult))*/
	}

	return &returnResult, nil
}

func (s SolarSearch) StaticHourReport(stationID string, startT int64, endT int64) (*[]HourlyDoc, error) {

	aggInvID := search.NewTermsAggregation().Field("InverterID").OrderByTermAsc()
	aggInvID = aggInvID.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	aggInvID = aggInvID.SubAggregation("Avg_ACP", search.NewAvgAggregation().Field("ACP"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Eff", search.NewAvgAggregation().Field("Inv_Eff"))
	aggInvID = aggInvID.SubAggregation("Avg_Irr", search.NewAvgAggregation().Field("Avg_Irr"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPRT", search.NewSumAggregation().Field("ACP_RealTime"))
	aggInvID = aggInvID.SubAggregation("Avg_ACPDaily", search.NewMaxAggregation().Field("ACP_Daily"))
	aggInvID = aggInvID.SubAggregation("Avg_ACPLife", search.NewMaxAggregation().Field("ACP_Life"))
	aggInvID = aggInvID.SubAggregation("Avg_Freq", search.NewAvgAggregation().Field("Freq"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Temp", search.NewAvgAggregation().Field("InvTemp"))
	aggInvID = aggInvID.SubAggregation("Avg_PVTemp", search.NewAvgAggregation().Field("PVTemp"))
	aggInvID = aggInvID.SubAggregation("Sum_IrrDurTime", search.NewSumAggregation().Field("IrrDurTime"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPDurTime", search.NewSumAggregation().Field("ACPDurTime"))
	histogram := search.NewDateHistogramAggregation().Field("Timestamp").Interval("hour").Format("yyyy-MM-dd HH:00:00")

	aggs := histogram.SubAggregation("aggInvID", aggInvID)

	matchQuery := search.NewMatchQuery("Station_ID", stationID)

	filter := search.NewRangeQuery("Timestamp").Gte(startT).Lt(endT)

	client := wsDi.Elastic.GetClient()
	result, err := client.Search().
		Index(SolarFormIndex).Type(SolarFormType).Size(0).
		Query(search.NewBoolQuery().Must(matchQuery).Filter(filter)).
		Aggregation("static", aggs).
		Do(context.Background())

	if err != nil {
		return nil, err
	}
	staticHour, _ := result.Aggregations.DateHistogram("static") //buckets 桶
	var returnResult []HourlyDoc
	//returnResult = append(returnResult, string(staticHour))
	mongo := doc.GetMongo()

	station := doc.Station{}
	err = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	if err != nil {
		return nil, err
	}
	for _, bucket := range staticHour.Buckets {
		aggInv, err := bucket.Aggregations.Terms("aggInvID")
		if !err {
			//fmt.Println(aggInv)
			//fmt.Println(err)
			return nil, errors.New("Newterms InverterID not found")
		}
		for _, Invbucket := range aggInv.Buckets {
			staticResult := HourlyDoc{}
			//staticResult["Timestamp"] = bucket.KeyAsString
			staticResult.Timestamp = int64(bucket.Key) / 1000 // int64 Timestamp
			staticResult.InverterID = uint8(Invbucket.Key.(float64))
			staticResult.StationID = stationID
			staticResult.ID = getDocObjectId(stationID, staticResult.InverterID, staticResult.Timestamp)
			DCP, _ := Invbucket.Aggregations.Avg("Avg_DCP")
			staticResult.DCP = *DCP.Value
			ACP, _ := Invbucket.Aggregations.Avg("Avg_ACP")
			staticResult.ACP = *ACP.Value
			InvEff, _ := Invbucket.Aggregations.Avg("Avg_Inv_Eff")
			staticResult.InvEff = *InvEff.Value
			Irr, _ := Invbucket.Aggregations.Avg("Avg_Irr")
			staticResult.Irr = *Irr.Value
			ACPHour, _ := Invbucket.Aggregations.Sum("Sum_ACPRT")
			staticResult.ACPHours = *ACPHour.Value
			ACPDaily, _ := Invbucket.Aggregations.Max("Avg_ACPDaily")
			staticResult.ACPDaily = *ACPDaily.Value
			ACPLife, _ := Invbucket.Aggregations.Max("Avg_ACPLife")
			staticResult.ACPLife = *ACPLife.Value
			Freq, _ := Invbucket.Aggregations.Avg("Avg_Freq")
			staticResult.Freq = *Freq.Value
			InvTemp, _ := Invbucket.Aggregations.Avg("Avg_Inv_Temp")
			staticResult.InvTemp = *InvTemp.Value
			PVTemp, _ := Invbucket.Aggregations.Avg("Avg_PVTemp")
			staticResult.PVTemp = *PVTemp.Value
			IrrDurTime, _ := Invbucket.Aggregations.Sum("Sum_IrrDurTime")
			staticResult.IrrDurTime = *IrrDurTime.Value
			ACPDurTime, _ := Invbucket.Aggregations.Sum("Sum_ACPDurTime")
			staticResult.ACPDurTime = *ACPDurTime.Value
			cap := 0.0

			//invID, ok := Invbucket.Key.(int)
			//fmt.Println("invID", Invbucket.Key.(float64))
			/*if !ok {
				return nil, errors.New("InverterID is not a number")
			}*/
			for _, invarr := range station.Inverter {
				if float64(invarr.ID) == Invbucket.Key.(float64) {
					cap = invarr.ModuleCap
					fmt.Println(cap)
				}
			}
			//fmt.Println(cap)
			if cap == 0.0 {
				staticResult.PR = 0

			} else {
				staticResult.PR = (*ACPHour.Value / (*Irr.Value * cap)) * 100
			}

			staticResult.Capacity = cap
			returnResult = append(returnResult, staticResult)
		}
		//fmt.Println(bucket)
		//staticResult["OnlineRate"] = float64(bucket.DocCount) / DataCountPerHour
		/*DCP, _ := bucket.Aggregations.Avg("Avg_DCP")
		staticResult["DCP"] = *DCP.Value

		returnResult = append(returnResult, SearchResult(staticResult))*/
	}

	return &returnResult, nil
}

/*
func (s SolarSearch) StaticDailyReport(stationID string, startT int64, endT int64) (*[]HourlyDoc, error) {

	pipeline := []bson.D{
		bson.D{
			{"$match",
				bson.M{
					"timestamp": bson.M{"$lt": 1498504000, "$gte": 1493504000},
				},
			},
		}
	}
	query := bson.D{
		{"aggregate", "log"},
		{"pipeline", pipeline},
	}

	var res interface{}
	// Db() returns *mgo.Database
	err = getMongo().DB(zbtDb).C(HourTbl).Run(query, &res)
	if nil != err {
		fmt.Fprintf(w, "ERR: %v", err)
	} else {
		fmt.Fprintf(w, "%v", res)
	}

}*/

func (s SolarSearch) aggDay(stationID string, InvID int, startT time.Time, endT time.Time) (*[]SearchResult, error) {

	histogram := search.NewDateHistogramAggregation().Field("Timestamp").Interval("hour").Format("yyyy-MM-dd HH:00:00")

	aggs := histogram.SubAggregation("Avg_Inv_Eff", search.NewAvgAggregation().Field("Inv_Eff"))
	aggs = aggs.SubAggregation("Sum_Irr", search.NewSumAggregation().Field("Avg_Irr"))
	aggs = aggs.SubAggregation("Max_ACPDaily", search.NewMaxAggregation().Field("ACP_Daily"))
	aggs = aggs.SubAggregation("Max_ACPLife", search.NewMaxAggregation().Field("ACP_Life"))
	aggs = aggs.SubAggregation("Avg_Freq", search.NewAvgAggregation().Field("Freq"))
	aggs = aggs.SubAggregation("Avg_Inv_Temp", search.NewAvgAggregation().Field("InvTemp"))
	aggs = aggs.SubAggregation("Avg_PVTemp", search.NewAvgAggregation().Field("PVTemp"))
	aggs = aggs.SubAggregation("Sum_IrrDurTime", search.NewSumAggregation().Field("IrrDurTime"))
	aggs = aggs.SubAggregation("Sum_ACPDurTime", search.NewSumAggregation().Field("ACPDurTime"))

	/*
		aggInvID := search.NewTermsAggregation().Field("InverterID").OrderByTermAsc()
		//aggInvID = aggInvID.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
		//aggInvID = aggInvID.SubAggregation("Avg_ACP", search.NewAvgAggregation().Field("ACP"))
		aggInvID = aggInvID.SubAggregation("Avg_Inv_Eff", search.NewAvgAggregation().Field("Inv_Eff"))
		aggInvID = aggInvID.SubAggregation("Sum_Irr", search.NewSumAggregation().Field("Irr"))
		//aggInvID = aggInvID.SubAggregation("Sum_ACPHours", search.NewSumAggregation().Field("ACPHours"))
		aggInvID = aggInvID.SubAggregation("Max_ACPDaily", search.NewMaxAggregation().Field("ACP_Daily"))
		aggInvID = aggInvID.SubAggregation("Max_ACPLife", search.NewMaxAggregation().Field("ACP_Life"))
		aggInvID = aggInvID.SubAggregation("Avg_Freq", search.NewAvgAggregation().Field("Freq"))
		aggInvID = aggInvID.SubAggregation("Avg_Inv_Temp", search.NewAvgAggregation().Field("InvTemp"))
		aggInvID = aggInvID.SubAggregation("Avg_PVTemp", search.NewAvgAggregation().Field("PVTemp"))
		aggInvID = aggInvID.SubAggregation("Sum_IrrDurTime", search.NewSumAggregation().Field("IrrDurTime"))
		aggInvID = aggInvID.SubAggregation("Sum_ACPDurTime", search.NewSumAggregation().Field("ACPDurTime"))

		aggs = aggs.SubAggregation("aggInvID", aggInvID)*/

	matchStationQuery := search.NewMatchQuery("Station_ID", stationID)
	matchInvQuery := search.NewMatchQuery("Station_ID", stationID)
	//search.NewMultiMatchQuery()
	filter := search.NewRangeQuery("Timestamp").Gte(startT.Unix()).Lt(endT.Unix())

	client := wsDi.Elastic.GetClient()
	result, err := client.Search().
		Index(SolarFormIndex).Type(SolarFormType).Size(0).
		Query(search.NewBoolQuery().Must(matchStationQuery, matchInvQuery).Filter(filter)).
		Aggregation("static", aggs).Aggregation("static", aggs).
		Do(context.Background())

	if err != nil {
		return nil, err
	}
	staticHour, _ := result.Aggregations.DateHistogram("static") //buckets 桶
	var returnResult []SearchResult
	//returnResult = append(returnResult, string(staticHour))
	mongo := doc.GetMongo()
	station := doc.Station{}
	err = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	if err != nil {
		return nil, err
	}

	for _, bucket := range staticHour.Buckets {
		aggInv, err := bucket.Aggregations.Terms("aggInvID")
		if !err {
			//fmt.Println(aggInv)
			//fmt.Println(err)
			return nil, errors.New("Newterms InverterID not found")
		}
		for _, Invbucket := range aggInv.Buckets {
			staticResult := make(map[string]interface{})
			staticResult["Timestamp"] = bucket.KeyAsString
			staticResult["InverterID"] = Invbucket.Key
			staticResult["DocCount"] = Invbucket.DocCount
			InvEff, _ := Invbucket.Aggregations.Avg("Avg_Inv_Eff")
			staticResult["Inv_Eff"] = *InvEff.Value
			Irr, _ := Invbucket.Aggregations.Avg("Avg_Irr")
			staticResult["Irr"] = *Irr.Value
			ACPDaily, _ := Invbucket.Aggregations.Max("Max_ACPDaily")
			staticResult["ACP_Daily"] = *ACPDaily.Value
			ACPLife, _ := Invbucket.Aggregations.Max("Max_ACPLife")
			staticResult["ACP_Life"] = *ACPLife.Value
			Freq, _ := Invbucket.Aggregations.Avg("Avg_Freq")
			staticResult["Freq"] = *Freq.Value
			InvTemp, _ := Invbucket.Aggregations.Avg("Avg_Inv_Temp")
			staticResult["InvTemp"] = *InvTemp.Value
			PVTemp, _ := Invbucket.Aggregations.Avg("Avg_PVTemp")
			staticResult["PVTemp"] = *PVTemp.Value
			IrrDurTime, _ := Invbucket.Aggregations.Sum("Sum_IrrDurTime")
			staticResult["IrrDurTime"] = *IrrDurTime.Value
			ACPDurTime, _ := Invbucket.Aggregations.Sum("Sum_ACPDurTime")
			staticResult["ACPDurTime"] = *ACPDurTime.Value
			staticResult["IrrACPTime"] = *ACPDurTime.Value * *IrrDurTime.Value * 100

			cap := 0.0
			//get capacity
			for _, invarr := range station.Inverter {
				if float64(invarr.ID) == Invbucket.Key.(float64) {
					cap = invarr.ModuleCap
					fmt.Println(cap)
				}
			}

			//fmt.Println(cap)
			if cap == 0.0 {
				staticResult["PR"] = 0
				staticResult["kWh_kWp"] = 0

			} else {
				staticResult["PR"] = (*ACPDaily.Value / (*Irr.Value * cap)) * 100
				staticResult["kWh_kWp"] = *ACPDaily.Value / cap
			}

			staticResult["Capacity"] = cap

			//get ensure acp
			ensacp, err := s.getEnsureACP(stationID)
			if err != nil {
				return nil, err
			}
			staticResult["APR"] = (*ACPDaily.Value / cap) / ensacp

			returnResult = append(returnResult, SearchResult(staticResult))
		}
		//fmt.Println(bucket)
		//staticResult["OnlineRate"] = float64(bucket.DocCount) / DataCountPerHour
		/*DCP, _ := bucket.Aggregations.Avg("Avg_DCP")
		staticResult["DCP"] = *DCP.Value

		returnResult = append(returnResult, SearchResult(staticResult))*/
	}

	return &returnResult, nil
}

func (s SolarSearch) aggMonth(aggs *search.DateHistogramAggregation, stationID string, startT time.Time, endT time.Time) (*[]SearchResult, error) {

	//aggs = aggs.NewTermsAggregation().Field("InverterID").OrderByAggregation("InverterID", false)
	fmt.Println(startT)
	fmt.Println(endT)
	//aggs = aggs.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	aggInvID := search.NewTermsAggregation().Field("InverterID").OrderByTermAsc()
	//aggInvID = aggInvID.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	//aggInvID = aggInvID.SubAggregation("Avg_ACP", search.NewAvgAggregation().Field("ACP"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Eff", search.NewAvgAggregation().Field("Inv_Eff"))
	aggInvID = aggInvID.SubAggregation("Sum_Irr", search.NewSumAggregation().Field("Avg_Irr"))
	//aggInvID = aggInvID.SubAggregation("Sum_ACPHours", search.NewSumAggregation().Field("ACPHours"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPDaily", search.NewSumAggregation().Field("ACP_Daily"))
	aggInvID = aggInvID.SubAggregation("Max_ACPLife", search.NewMaxAggregation().Field("ACP_Life"))
	aggInvID = aggInvID.SubAggregation("Avg_Freq", search.NewAvgAggregation().Field("Freq"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Temp", search.NewAvgAggregation().Field("InvTemp"))
	aggInvID = aggInvID.SubAggregation("Avg_PVTemp", search.NewAvgAggregation().Field("PVTemp"))
	aggInvID = aggInvID.SubAggregation("Sum_IrrDurTime", search.NewSumAggregation().Field("IrrDurTime"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPDurTime", search.NewSumAggregation().Field("ACPDurTime"))
	aggInvID = aggInvID.SubAggregation("Avg_APR", search.NewAvgAggregation().Field("APR"))
	aggs = aggs.SubAggregation("aggInvID", aggInvID)

	matchQuery := search.NewMatchQuery("Station_ID", stationID)

	filter := search.NewRangeQuery("Timestamp").Gte(startT.Unix()).Lt(endT.Unix())

	client := wsDi.Elastic.GetClient()
	result, err := client.Search().
		Index(SolarFormIndex).Type(SolarFormType).Size(0).
		Query(search.NewBoolQuery().Must(matchQuery).Filter(filter)).
		Aggregation("static", aggs).
		Do(context.Background())

	if err != nil {
		return nil, err
	}
	staticHour, _ := result.Aggregations.DateHistogram("static") //buckets 桶
	var returnResult []SearchResult
	//returnResult = append(returnResult, string(staticHour))
	mongo := doc.GetMongo()

	station := doc.Station{}
	err = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	if err != nil {
		return nil, err
	}

	for _, bucket := range staticHour.Buckets {
		aggInv, err := bucket.Aggregations.Terms("aggInvID")
		if !err {
			//fmt.Println(aggInv)
			//fmt.Println(err)
			return nil, errors.New("Newterms InverterID not found")
		}
		for _, Invbucket := range aggInv.Buckets {
			staticResult := make(map[string]interface{})
			staticResult["Timestamp"] = bucket.KeyAsString
			staticResult["InverterID"] = Invbucket.Key
			staticResult["DocCount"] = Invbucket.DocCount
			InvEff, _ := Invbucket.Aggregations.Avg("Avg_Inv_Eff")
			staticResult["Inv_Eff"] = *InvEff.Value
			Irr, _ := Invbucket.Aggregations.Avg("Avg_Irr")
			staticResult["Irr"] = *Irr.Value
			ACPMonth, _ := Invbucket.Aggregations.Sum("Sum_ACPDaily")
			staticResult["ACP_Month"] = *ACPMonth.Value
			ACPLife, _ := Invbucket.Aggregations.Max("MAX_ACPLife")
			staticResult["ACP_Life"] = *ACPLife.Value
			Freq, _ := Invbucket.Aggregations.Avg("Avg_Freq")
			staticResult["Freq"] = *Freq.Value
			InvTemp, _ := Invbucket.Aggregations.Avg("Avg_Inv_Temp")
			staticResult["InvTemp"] = *InvTemp.Value
			PVTemp, _ := Invbucket.Aggregations.Avg("Avg_PVTemp")
			staticResult["PVTemp"] = *PVTemp.Value
			IrrDurTime, _ := Invbucket.Aggregations.Sum("Sum_IrrDurTime")
			staticResult["IrrDurTime"] = *IrrDurTime.Value
			ACPDurTime, _ := Invbucket.Aggregations.Sum("Sum_ACPDurTime")
			staticResult["ACPDurTime"] = *ACPDurTime.Value
			staticResult["IrrACPTime"] = *ACPDurTime.Value * *IrrDurTime.Value * 100
			APR, _ := Invbucket.Aggregations.Avg("Avg_APR")
			staticResult["APR"] = *APR.Value
			cap := 0.0
			//get capacity
			for _, invarr := range station.Inverter {
				if float64(invarr.ID) == Invbucket.Key.(float64) {
					cap = invarr.ModuleCap
					fmt.Println(cap)
				}
			}

			//fmt.Println(cap)
			if cap == 0.0 {
				staticResult["PR"] = 0
				staticResult["kWh_kWp"] = 0

			} else {
				staticResult["PR"] = (*ACPMonth.Value / (*Irr.Value * cap)) * 100
				staticResult["kWh_kWp"] = *ACPMonth.Value / cap
			}

			staticResult["Capacity"] = cap

			//get ensure acp
			ensacp, err := s.getEnsureACP(stationID)
			if err != nil {
				return nil, err
			}
			staticResult["APR"] = (*ACPMonth.Value / cap) / ensacp

			returnResult = append(returnResult, SearchResult(staticResult))
		}
		//fmt.Println(bucket)
		//staticResult["OnlineRate"] = float64(bucket.DocCount) / DataCountPerHour
		/*DCP, _ := bucket.Aggregations.Avg("Avg_DCP")
		staticResult["DCP"] = *DCP.Value

		returnResult = append(returnResult, SearchResult(staticResult))*/
	}

	return &returnResult, nil
}

func (s SolarSearch) aggYear(aggs *search.DateHistogramAggregation, stationID string, startT time.Time, endT time.Time) (*[]SearchResult, error) {

	//aggs = aggs.NewTermsAggregation().Field("InverterID").OrderByAggregation("InverterID", false)
	fmt.Println(startT)
	fmt.Println(endT)
	//aggs = aggs.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	aggInvID := search.NewTermsAggregation().Field("InverterID").OrderByTermAsc()
	//aggInvID = aggInvID.SubAggregation("Avg_DCP", search.NewAvgAggregation().Field("DCP"))
	//aggInvID = aggInvID.SubAggregation("Avg_ACP", search.NewAvgAggregation().Field("ACP"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Eff", search.NewAvgAggregation().Field("Inv_Eff"))
	aggInvID = aggInvID.SubAggregation("Sum_Irr", search.NewSumAggregation().Field("Avg_Irr"))
	//aggInvID = aggInvID.SubAggregation("Sum_ACPHours", search.NewSumAggregation().Field("ACPHours"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPMonth", search.NewSumAggregation().Field("ACP_Month"))
	aggInvID = aggInvID.SubAggregation("Max_ACPLife", search.NewMaxAggregation().Field("ACP_Life"))
	aggInvID = aggInvID.SubAggregation("Avg_Freq", search.NewAvgAggregation().Field("Freq"))
	aggInvID = aggInvID.SubAggregation("Avg_Inv_Temp", search.NewAvgAggregation().Field("InvTemp"))
	aggInvID = aggInvID.SubAggregation("Avg_PVTemp", search.NewAvgAggregation().Field("PVTemp"))
	aggInvID = aggInvID.SubAggregation("Sum_IrrDurTime", search.NewSumAggregation().Field("IrrDurTime"))
	aggInvID = aggInvID.SubAggregation("Sum_ACPDurTime", search.NewSumAggregation().Field("ACPDurTime"))
	aggInvID = aggInvID.SubAggregation("Avg_APR", search.NewAvgAggregation().Field("APR"))
	aggs = aggs.SubAggregation("aggInvID", aggInvID)

	matchQuery := search.NewMatchQuery("Station_ID", stationID)

	filter := search.NewRangeQuery("Timestamp").Gte(startT.Unix()).Lt(endT.Unix())

	client := wsDi.Elastic.GetClient()
	result, err := client.Search().
		Index(SolarFormIndex).Type(SolarFormType).Size(0).
		Query(search.NewBoolQuery().Must(matchQuery).Filter(filter)).
		Aggregation("static", aggs).
		Do(context.Background())

	if err != nil {
		return nil, err
	}
	staticHour, _ := result.Aggregations.DateHistogram("static") //buckets 桶
	var returnResult []SearchResult
	//returnResult = append(returnResult, string(staticHour))
	mongo := doc.GetMongo()

	station := doc.Station{}
	err = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	if err != nil {
		return nil, err
	}

	for _, bucket := range staticHour.Buckets {
		aggInv, err := bucket.Aggregations.Terms("aggInvID")
		if !err {
			//fmt.Println(aggInv)
			//fmt.Println(err)
			return nil, errors.New("Newterms InverterID not found")
		}
		for _, Invbucket := range aggInv.Buckets {
			staticResult := make(map[string]interface{})
			staticResult["Timestamp"] = bucket.KeyAsString
			staticResult["InverterID"] = Invbucket.Key
			staticResult["DocCount"] = Invbucket.DocCount
			InvEff, _ := Invbucket.Aggregations.Avg("Avg_Inv_Eff")
			staticResult["Inv_Eff"] = *InvEff.Value
			Irr, _ := Invbucket.Aggregations.Avg("Avg_Irr")
			staticResult["Irr"] = *Irr.Value
			ACPYear, _ := Invbucket.Aggregations.Sum("Sum_ACPMonth")
			staticResult["ACP_Year"] = *ACPYear.Value
			ACPLife, _ := Invbucket.Aggregations.Max("Max_ACPLife")
			staticResult["ACP_Life"] = *ACPLife.Value
			Freq, _ := Invbucket.Aggregations.Avg("Avg_Freq")
			staticResult["Freq"] = *Freq.Value
			InvTemp, _ := Invbucket.Aggregations.Avg("Avg_Inv_Temp")
			staticResult["InvTemp"] = *InvTemp.Value
			PVTemp, _ := Invbucket.Aggregations.Avg("Avg_PVTemp")
			staticResult["PVTemp"] = *PVTemp.Value
			IrrDurTime, _ := Invbucket.Aggregations.Sum("Sum_IrrDurTime")
			staticResult["IrrDurTime"] = *IrrDurTime.Value
			ACPDurTime, _ := Invbucket.Aggregations.Sum("Sum_ACPDurTime")
			staticResult["ACPDurTime"] = *ACPDurTime.Value
			staticResult["IrrACPTime"] = *ACPDurTime.Value * *IrrDurTime.Value * 100
			APR, _ := Invbucket.Aggregations.Avg("Avg_APR")
			staticResult["APR"] = *APR.Value
			cap := 0.0
			//get capacity
			for _, invarr := range station.Inverter {
				if float64(invarr.ID) == Invbucket.Key.(float64) {
					cap = invarr.ModuleCap
					fmt.Println(cap)
				}
			}

			//fmt.Println(cap)
			if cap == 0.0 {
				staticResult["PR"] = 0
				staticResult["kWh_kWp"] = 0

			} else {
				staticResult["PR"] = (*ACPYear.Value / (*Irr.Value * cap)) * 100
				staticResult["kWh_kWp"] = *ACPYear.Value / cap
			}

			staticResult["Capacity"] = cap

			//get ensure acp
			ensacp, err := s.getEnsureACP(stationID)
			if err != nil {
				return nil, err
			}
			staticResult["APR"] = (*ACPYear.Value / cap) / ensacp

			returnResult = append(returnResult, SearchResult(staticResult))
		}
		//fmt.Println(bucket)
		//staticResult["OnlineRate"] = float64(bucket.DocCount) / DataCountPerHour
		/*DCP, _ := bucket.Aggregations.Avg("Avg_DCP")
		staticResult["DCP"] = *DCP.Value

		returnResult = append(returnResult, SearchResult(staticResult))*/
	}

	return &returnResult, nil
}

func (s SolarSearch) getEnsureACP(stationID string) (float64, error) {
	mongo := doc.GetMongo()
	station := doc.Station{}
	err := mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	if err != nil {
		return 0, err
	}

	now := time.Now()
	unixtime := now.Unix()
	month := int(now.Month())
	fmt.Println("month", month)
	onlnineDate := station.OnlineDate
	OnlineNum := (unixtime - onlnineDate) / (Min * Sec * Hour)
	fmt.Println("OnlineNum", OnlineNum)

	ensureacp := station.Conf.EnsureACP
	decay := station.Conf.Decay
	corval := 0.0
	for _, monarr := range station.Conf.APRMon {
		if month == monarr.Mon {
			corval = monarr.Value
		}

	}
	coracp := ensureacp * (1 - (float64(OnlineNum/365) * decay * 0.01)) * corval
	return coracp, nil

}

func (s SolarSearch) ToJsonStr() string {
	jsonByte, _ := json.Marshal(s)
	return string(jsonByte)
}
