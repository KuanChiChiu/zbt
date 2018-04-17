package doc

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"dforcepro.com/api"
	"gopkg.in/mgo.v2/bson"
)

const (
	EventC             = "Event"
	StatusSysAlert     = 1
	StatusUserMaintain = 2
	StatusSysCheck     = 4
)

type EventAPI bool

func (ea EventAPI) getEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	//TODO: 加入判斷token權限
	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}
	Group := req.Header.Get("AuthGroup")

	//AuthGroup := "admin"
	station := []Station{}
	eventDoc := []EventDoc{}
	var result []interface{}
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(StationS).Find(bson.M{"group": bson.M{"$in": []string{Group}}}).All(&station)
	if err != nil {
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
		return
	}

	queryarr := []bson.M{}
	for _, sta := range station {

		queryarr = append(queryarr, bson.M{"stationid": sta.StationID})
	}
	and_query := []bson.M{}
	now := time.Now().Unix()
	and_query = append(and_query, bson.M{"$or": queryarr})
	and_query = append(and_query, bson.M{"eventtimeunix": bson.M{"$gte": now - 7200, "$lte": now}})

	err_ := mongo.DB(zbtDb).C(EventC).Find(bson.M{"$and": and_query}).All(&eventDoc)

	if err_ != nil {
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
		return

	} else {

		for _, row := range eventDoc {
			mapresult := make(map[string]interface{})
			//fmt.Println(row)
			for _, sta := range station {
				if row.StationID == sta.StationID {
					mapresult["City"] = sta.City
					mapresult["Town"] = sta.Town
					mapresult["Name"] = sta.Name
				}
			}
			if row.AlertType == 1 {
				mapresult["AlertType"] = "Alert"
			} else {

			}
			mapresult["AlertType"] = "Warning"

			mapresult["Station_ID"] = row.StationID
			mapresult["Item"] = row.EventTitle
			//TODO:判斷tinezone
			mapresult["CreateTime"] = time.Unix(row.EventTimeUnix, int64(8)).Format(TimeFormat)
			mapresult["SensorID"] = "Inv " + strconv.Itoa(row.InverterID)
			mapresult["Message"] = row.Message
			result = append(result, mapresult)
		}
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)

	}
	_afterEndPoint(w, req)
	return
}

/*
func (ea EventAPI) createEndpoint(w http.ResponseWriter, req *http.Request) {
	// for 新舊系統串接
	eventDoc := EventDoc{}
	err := json.NewDecoder(req.Body).Decode(&eventDoc)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json format error."))
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	tm := time.Unix(eventDoc.EventTimeUnix, 0)
	eventDoc.EventTimeFomat = tm.Format(TimeFormat)
	eventDoc.ID = bson.NewObjectId()
	err = getMongo().DB(YtzDb).C(EventC).Insert(eventDoc)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_di.Log.Err(err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(eventDoc.ID.Hex()))
}*/
/*
func (ea EventAPI) getEndpoint(w http.ResponseWriter, req *http.Request) {
	queryParams := util.GetQueryValue(req, []string{"status", "limit", "page"}, false)
	statusStr, ok := (*queryParams)["status"]
	var statusValue int
	if ok {
		statusValue, ok = statusStr.(int)
		if !ok || statusValue <= 0 || statusValue >= 8 {
			statusValue = 1
		}
	} else {
		statusValue = 1
	}
	group := req.Header.Get("AuthGroup")
	account := req.Header.Get("AuthAccount")
	permission := []string{group, account}
	query := bson.M{
		"status": statusValue,
		"permission": bson.M{
			"$in": permission,
		},
	}
	var resultList []EventDoc
	mongoFind := getMongo().DB(zbtDb).C(EventC).Find(query).Select(bson.M{"permission": 0})
	total, err := mongoFind.Count()
	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if total == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var limit, page = 100, 1

	limitStr, ok := (*queryParams)["limit"]
	if ok {
		limit, ok = limitStr.(int)
		if !ok || limit > 100 || limit < 1 {
			limit = 100
		}
	}

	pageStr, ok := (*queryParams)["page"]
	if ok {
		page, ok = pageStr.(int)
		if !ok {
			page = 1
		}
	}

	paginationRes, err := util.MongoPagination(mongoFind, limit, page)

	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if paginationRes.Total == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	mongoFind.Limit(limit).Skip(page - 1).Sort("-eventtimeunix").All(&resultList)
	paginationRes.Raws = &resultList

	json.NewEncoder(w).Encode(paginationRes)
	_afterEndPoint(w, req)
}*/

type EventDoc struct {
	ID             bson.ObjectId `json:"id,omitempty" bson:"_id"`
	AlertType      int           `json:"alertType,omitempty"`
	EventTimeUnix  int64         `json:"eventTimeUnix,omitempty"`
	EventTimeFomat string        `json:"eventTimeFormat,omitempty" bson:"-"`
	EventTitle     string        `json:"eventTitle,omitempty"`
	StationID      string        `json:"StationID,omitempty"`
	InverterID     int           `json:"InverterID,omitempty"`
	Message        string        `json:"message,omitempty"`
	Status         int           `json:"status,omitempty"` // 狀態 1 系統發出警告; 2 人工維修; 4系統判定回復正常
	Regions        []string      `json:"-"`
	Permission     *[]string     `json:"permission,omitempty"`
}

func (ea EventAPI) Enable() bool {
	return bool(ea)
}

func (ea EventAPI) GetAPIs() *[]*api.APIHandler {
	return &[]*api.APIHandler{
		&api.APIHandler{Path: "/v1/event", Next: ea.getEndpoint, Method: "GET", Auth: true},
		//&api.APIHandler{Path: "/v1/event", Next: ea.createEndpoint, Method: "POST", Auth: false},
	}
}
