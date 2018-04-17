package doc

import (
	"encoding/json"
	"net/http"
	"time"

	"dforcepro.com/api"
	"gopkg.in/mgo.v2/bson"
)

const (
	MaintainC    = "Maintain"
	MaintainItem = "MaintainItem"
)

type MaintainAPI bool

func (ma MaintainAPI) Enable() bool {
	return bool(ma)
}

func (ma MaintainAPI) GetAPIs() *[]*api.APIHandler {
	return &[]*api.APIHandler{
		&api.APIHandler{Path: "/v1/maintain", Next: ma.getEndpoint, Method: "GET", Auth: true},
		&api.APIHandler{Path: "/v1/maintain", Next: ma.createEndpoint, Method: "POST", Auth: true},
		&api.APIHandler{Path: "/v1/maintain/item", Next: ma.getitemEndpoint, Method: "GET", Auth: true},
		&api.APIHandler{Path: "/v1/maintain/item", Next: ma.createitemEndpoint, Method: "POST", Auth: false},
	}
}

func (ma MaintainAPI) createEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	maintain := Maintain{}
	station := Station{}
	item := Item{}
	_ = json.NewDecoder(req.Body).Decode(&maintain)
	mongo := getMongo()
	err_ := mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": maintain.StationID}).One(&station)
	if err_ != nil {
		_di.Log.Err(err_.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("can not found station info."))

	} else {
		maintain.City = station.City
		maintain.Town = station.Town
		maintain.Name = station.Name
		_ = mongo.DB(zbtDb).C(MaintainItem).Find(bson.M{"itemid": maintain.ItemID}).One(&item)
		maintain.Item = item.Item
	}

	err := mongo.DB(zbtDb).C(MaintainC).Insert(maintain)

	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("can not insert data."))

	} else {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("success"))
	}

}

func (ma MaintainAPI) createitemEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	pkg := ItemPkg{}
	_ = json.NewDecoder(req.Body).Decode(&pkg)

	mongo := getMongo()
	for _, item := range pkg.Rows {

		err := mongo.DB(zbtDb).C(MaintainItem).Insert(item)
		if err != nil {
			_di.Log.Err(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("can not insert data."))

		}

	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("success"))

}

func (ma MaintainAPI) getEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)

	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}
	Group := req.Header.Get("AuthGroup")
	station := []Station{}
	maintain := []Maintain{}

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
	err_ := mongo.DB(zbtDb).C(MaintainC).Find(bson.M{"$or": queryarr}).All(&maintain)

	if err_ != nil {
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
		return

	} else {

		for _, row := range maintain {
			mapresult := make(map[string]interface{})

			mapresult["SiteName"] = row.Name
			mapresult["City"] = row.City
			mapresult["Town"] = row.Town
			mapresult["Station_ID"] = row.StationID
			mapresult["Item"] = row.Item
			//TODO:判斷tinezone

			mapresult["Maintain_Time"] = time.Unix(row.Time, 0).In(time.FixedZone("GMT", 8*3600)).Format(TimeFormat)
			mapresult["Desc"] = row.Desc

			result = append(result, mapresult)
		}
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)

	}
	_afterEndPoint(w, req)
	return

}

func (ma MaintainAPI) getitemEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)

	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}

	item := []Item{}
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(MaintainItem).Find(bson.M{}).Select(bson.M{"_id": 0}).All(&item)
	if err != nil {
		responseJSON := ItemRes{&item}
		json.NewEncoder(w).Encode(responseJSON)
		return
	}
	responseJSON := ItemRes{&item}
	json.NewEncoder(w).Encode(responseJSON)

	_afterEndPoint(w, req)
	return

}

type Maintain struct {
	Name      string `json:"SiteName,omitempty"`      //案場名稱
	City      string `json:"City,omitempty"`          //縣市
	Town      string `json:"Town,omitempty"`          //鄉鎮
	StationID string `json:"Station_ID,omitempty"`    //案場編號
	Item      string `json:"Item,omitempty"`          //維修保養項目
	ItemID    int    `json:"ItemID,omitempty"`        //維修保養項目
	Time      int64  `json:"Maintain_Time,omitempty"` // 維修保養時間
	Desc      string `json:"Desc,omitempty"`          //敘述

}
type ItemPkg struct {
	Rows []Item `json:"rows,omitempty" bson:"rows"` // DocumentId
}

type ItemRes struct {
	Rows *[]Item `json:"result,omitempty"`
}

type Item struct {
	Item   string `json:"Item,omitempty"`   //項目
	ItemID int    `json:"ItemID,omitempty"` //項目ID

}
