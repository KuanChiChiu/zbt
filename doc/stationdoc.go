package doc

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"dforcepro.com/api"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
)

const (
	StationS = "Station"
	VendorC  = "Vendor"
	CityC    = "City"
	TownC    = "Town"
	//DeviceConfigC = "DeviceConfig"
)

type StationAPI bool

func (sa StationAPI) Enable() bool {
	return bool(sa)
}

func (sa StationAPI) GetAPIs() *[]*api.APIHandler {
	return &[]*api.APIHandler{
		&api.APIHandler{Path: "/v1/station", Next: sa.getEndpoint, Method: "GET", Auth: true},
		&api.APIHandler{Path: "/v1/stationlist", Next: sa.getStationListEndpoint, Method: "GET", Auth: true},
		&api.APIHandler{Path: "/v1/station/{id}/inv", Next: sa.getInvEndpoint, Method: "GET", Auth: true},    //get inv list
		&api.APIHandler{Path: "/v1/stationmulti", Next: sa.createEndpointMulti, Method: "POST", Auth: false}, //for myself
		&api.APIHandler{Path: "/v1/station", Next: sa.createEndpoint, Method: "POST", Auth: false},
		&api.APIHandler{Path: "/v1/station/{id}", Next: sa.getOneEndpoint, Method: "GET", Auth: false},
		&api.APIHandler{Path: "/v1/station/{id}", Next: sa.putEndpoint, Method: "PUT", Auth: false},
		&api.APIHandler{Path: "/v1/station/{id}", Next: sa.deleteEndpoint, Method: "DELETE", Auth: false},
		&api.APIHandler{Path: "/v1/vendor", Next: sa.createVendor, Method: "POST", Auth: false},
		&api.APIHandler{Path: "/v1/city", Next: sa.createCity, Method: "POST", Auth: false},
		&api.APIHandler{Path: "/v1/town", Next: sa.createTown, Method: "POST", Auth: false},
		//&api.APIHandler{Path: "/v1/station/{s_id}/inv", Next: sa.addDeviceEndpoint, Method: "POST", Auth: true},
		//&api.APIHandler{Path: "/v1/station/{s_id}/inv/{d_id}", Next: sa.removeDeviceEndpoint, Method: "DELETE", Auth: true},
	}
}
func (s *Station) GenObjectId() {
	if bson.ObjectId("") == s.ID {

		s.ID = GetStationDocObjectId(s.StationID)
		//s.ID = bson.NewObjectId()
	}
}

func GetStationDocObjectId(StationID string) bson.ObjectId {

	var b [12]byte

	var sum [8]byte
	id := sum[:]
	hw := md5.New()
	hw.Write([]byte(StationID))
	copy(id, hw.Sum(nil))
	//ex T0510602 ->id[1]=0 to id[7]=2
	b[0] = id[0]
	b[1] = id[1]
	b[2] = id[2]
	b[3] = id[3]
	b[4] = id[4]
	b[5] = id[5]
	b[6] = id[6]
	b[7] = id[7]
	b[8] = id[0]
	b[9] = id[1]
	b[10] = id[2]
	b[11] = id[3]
	return bson.ObjectId(b[:])

}

func GetStationDocIdStr(StationID string) string {

	var b [12]byte

	var sum [8]byte
	id := sum[:]
	hw := md5.New()
	hw.Write([]byte(StationID))
	copy(id, hw.Sum(nil))
	//ex T0510602 ->id[1]=0 to id[7]=2
	b[0] = id[0]
	b[1] = id[1]
	b[2] = id[2]
	b[3] = id[3]
	b[4] = id[4]
	b[5] = id[5]
	b[6] = id[6]
	b[7] = id[7]
	b[8] = id[0]
	b[9] = id[1]
	b[10] = id[2]
	b[11] = id[3]
	return string(b[:])

}
func (sa StationAPI) createEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	stationDoc := Station{}
	_ = json.NewDecoder(req.Body).Decode(&stationDoc)
	stationDoc.GenObjectId()
	for i := range stationDoc.Inverter {
		stationDoc.Inverter[i].ChangeDate = stationDoc.Inverter[i].InstallDate

	}

	mongo := getMongo()
	err_ins := mongo.DB(zbtDb).C(StationS).Insert(stationDoc)

	da, err := NewInverterAlert(stationDoc.ID)

	if err != nil {
		_di.Log.Err(err.Error())
	}
	err = da.Save()

	if err != nil {
		_di.Log.Err(err.Error())
	}

	if err_ins != nil {
		// 寫 log 將 Document 寫進資料夾
		_di.Log.WriteFile(fmt.Sprintf("input/%s/%s", StationS, stationDoc.ID.Hex()), toJSONByte(stationDoc))
		_di.Log.Err(err_ins.Error())
		// 回傳錯誤息
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("can not insert data."))
	} else {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("success"))
	}
}

func (sa StationAPI) createEndpointMulti(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	Pkg := StationPkg{}
	_ = json.NewDecoder(req.Body).Decode(&Pkg)
	mongo := getMongo()

	for _, sd := range Pkg.Rows {
		Resform := ConfDoc{UploadData: "True", ConfigFlag: "False", Resend: "False", Version: "V1"}
		var strarr []string
		sd.GenObjectId()
		sd.getCityTownName()
		sd.initUserInfo()
		sd.initConfig()
		sd.TimeZone = 8
		sd.Status = 1
		strarr = append(strarr, "admin")
		sd.Group = strarr
		for i := range sd.Inverter {
			sd.Inverter[i].ChangeDate = sd.Inverter[i].InstallDate
			sd.Inverter[i].getVendorByID()
		}
		err := mongo.DB(zbtDb).C(StationS).Upsert(bson.M{"_id": sd.ID}, sd)
		if err != nil {

			_di.Log.Err(err.Error())
			// 回傳錯誤息
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("can not insert data to station table"))
		}
		Resform.StationID = sd.StationID
		Resform.GenObjectId()

		errconf := mongo.DB(zbtDb).C(SolarRC).Upsert(bson.M{"_id": Resform.ID}, Resform)
		if errconf != nil {

			_di.Log.Err(errconf.Error())
			// 回傳錯誤息
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("can not insert data to rawdata return table"))
		}
		da, err := NewInverterAlert(sd.ID)

		if err != nil {
			_di.Log.Err(err.Error())
		}
		err = da.Save()
		if err != nil {
			_di.Log.Err(err.Error())
		}

	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("success"))

}
func (sa StationAPI) getEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}
	Group := req.Header.Get("AuthGroup")

	var results []interface{}
	mongo := getMongo()

	err := mongo.DB(zbtDb).C(StationS).Find(bson.M{"group": bson.M{"$in": []string{Group}}}).All(&results)

	if err != nil {

		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Auth fail for station."))

	}
	responseJSON := simpleRes{&results}
	json.NewEncoder(w).Encode(responseJSON)
	_afterEndPoint(w, req)
	//w.Write([]byte("get case"))
}

func (sa StationAPI) getStationListEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}
	Group := req.Header.Get("AuthGroup")

	var results []interface{}
	station := []Station{}
	mongo := getMongo()

	err := mongo.DB(zbtDb).C(StationS).Find(bson.M{"group": bson.M{"$in": []string{Group}}}).All(&station)

	if err != nil {

		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Auth fail for station."))
		return
	}
	for _, sta := range station {
		list := StationList{Name: sta.Name, StationID: sta.StationID}
		results = append(results, list)

	}
	responseJSON := simpleRes{&results}
	json.NewEncoder(w).Encode(responseJSON)
	_afterEndPoint(w, req)
}

func (sa StationAPI) getInvEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}
	Group := req.Header.Get("AuthGroup")

	vars := mux.Vars(req)
	stationID := vars["id"]

	var results []interface{}
	station := Station{}
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID, "group": bson.M{"$in": []string{Group}}}).One(&station)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Auth fail for station."))
		responseJSON := simpleRes{&results}
		json.NewEncoder(w).Encode(responseJSON)
		_afterEndPoint(w, req)
		return
	}
	for _, inv := range station.Inverter {

		results = append(results, "Inv "+strconv.Itoa(inv.ID))
	}
	responseJSON := simpleRes{&results}
	json.NewEncoder(w).Encode(responseJSON)
	_afterEndPoint(w, req)
}

func (sa StationAPI) getOneEndpoint(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	stationID := vars["id"]

	if "" == stationID {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var result []interface{}
	var err error
	err = getMongo().DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).All(&result)

	if err != nil {
		_di.Log.Err(err.Error())
	}
	json.NewEncoder(w).Encode(queryRes{&result, 0, 0, 0, 0})

}

func (sa StationAPI) putEndpoint(w http.ResponseWriter, req *http.Request) {

	vars := mux.Vars(req)
	stationID := vars["id"]
	if "" == stationID {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	mystation := Station{}
	_ = json.NewDecoder(req.Body).Decode(&mystation)
	//mystation.GenObjectId()
	err := getMongo().DB(zbtDb).C(StationS).Update(bson.M{"Station_ID": stationID}, bson.M{"$set": mystation})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	} else {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("success"))
	}
}

func (sa StationAPI) deleteEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	vars := mux.Vars(req)
	stationID := vars["id"]
	if "" == stationID {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := getMongo().DB(zbtDb).C(StationS).Remove(bson.M{"Station_ID": stationID})
	if err != nil {
		_di.Log.Err(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("fail"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}

}

func (sa StationAPI) createVendor(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	Pkg := VendorPkg{}
	_ = json.NewDecoder(req.Body).Decode(&Pkg)
	mongo := getMongo()
	for _, vd := range Pkg.Rows {

		err := mongo.DB(zbtDb).C(VendorC).Upsert(bson.M{"id": vd.ID}, vd)
		if err != nil {
			_di.Log.Err(err.Error())
			w.Write([]byte("can not insert data."))

		}
	}
	/*err := mongo.DB(zbtDb).C(VendorC).Insert(Pkg.Rows)
	if err != nil {
		_di.Log.Err(err.Error())
		w.Write([]byte("can not insert data."))

	} else*/
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("success"))

}

func (sa StationAPI) createCity(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	Pkg := CityPkg{}
	_ = json.NewDecoder(req.Body).Decode(&Pkg)
	mongo := getMongo()
	for _, cd := range Pkg.Rows {

		err := mongo.DB(zbtDb).C(CityC).Upsert(bson.M{"cityid": cd.CityID}, cd)
		if err != nil {
			_di.Log.Err(err.Error())
			w.Write([]byte("can not insert data."))

		}
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("success"))

}

func (sa StationAPI) createTown(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	Pkg := TownPkg{}
	_ = json.NewDecoder(req.Body).Decode(&Pkg)
	mongo := getMongo()
	for _, td := range Pkg.Rows {

		err := mongo.DB(zbtDb).C(TownC).Upsert(bson.M{"townid": td.TownID}, td)
		if err != nil {
			_di.Log.Err(err.Error())
			w.Write([]byte("can not insert data."))

		}
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("success"))

}

func (sd *Station) getCityTownName() {

	citydoc := City{}
	towndoc := Town{}
	mongo := getMongo()
	errcity := mongo.DB(zbtDb).C(CityC).Find(bson.M{"cityid": sd.CityID}).One(&citydoc)
	if errcity != nil {
		_di.Log.Err(errcity.Error())
		sd.City = ""
	} else {

		sd.City = citydoc.City
	}
	errtown := mongo.DB(zbtDb).C(TownC).Find(bson.M{"townid": sd.TownID}).One(&towndoc)
	if errtown != nil {
		_di.Log.Err(errtown.Error())
		sd.Town = ""
	} else {

		sd.Town = towndoc.Town
	}
	//return sd
}

//init ensureACP config
func (sd *Station) initConfig() {

	conf := _Config{}
	aprmon := []_APRMon{
		{Mon: 1, Value: 0.8}, {Mon: 2, Value: 0.85},
		{Mon: 3, Value: 0.95}, {Mon: 4, Value: 1.02},
		{Mon: 5, Value: 1.18}, {Mon: 6, Value: 1.3},
		{Mon: 7, Value: 1.26}, {Mon: 8, Value: 1.18},
		{Mon: 9, Value: 0.98}, {Mon: 10, Value: 0.88},
		{Mon: 11, Value: 0.82}, {Mon: 12, Value: 0.78},
	}
	cnthour := []_CountHour{
		{Field: "PVTemp", StartH: 8, StartM: 0, EndH: 18, EndM: 0},
		{Field: "Inv_Eff", StartH: 8, StartM: 0, EndH: 18, EndM: 0},
		{Field: "InvTemp", StartH: 8, StartM: 0, EndH: 18, EndM: 0},
	}
	conf.APRMon = aprmon
	conf.CountHour = cnthour
	conf.EnsureACP = 2.8
	conf.Decay = 1
	conf.IrrLimit = 0.1
	conf.ACPLimit = 0
	sd.Conf = conf

}

func (sd *Station) initUserInfo() {

	charge := _Contact{Name: "RT", Email: "test@gmail.com", Address: "台北市大安區基隆路四段43號", Phone: "0933123456", Tele: "22441234"}

	sd.Charge = charge
	sd.Owner = charge
	sd.Maintenance = charge

}
func (id *_Inv) getVendorByID() {
	vendor := Vendor{}
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(VendorC).Find(bson.M{"id": id.VendorID}).One(&vendor)
	if err != nil {
		_di.Log.Err(err.Error())
		//return nil
	} else {
		id.Vendor = vendor.Vendor
	}

}

type StationPkg struct {
	Rows []Station `json:"rows,omitempty" bson:"rows"` // DocumentId
}

type Station struct {
	ID          bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Name        string        `json:"Name,omitempty"`          // 案場名稱
	Address     string        `json:"Address,omitempty"`       //案場地址
	Capacity    float64       `json:"Capacity,omitempty"`      //案場總容量
	OnlineDate  int64         `json:"OnlineDate,omitempty"`    //案場上線日期
	Longitude   float64       `json:"Longitude,omitempty"`     //經度
	Latitude    float64       `json:"Latitude,omitempty"`      //緯度
	Country     string        `json:"Country,omitempty"`       //國家
	TimeZone    int           `json:"TimeZone,omitempty"`      //時區
	City        string        `json:"City,omitempty"`          //縣市
	CityID      string        `json:"CityID,omitempty"`        //縣市ID
	Town        string        `json:"Town,omitempty"`          //鄉鎮
	TownID      string        `json:"TownID,omitempty"`        //鄉鎮ID
	StationID   string        `json:"Station_ID,omitempty"`    //案場編號
	Status      int           `json:"Status,omitempty"`        //案場狀態（1:綠燈,0:黃燈,-1:紅燈）
	Tilt        float64       `json:"Tilt,omitempty"`          //模組傾斜角
	Azimuth     float64       `json:"Azimuth,omitempty"`       //模組方位角
	Charge      _Contact      `json:"Charge,omitempty"`        //負責人
	Owner       _Contact      `json:"Owner,omitempty"`         //案主
	Maintenance _Contact      `json:"Maintenance,omitempty"`   //維修商
	Inverter    []_Inv        `json:"Inverter,omitempty"`      //逆變器
	Module      []_Mdl        `json:"Module,omitempty"`        //太陽能模組
	Irr         []_Irr        `json:"Irr,omitempty"`           //日射計
	PVTemp      []_PVTemp     `json:"PVTemp,omitempty"`        //模組溫度計
	Env         []_EnvSensor  `json:"Environment,omitempty"`   //環境感測
	Conf        _Config       `json:"Config,omitempty"`        //環境感測
	Group       []string      `json:"Group,omitempty"`         //權限群組
	APRTar      float64       `json:"APRTar"`                  //APR保發目標值

}

type _Coordinates struct {
	Longitude float64 `json:"Longitude,omitempty"`
	Latitude  float64 `json:"Latitude,omitempty"`
}

type _Contact struct {
	Name    string `json:"Name,omitempty"`
	Phone   string `json:"Phone,omitempty"`
	Tele    string `json:"Tele,omitempty"`
	Email   string `json:"Email,omitempty"`
	Address string `json:"Address,omitempty"`
}

type _Inv struct {
	ID          int     `json:"ID,omitempty"`          //逆變器編號
	Vendor      string  `json:"Vendor,omitempty"`      //逆變器供應商
	VendorID    int     `json:"VendorID,omitempty"`    //逆變器設備商ID
	InstallDate int64   `json:"InstallDate,omitempty"` //第一次安裝時間
	ChangeDate  int64   `json:"ChangeDate,omitempty"`  //最後更換時間
	SNnumber    string  `json:"SNnumber,omitempty"`    //SN碼
	ModuleCap   float64 `json:"ModuleCap,omitempty"`   //模組安裝容量
	InvCap      float64 `json:"InvCap,omitempty"`      //逆變器額定容量
	Model       string  `json:"Model,omitempty"`       //型號

}

type _Mdl struct {
	ID     int    `json:"ID,omitempty"`     //模組編號
	Vendor string `json:"Vendor,omitempty"` //模組供應商
	Model  string `json:"Model,omitempty"`  //模組型號

}

type _Irr struct {
	ID     int     `json:"ID,omitempty"`        //日射計編號
	Vendor string  `json:"Vendor,omitempty"`    //日射計供應商
	Model  string  `json:"Model,omitempty"`     //日射計型號
	Tilt   float64 `json:"TiltAngle,omitempty"` //日射計傾斜角

}

type _PVTemp struct {
	ID     int    `json:"ID,omitempty"`     //模組溫度編號
	Vendor string `json:"Vendor,omitempty"` //模組溫度供應商
	Model  string `json:"Model,omitempty"`  //模組溫度型號

}

type _EnvSensor struct {
	ID     int    `json:"ID,omitempty"`     //環境感測編號
	Vendor string `json:"Vendor,omitempty"` //環境感測供應商
	Model  string `json:"Model,omitempty"`  //環境感測型號

}

//保發與績效參數設定
type _Config struct {
	APRMon    []_APRMon    `json:"APRMonth,omitempty"`
	CountHour []_CountHour `json:"CountHour,omitempty"`
	EnsureACP float64      `json:"EnsureACP,omitempty"`
	Decay     float64      `json:"Decay,omitempty"`    //每年衰減％數
	IrrLimit  float64      `json:"IrrLimit,omitempty"` // kWh/m2
	ACPLimit  float64      `json:"ACPLimit,omitempty"` // kw
}

type _APRMon struct {
	//ID    int     `json:"ID,omitempty"`    //月份0~11
	Mon   int     `json:"Month,omitempty"` //月份
	Value float64 `json:"Value,omitempty"` //校正數值

}

type _CountHour struct {
	Field  string `json:"Field,omitempty"`  //欄位
	StartH int    `json:"StartH,omitempty"` //起始小時
	StartM int    `json:"StartM,omitempty"` //起始分鐘
	EndH   int    `json:"EndH,omitempty"`   //結束小時
	EndM   int    `json:"EndM,omitempty"`   //結束分鐘

}

//設備商對應列表
type VendorPkg struct {
	Rows []Vendor `json:"rows,omitempty" bson:"rows"`
}

type Vendor struct {
	ID     int    `json:"ID,omitempty"`     //設備商ID
	Vendor string `json:"Vendor,omitempty"` //設備商

}

//縣市對應列表
type CityPkg struct {
	Rows []City `json:"rows,omitempty" bson:"rows"`
}

type City struct {
	CityID string `json:"CityID,omitempty"` //縣市ID
	City   string `json:"City,omitempty"`   //縣市

}

//鄉鎮對應列表
type TownPkg struct {
	Rows []Town `json:"rows,omitempty" bson:"rows"`
}

type Town struct {
	TownID string `json:"TownID,omitempty"` //鄉鎮ID
	Town   string `json:"Town,omitempty"`   //鄉鎮
	CityID string `json:"CityID,omitempty"` //縣市ID

}

type StationList struct {
	Name      string `json:"Name,omitempty"`
	StationID string `json:"Station_ID,omitempty"`
}
