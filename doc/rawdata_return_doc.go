package doc

import (
	"crypto/md5"
	"encoding/json"
	"net/http"

	"dforcepro.com/api"
	"gopkg.in/mgo.v2/bson"
)

type SolarConfAPI bool

func (sca SolarConfAPI) Enable() bool {
	return bool(sca)
}

func (sca SolarConfAPI) GetAPIs() *[]*api.APIHandler {
	return &[]*api.APIHandler{
		&api.APIHandler{Path: "/v1/conf/rawdata", Next: sca.getRawdataConf, Method: "GET", Auth: false},
		&api.APIHandler{Path: "/v1/conf/rawdata", Next: sca.createRawdataConf, Method: "POST", Auth: false},
		&api.APIHandler{Path: "/v1/conf/rawdata", Next: sca.updateRawdataConf, Method: "PUT", Auth: false},
		&api.APIHandler{Path: "/v1/conf/rawdata", Next: sca.deleteRawdataConf, Method: "DELETE", Auth: false},
	}
}

type ConfDoc struct {
	ID              bson.ObjectId `json:"ID,omitempty" bson:"_id"`                      // DocumentId
	UploadData      string        `json:"Upload_Data,omitempty" bson:"Upload_Data"`     // 資料上傳
	ConfigFlag      string        `json:"Config_Flag,omitempty" bson:"Config_Flag"`     // 設定檔更新
	IP_URL          []string      `json:"IP_URL,omitempty" bson:"IP_URL"`               // 資料中心_IP_1 ~IP_5
	DateTime        int64         `json:"DateTime,omitempty" bson:"DateTime"`           // 今天日期時間
	SendRate        int           `json:"Send_Rate,omitempty" bson:"Send_Rate"`         // 傳送頻率
	GainRate        int           `json:"Gain_Rate,omitempty" bson:"Gain_Rate"`         // 資料採集頻率
	Resend          string        `json:"Resend,omitempty" bson:"Resend"`               // 資料重送
	BackupTime      int           `json:"Backup_Time,omitempty" bson:"Backup_Time"`     // 資料儲放時間
	MAC_Address     string        `json:"MAC_Address,omitempty" bson:"MAC_Address"`     // MAC_Address
	StationID       string        `json:"Station_ID,omitempty" bson:"Station_ID"`       // Station_ID
	ResendTimeStart int64         `json:"Resend_time_S,omitempty" bson:"Resend_time_S"` // 要求特定時間重送資料
	ResendTimeEnd   int64         `json:"Resend_time_E,omitempty" bson:"Resend_time_E"` // 要求特定時間重送資料
	Command         int           `json:"Command,omitempty" bson:"Command"`             // 重置累積功率
	Version         string        `json:"GW_Ver,omitempty" bson:"GW_Ver"`               // 軔體版本確認

}

const (
	SolarRC = "SolarResConf"
)

func (w *ConfDoc) GenObjectId() {
	if bson.ObjectId("") == w.ID {
		var b [12]byte

		var sum [8]byte
		id := sum[:]
		hw := md5.New()
		hw.Write([]byte(w.StationID))
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
		w.ID = bson.ObjectId(b[:])
	}
}

func (sca SolarConfAPI) getRawdataConf(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)

	// TODO: 依 caseID查資料
	//queries := req.URL.Query()

	var results []interface{}
	mongo := getMongo()

	query := mongo.DB(zbtDb).C(SolarRC).Find(bson.M{})
	query.All(&results)

	responseJSON := simpleRes{&results}
	json.NewEncoder(w).Encode(responseJSON)
	_afterEndPoint(w, req)
}

func (sca SolarConfAPI) createRawdataConf(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	var Resform ConfDoc

	_ = json.NewDecoder(req.Body).Decode(&Resform)

	Resform.GenObjectId()
	mongo := getMongo()

	err := mongo.DB(zbtDb).C(SolarRC).Insert(Resform)

	if err != nil {
		// 寫 log 將 Document 寫進資料夾
		_di.Log.WriteFile("document/", toJSONByte(Resform))
		_di.Log.Err(err.Error())
		// 回傳錯誤息
		json.NewEncoder(w).Encode(errorRes{err.Error()})
	} else {
		json.NewEncoder(w).Encode(statusRes{"success"})
	}

	_afterEndPoint(w, req)
}

func (sca SolarConfAPI) updateRawdataConf(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	var Resform ConfDoc

	_ = json.NewDecoder(req.Body).Decode(&Resform)

	Resform.GenObjectId()
	mongo := getMongo()

	err := mongo.DB(zbtDb).C(SolarRC).Update(bson.M{"Station_ID": Resform.StationID}, Resform)

	if err != nil {
		// 寫 log 將 Document 寫進資料夾
		_di.Log.WriteFile("document/", toJSONByte(Resform))
		_di.Log.Err(err.Error())
		// 回傳錯誤息
		json.NewEncoder(w).Encode(errorRes{err.Error()})
	} else {
		json.NewEncoder(w).Encode(statusRes{"update success"})
	}

	_afterEndPoint(w, req)
}

func (sca SolarConfAPI) deleteRawdataConf(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	var Resform ConfDoc

	_ = json.NewDecoder(req.Body).Decode(&Resform)

	Resform.GenObjectId()
	mongo := getMongo()

	err := mongo.DB(zbtDb).C(SolarRC).Remove(bson.M{"Station_ID": Resform.StationID})

	if err != nil {
		// 寫 log 將 Document 寫進資料夾
		_di.Log.WriteFile("document/", toJSONByte(Resform))
		_di.Log.Err(err.Error())
		// 回傳錯誤息
		json.NewEncoder(w).Encode(errorRes{err.Error()})
	} else {
		json.NewEncoder(w).Encode(statusRes{"remove success"})
	}

	_afterEndPoint(w, req)
}
