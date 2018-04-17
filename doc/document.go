package doc

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"dforcepro.com/resource"
	"dforcepro.com/resource/db"
	"github.com/gorilla/mux"
)

type Doc interface {
	initApi(router *mux.Router)
}

type queryRes struct {
	//Status   string         `json:"status,omitempty"` // success
	Rows     *[]interface{} `json:"result,omitempty"`
	Total    int            `json:"total,omitempty"`
	AllPages int            `json:"allPages,omitempty"`
	Page     int            `json:"page,omitempty"`
	Limit    int            `json:"limit,omitempty"`
}

type simpleRes struct {
	//Status string         `json:"status,omitempty"` // success
	Rows *[]interface{} `json:"result,omitempty"`
}

type statusRes struct {
	Status string `json:"status,omitempty"` // success

}

type errorRes struct {
	Message string `json:"Message,omitempty"`
}

var _di *resource.Di

func SetDI(c *resource.Di) {
	_di = c
}

func _beforeEndPoint(w http.ResponseWriter, req *http.Request) {
	// 檢查 DI 是否正確設置
	if _di == nil {
		log.Fatal(errors.New("DI doesn't set. Use func SetDI first"))
	}
}

func toJSONByte(obj interface{}) []byte {
	jsonByte, _ := json.Marshal(obj)
	return jsonByte
}

func toJSONStr(obj interface{}) string {
	return string(toJSONByte(obj))
}

/*func InitAPI(doc Doc, router *mux.Router) {
	doc.initApi(router)
}*/

func getMongo() db.Mongo {
	return _di.Mongodb
}

func GetMongo() db.Mongo {
	return _di.Mongodb
}

func getRedis(redisdb int) *db.Redis {
	return (&_di.Redis).DB(redisdb)
}
func GetRedis(redisdb int) *db.Redis {
	return (&_di.Redis).DB(redisdb)
}

func _afterEndPoint(w http.ResponseWriter, req *http.Request) {

}

func GetByID(c string, id string, i interface{}) (bool, error) {
	err := getMongo().DB(zbtDb).C(c).FindId(id).One(i)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d RawDataInit) Save() error {

	err := getMongo().DB(zbtDb).C(RawData).Insert(&d)
	if err != nil {
		return nil
	}

	return nil
}

func (d RTDoc) Save() error {

	err := getMongo().DB(zbtDb).C(RT).Insert(&d)
	if err != nil {
		return nil
	}

	return nil
}
