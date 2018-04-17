package doc

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"dforcepro.com/alert"
	"dforcepro.com/api"
	"dforcepro.com/resource/db"
	"dforcepro.com/resource/queue"
	"dforcepro.com/util"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/gorilla/mux"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"golang.org/x/net/context/ctxhttp"
)

type SolarAPI bool

func (sa SolarAPI) Enable() bool {
	return bool(sa)
}

//{stationid}/{starttime}/{endtime}

func (sa SolarAPI) GetAPIs() *[]*api.APIHandler {
	return &[]*api.APIHandler{
		&api.APIHandler{Path: "/v1/pwr/rawdata", Next: sa.getEndpoint, Method: "GET", Auth: false},
		&api.APIHandler{Path: "/v1/pwr/rawdata/{id}", Next: sa.getoneEndpoint, Method: "GET", Auth: true},
		&api.APIHandler{Path: "/v1/pwr/rawdata/test/{id}", Next: sa.getoneEndpointtest, Method: "GET", Auth: false},
		&api.APIHandler{Path: "/v1/pwr/rawdata", Next: sa.createEndpoint, Method: "POST", Auth: false},
		&api.APIHandler{Path: "/v1/pwr/rawdata_", Next: sa.createEndpointv2, Method: "POST", Auth: false},
		&api.APIHandler{Path: "/v1/pwr/overview", Next: sa.overviewEndpoint, Method: "GET", Auth: true}, //永旺總攬
		&api.APIHandler{Path: "/v1/pwr/inv/{id}", Next: sa.invEndpoint, Method: "GET", Auth: true},      //永旺逆變器
		&api.APIHandler{Path: "/v1/pwr/getrate", Next: sa.getrate, Method: "GET", Auth: false},
		&api.APIHandler{Path: "/v1/pwr/dailydata", Next: sa.dailydata, Method: "GET", Auth: false},
		&api.APIHandler{Path: "/v1/pwr/testelastic", Next: sa.testelastic, Method: "GET", Auth: false},
	}
}

type Package struct {
	Rows []RTDoc `json:"rows,omitempty"` // RTDoc

}

type PackageRD struct {
	Rows []RawDataInit `json:"rows,omitempty"` // RTDoc

}

type RTDocStr struct {
	ID              bson.ObjectId  `json:"ID,omitempty" bson:"_id"`    // DocumentId
	PreDataID       *bson.ObjectId `json:"PreDataID,omitempty"`        // PreDataID
	Timestamp       int64          `json:"Timestamp,omitempty"`        // 數據採集時間
	UploadTimestamp int64          `json:"Upload_Timestamp,omitempty"` // 資料上傳時間(for中租)
	Datetime        time.Time      `json:"Datetime,omitempty"`         // 數據採集時間
	StationID       string         `json:"Station_ID,omitempty"`       //案場編號(for中租)
	InverterID      int            `json:"InverterID,omitempty"`       // 逆變器編號(for中租)
	MacAddress      string         `json:"MAC_Address,omitempty"`
	CPU             string         `json:"CPU,omitempty"`          // CPU使用率
	Storage         string         `json:"Storage,omitempty"`      // Storage使用率
	Memory          string         `json:"Memory,omitempty"`       // 記憶體使用率
	NetworkFlow     string         `json:"Network_Flow,omitempty"` // 使用上傳/下載流量
	DCPart          []RT_DC_Part   `json:"DC_Part,omitempty"`      //直流測資訊
	DC1DCA          string         `json:"DC1_DCA,omitempty"`      // /*for報表
	DC1DCV          string         `json:"DC1_DCV,omitempty"`
	DC1DCP          string         `json:"DC1_DCP,omitempty"`
	DC2DCA          string         `json:"DC2_DCA,omitempty"`
	DC2DCV          string         `json:"DC2_DCV,omitempty"`
	DC2DCP          string         `json:"DC2_DCP,omitempty"`
	DC3DCA          string         `json:"DC3_DCA,omitempty"`
	DC3DCV          string         `json:"DC3_DCV,omitempty"`
	DC3DCP          string         `json:"DC3_DCP,omitempty"`
	DC4DCA          string         `json:"DC4_DCA,omitempty"`
	DC4DCV          string         `json:"DC4_DCV,omitempty"`
	DC4DCP          string         `json:"DC4_DCP,omitempty"`
	DC5DCA          string         `json:"DC5_DCA,omitempty"`
	DC5DCV          string         `json:"DC5_DCV,omitempty"`
	DC5DCP          string         `json:"DC5_DCP,omitempty"`
	DC6DCA          string         `json:"DC6_DCA,omitempty"`
	DC6DCV          string         `json:"DC6_DCV,omitempty"`
	DC6DCP          string         `json:"DC6_DCP,omitempty"`
	DC7DCA          string         `json:"DC7_DCA,omitempty"`
	DC7DCV          string         `json:"DC7_DCV,omitempty"`
	DC7DCP          string         `json:"DC7_DCP,omitempty"`
	DC8DCA          string         `json:"DC8_DCA"`
	DC8DCV          string         `json:"DC8_DCV"`
	DC8DCP          string         `json:"DC8_DCP"`            // */
	DCA             string         `json:"DCA,omitempty"`      //(for中租)
	DCV             string         `json:"DCV,omitempty"`      //(for中租)
	DCP             string         `json:"DCP,omitempty"`      //(for中租)
	DCPLife         string         `json:"DCP_Life,omitempty"` //(for中租)
	ACPart          []RT_AC_Part   `json:"AC_Part,omitempty"`  //交流測資訊
	L1ACA           string         `json:"L1_ACA,omitempty"`
	L1ACV           string         `json:"L1_ACV,omitempty"`
	L1ACP           string         `json:"L1_ACP,omitempty"`
	L2ACA           string         `json:"L2_ACA,omitempty"`
	L2ACV           string         `json:"L2_ACV,omitempty"`
	L2ACP           string         `json:"L2_ACP,omitempty"`
	L3ACA           string         `json:"L3_ACA,omitempty"`
	L3ACV           string         `json:"L3_ACV,omitempty"`
	L3ACP           string         `json:"L3_ACP,omitempty"`
	ACA             string         `json:"ACA,omitempty"` //(for中租)
	ACV             string         `json:"ACV,omitempty"` //(for中租)
	ACP             string         `json:"ACP,omitempty"` //(for中租)
	ACPRealTime     string         `json:"ACP_RealTime,omitempty"`
	ACPDaily        string         `json:"ACP_Daily,omitempty"` //(for中租)
	ACPLife         string         `json:"ACP_Life,omitempty"`  //(for中租)
	L1ACFreq        string         `json:"L1_AC_Freq,omitempty"`
	L2ACFreq        string         `json:"L2_AC_Freq,omitempty"`
	L3ACFreq        string         `json:"L3_AC_Freq,omitempty"`
	Freq            string         `json:"Freq,omitempty"` //(for中租)
	InvTemp         string         `json:"Inverter_Temp,omitempty"`
	InverterStatus  int            `json:"Inverter_Status,omitempty"` //Inverter狀態
	PVTemp          []PV_Temp      `json:"PVTemp,omitempty"`          //太陽能板模組溫度
	AvgPVTemp       string         `json:"AvgPVTemp,omitempty"`
	Irr             []IRR          `json:"Irr,omitempty"` //日射量
	AvgIrr          string         `json:"AvgIrr,omitempty"`
	InvEff          string         `json:"Inv_Eff,omitempty"`
	Cap             string         `json:"Capacity,omitempty"`
	RA              string         `json:"RA,omitempty"`
	PR              string         `json:"PR,omitempty"`
	Environment     []Environment  `json:"Environment,omitempty"` //環境感測
	Alarm           []string       `json:"Alarm,omitempty"`       //告警訊息
	IrrDurTime      string         `json:"IrrDurTime,omitempty"`
	ACPDurTime      string         `json:"ACPDurTime,omitempty"`
	HasAlert        bool           `json:"HasAlert" bson:"-"` // 是否有告警
}

type RTDoc struct {
	ID              bson.ObjectId  `json:"ID,omitempty" bson:"_id"`    // DocumentId
	PreDataID       *bson.ObjectId `json:"PreDataID,omitempty"`        // PreDataID
	Timestamp       int64          `json:"Timestamp,omitempty"`        // 數據採集時間
	UploadTimestamp int64          `json:"Upload_Timestamp,omitempty"` // 資料上傳時間(for中租)
	Datetime        time.Time      `json:"Datetime,omitempty"`         // 數據採集時間
	StationID       string         `json:"Station_ID,omitempty"`       //案場編號(for中租)
	InverterID      int            `json:"InverterID,omitempty"`       // 逆變器編號(for中租)
	MacAddress      string         `json:"MAC_Address,omitempty"`
	CPU             *float64       `json:"CPU,omitempty"`          // CPU使用率
	Storage         *float64       `json:"Storage,omitempty"`      // Storage使用率
	Memory          *float64       `json:"Memory,omitempty"`       // 記憶體使用率
	NetworkFlow     *float64       `json:"Network_Flow,omitempty"` // 使用上傳/下載流量
	DCPart          []RT_DC_Part   `json:"DC_Part,omitempty"`      //直流測資訊
	DC1DCA          *float64       `json:"DC1_DCA,omitempty"`      // /*for報表
	DC1DCV          *float64       `json:"DC1_DCV,omitempty"`
	DC1DCP          *float64       `json:"DC1_DCP,omitempty"`
	DC2DCA          *float64       `json:"DC2_DCA,omitempty"`
	DC2DCV          *float64       `json:"DC2_DCV,omitempty"`
	DC2DCP          *float64       `json:"DC2_DCP,omitempty"`
	DC3DCA          *float64       `json:"DC3_DCA,omitempty"`
	DC3DCV          *float64       `json:"DC3_DCV,omitempty"`
	DC3DCP          *float64       `json:"DC3_DCP,omitempty"`
	DC4DCA          *float64       `json:"DC4_DCA,omitempty"`
	DC4DCV          *float64       `json:"DC4_DCV,omitempty"`
	DC4DCP          *float64       `json:"DC4_DCP,omitempty"`
	DC5DCA          *float64       `json:"DC5_DCA,omitempty"`
	DC5DCV          *float64       `json:"DC5_DCV,omitempty"`
	DC5DCP          *float64       `json:"DC5_DCP,omitempty"`
	DC6DCA          *float64       `json:"DC6_DCA,omitempty"`
	DC6DCV          *float64       `json:"DC6_DCV,omitempty"`
	DC6DCP          *float64       `json:"DC6_DCP,omitempty"`
	DC7DCA          *float64       `json:"DC7_DCA,omitempty"`
	DC7DCV          *float64       `json:"DC7_DCV,omitempty"`
	DC7DCP          *float64       `json:"DC7_DCP,omitempty"`
	DC8DCA          *float64       `json:"DC8_DCA,omitempty"`
	DC8DCV          *float64       `json:"DC8_DCV,omitempty"`
	DC8DCP          *float64       `json:"DC8_DCP,omitempty"`  // */
	DCA             *float64       `json:"DCA,omitempty"`      //(for中租)
	DCV             *float64       `json:"DCV,omitempty"`      //(for中租)
	DCP             *float64       `json:"DCP,omitempty"`      //(for中租)
	DCPLife         *float64       `json:"DCP_Life,omitempty"` //(for中租)
	ACPart          []RT_AC_Part   `json:"AC_Part,omitempty"`  //交流測資訊
	L1ACA           *float64       `json:"L1_ACA,omitempty"`
	L1ACV           *float64       `json:"L1_ACV,omitempty"`
	L1ACP           *float64       `json:"L1_ACP,omitempty"`
	L2ACA           *float64       `json:"L2_ACA,omitempty"`
	L2ACV           *float64       `json:"L2_ACV,omitempty"`
	L2ACP           *float64       `json:"L2_ACP,omitempty"`
	L3ACA           *float64       `json:"L3_ACA,omitempty"`
	L3ACV           *float64       `json:"L3_ACV,omitempty"`
	L3ACP           *float64       `json:"L3_ACP,omitempty"`
	ACA             *float64       `json:"ACA,omitempty"` //(for中租)
	ACV             *float64       `json:"ACV,omitempty"` //(for中租)
	ACP             *float64       `json:"ACP,omitempty"` //(for中租)
	ACPRealTime     *float64       `json:"ACP_RealTime,omitempty"`
	ACPDaily        *float64       `json:"ACP_Daily,omitempty"` //(for中租)
	ACPLife         *float64       `json:"ACP_Life,omitempty"`  //(for中租)
	L1ACFreq        *float64       `json:"L1_AC_Freq,omitempty"`
	L2ACFreq        *float64       `json:"L2_AC_Freq,omitempty"`
	L3ACFreq        *float64       `json:"L3_AC_Freq,omitempty"`
	Freq            *float64       `json:"Freq,omitempty"` //(for中租)
	InvTemp         *float64       `json:"Inverter_Temp,omitempty"`
	InverterStatus  int            `json:"Inverter_Status,omitempty"` //Inverter狀態
	PVTemp          []PV_Temp      `json:"PVTemp,omitempty"`          //太陽能板模組溫度
	AvgPVTemp       *float64       `json:"AvgPVTemp,omitempty"`
	Irr             []IRR          `json:"Irr,omitempty"`          //日射量
	IrrRealTime     *float64       `json:"Irr_RealTime,omitempty"` //瞬時日射矩形面積(該筆減上筆日射乘時間(s)/2 )
	AvgIrr          *float64       `json:"AvgIrr,omitempty"`
	InvEff          *float64       `json:"Inv_Eff,omitempty"`
	Cap             *float64       `json:"Capacity,omitempty"`
	RA              *float64       `json:"RA,omitempty"`
	PR              *float64       `json:"PR,omitempty"`
	Environment     []Environment  `json:"Environment,omitempty"` //環境感測
	Alarm           []string       `json:"Alarm,omitempty"`       //告警訊息
	IrrDurTime      *float64       `json:"IrrDurTime,omitempty"`
	ACPDurTime      *float64       `json:"ACPDurTime,omitempty"`
	HasAlert        bool           `json:"HasAlert" bson:"-"` // 是否有告警
}

type RawDataInit struct {
	ID              bson.ObjectId `json:"ID,omitempty" bson:"_id"`    // DocumentId
	Timestamp       int64         `json:"Timestamp,omitempty"`        // 數據採集時間
	UploadTimestamp int64         `json:"Upload_Timestamp,omitempty"` // 資料上傳時間(for中租)
	Datetime        time.Time     `json:"Datetime,omitempty"`         // 數據採集時間
	StationID       string        `json:"Station_ID,omitempty"`       //案場編號(for中租)
	InverterID      int           `json:"InverterID,omitempty"`       // 逆變器編號(for中租)
	MacAddress      string        `json:"MAC_Address,omitempty"`
	CPU             *float64      `json:"CPU,omitempty"`          // CPU使用率
	Storage         *float64      `json:"Storage,omitempty"`      // Storage使用率
	Memory          *float64      `json:"Memory,omitempty"`       // 記憶體使用率
	NetworkFlow     *float64      `json:"Network_Flow,omitempty"` // 使用上傳/下載流量
	DCPart          []RT_DC_Part  `json:"DC_Part,omitempty"`      //直流測資訊
	ACPart          []RT_AC_Part  `json:"AC_Part,omitempty"`      //交流測資訊
	ACPDaily        *float64      `json:"ACP_Daily,omitempty"`    //(for中租)
	ACPLife         *float64      `json:"ACP_Life,omitempty"`     //(for中租)
	InvTemp         *float64      `json:"Inverter_Temp,omitempty"`
	InverterStatus  int           `json:"Inverter_Status,omitempty"` //Inverter狀態
	PVTemp          []PV_Temp     `json:"PVTemp,omitempty"`          //太陽能板模組溫度
	Irr             []IRR         `json:"Irr,omitempty"`             //日射量
	Environment     []Environment `json:"Environment,omitempty"`     //環境感測
	Alarm           []string      `json:"Alarm,omitempty"`           //告警訊息
}

type RawDataDoc struct {
	Timestamp  int64    `json:"Timestamp,omitempty"`   // 數據採集時間
	StationID  string   `json:"Station_ID,omitempty"`  //案場編號(for中租)
	InverterID int      `json:"Inverter_NO,omitempty"` // 逆變器編號(for中租)
	DC1DCA     *float64 `json:"DC1_DCA,omitempty"`     // /*for報表
	DC1DCV     *float64 `json:"DC1_DCV,omitempty"`
	DC1DCP     *float64 `json:"DC1_DCP,omitempty"`
	DC2DCA     *float64 `json:"DC2_DCA,omitempty"`
	DC2DCV     *float64 `json:"DC2_DCV,omitempty"`
	DC2DCP     *float64 `json:"DC2_DCP,omitempty"`
	L1ACA      *float64 `json:"L1_ACA,omitempty"`
	L1ACV      *float64 `json:"L1_ACV,omitempty"`
	L1ACP      *float64 `json:"L1_ACP,omitempty"`
	L2ACA      *float64 `json:"L2_ACA,omitempty"`
	L2ACV      *float64 `json:"L2_ACV,omitempty"`
	L2ACP      *float64 `json:"L2_ACP,omitempty"`
	L3ACA      *float64 `json:"L3_ACA,omitempty"`
	L3ACV      *float64 `json:"L3_ACV,omitempty"`
	L3ACP      *float64 `json:"L3_ACP,omitempty"`
	ACPDaily   *float64 `json:"Today_Energy(kWh),omitempty"` //(for中租)
	ACPLife    *float64 `json:"Life_Energy(kWh),omitempty"`  //(for中租)
	L1ACFreq   *float64 `json:"L1_AC_Freq,omitempty"`
	L2ACFreq   *float64 `json:"L2_AC_Freq,omitempty"`
	L3ACFreq   *float64 `json:"L3_AC_Freq,omitempty"`
	InvTemp    *float64 `json:"InvTemp,omitempty"`
	AvgPVTemp  *float64 `json:"AvgPVTemp,omitempty"`
	AvgIrr     *float64 `json:"AvgIrr,omitempty"`
}

type InvRTDoc struct {
	Timestamp    string   `json:"Timestamp"`
	StationID    string   `json:"Station_ID"`
	InverterID   int      `json:"InverterID"`
	DC1DCA       *float64 `json:"DC1_DCA"`
	DC1DCV       *float64 `json:"DC1_DCV"`
	DC1DCP       *float64 `json:"DC1_DCP"`
	DC2DCA       *float64 `json:"DC2_DCA"`
	DC2DCV       *float64 `json:"DC2_DCV"`
	DC2DCP       *float64 `json:"DC2_DCP"`
	ACA          *float64 `json:"ACA"`
	ACV          *float64 `json:"ACV"`
	ACP          *float64 `json:"ACP"`
	ACPDaily     *float64 `json:"ACP_Daily"`
	ACPLife      *float64 `json:"ACP_Life"`
	Freq         *float64 `json:"GridFreq"`
	InvTemp      *float64 `json:"Temp"`
	Irr          *float64 `json:"Irr"`
	InvEff       *float64 `json:"Inv_Eff"`
	PR           *float64 `json:"PR"`
	No           int      `json:"No"`
	RS485ID      int      `json:"RS485ID"`
	Inv_Status   int      `json:"Inv_Status"`
	InverterName string   `json:"InverterName"`
	City         string   `json:"City"`     //縣市
	Town         string   `json:"Town"`     //鄉鎮
	Capacity     *float64 `json:"Capacity"` //案場總容量
	Name         string   `json:"Name"`     //案場名稱

}

type InvRTRes struct {
	Rows *[]InvRTDoc `json:"result,omitempty"`
}

type OverviewRTDoc struct {
	ID             bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	City           string        `json:"City"`                    //縣市
	Town           string        `json:"Town"`                    //鄉鎮
	Timestamp      string        `json:"Timestamp"`
	StationID      string        `json:"Station_ID"`
	Name           string        `json:"Name"`
	DC1DCA         *float64      `json:"DC1DCA"`
	DC1DCV         *float64      `json:"DC1DCV"`
	DC1DCP         *float64      `json:"DC1DCP"`
	DC2DCA         *float64      `json:"DC2DCA"`
	DC2DCV         *float64      `json:"DC2DCV"`
	DC2DCP         *float64      `json:"DC2DCP"`
	DCA            *float64      `json:"DCA"`
	DCV            *float64      `json:"DCV"`
	DCP            *float64      `json:"DCP"`
	ACA            *float64      `json:"ACA"`
	ACV            *float64      `json:"ACV"`
	ACP            *float64      `json:"ACP"`
	ACPDaily       *float64      `json:"ACP_Daily"`
	ACPLife        *float64      `json:"ACP_Life"`
	Status         int           `json:"Status"`
	Capacity       *float64      `json:"Capacity"` //案場總容量
	ACPPerCap      *float64      `json:"Kw_kwp"`   //案場總容量
	ACPDailyPerCap *float64      `json:"Kwh_kwp"`  //案場總容量
	APRTar         *float64      `json:"APRTar"`   //APR保發目標值
	PR             *float64      `json:"PR"`       //PR
}

type OvRTRes struct {
	Rows *[]OverviewRTDoc `json:"result,omitempty"`
}

type DC_Part struct {
	Index   int      `json:"Index,omitempty"`    //MPPT編號
	DCA     *float64 `json:"DCA,omitempty"`      //直流電流
	DCV     *float64 `json:"DCV,omitempty"`      //直流電壓
	DCP     *float64 `json:"DCP,omitempty"`      //直流功率
	DCPLife *float64 `json:"DCP_Life,omitempty"` //累積直流功率
}

type RT_DC_Part struct {
	Index   int      `json:"Index,omitempty"`    //MPPT編號
	DCA     *float64 `json:"DCA,omitempty"`      //直流電流
	DCV     *float64 `json:"DCV,omitempty"`      //直流電壓
	DCP     *float64 `json:"DCP,omitempty"`      //直流功率
	DCPLife *float64 `json:"DCP_Life,omitempty"` //累積直流功率
}

type AC_Part struct {
	Index int      `json:"Index,omitempty"` //交流三相各相編號
	ACA   *float64 `json:"ACA,omitempty"`   //交流電流
	ACV   *float64 `json:"ACV,omitempty"`   //交流電壓
	ACP   *float64 `json:"ACP,omitempty"`   //交流功率
	Freq  *float64 `json:"Freq,omitempty"`  //交流電網頻率
	Phase *float64 `json:"Phase,omitempty"` //交流相位
}

type RT_AC_Part struct {
	Index int      `json:"Index,omitempty"` //交流三相各相編號
	ACA   *float64 `json:"ACA,omitempty"`   //交流電流
	ACV   *float64 `json:"ACV,omitempty"`   //交流電壓
	ACP   *float64 `json:"ACP,omitempty"`   //交流功率
	Freq  *float64 `json:"Freq,omitempty"`  //交流電網頻率
	Phase *float64 `json:"Phase,omitempty"` //交流相位
}

type PV_Temp struct {
	ID     int      `json:"ID,omitempty"`     //模組溫度編號
	Value  *float64 `json:"Value,omitempty"`  //模組溫度數值
	Status int      `json:"Status,omitempty"` // 模組溫度Sensor狀態
}

type PV_Temp_Old struct {
	ID     int `json:"TempID,omitempty"`      //模組溫度編號
	Value  int `json:"Temp,omitempty"`        //模組溫度數值
	Status int `json:"Temp_Status,omitempty"` // 模組溫度Sensor狀態
}

type IRR struct {
	ID     int      `json:"ID,omitempty"`     //日射計編號
	Value  *float64 `json:"Value,omitempty"`  //日射計數值
	Status int      `json:"Status,omitempty"` // 日射計狀態
}

type Environment struct {
	WindSpeed     *float64 `json:"WindSpeed,omitempty"`     //風速數值
	WindDirection *float64 `json:"WindDirection,omitempty"` //風向數值
	Temp          *float64 `json:"Temp,omitempty"`          //溫度計數值
	Humidity      *float64 `json:"Humidity,omitempty"`      //濕度計數值
	Rain          *float64 `json:"Rain,omitempty"`          //降雨量數值
	ID            int      `json:"ID,omitempty"`            //環境感測器編號
	Status        int      `json:"Status,omitempty"`        // 環境感測器狀態
}

type ResDoc struct {
	UploadData      string   `json:"Upload_Data,omitempty" bson:"Upload_Data"`     // 資料上傳
	ConfigFlag      string   `json:"Config_Flag,omitempty" bson:"Config_Flag"`     // 設定檔更新
	IPURL           []string `json:"IP_URL,omitempty" bson:"IP_URL"`               // 資料中心_IP_1 ~IP_5
	DateTime        int64    `json:"DateTime,omitempty" bson:"DateTime"`           // 今天日期時間
	SendRate        int      `json:"Send_Rate,omitempty" bson:"Send_Rate"`         // 傳送頻率
	GainRate        int      `json:"Gain_Rate,omitempty" bson:"Gain_Rate"`         // 資料採集頻率
	Resend          string   `json:"Resend,omitempty" bson:"Resend"`               // 資料重送
	BackupTime      int      `json:"Backup_Time,omitempty" bson:"Backup_Time"`     // 資料儲放時間
	MACAddr         string   `json:"MAC_Address,omitempty" bson:"MAC_Address"`     // MAC_Address
	StationID       string   `json:"Station_ID,omitempty" bson:"Station_ID"`       // Station_ID
	ResendTimeStart int64    `json:"Resend_time_S,omitempty" bson:"Resend_time_S"` // 要求特定時間重送資料
	ResendTimeEnd   int64    `json:"Resend_time_E,omitempty" bson:"Resend_time_E"` // 要求特定時間重送資料
	Command         int      `json:"Command,omitempty" bson:"Command"`             // 重置累積功率
	Version         string   `json:"GW_Ver,omitempty" bson:"GW_Ver"`               // 軔體版本確認
}

type NoConfDoc struct {
	UploadData string `json:"Upload_Data,omitempty" bson:"Upload_Data"` // 資料上傳
	ConfigFlag string `json:"Config_Flag,omitempty" bson:"Config_Flag"` // 設定檔更新
}

type ReportDoc struct {
	ID          bson.ObjectId `json:"ID,omitempty" bson:"_id"` // DocumentId
	Timestamp   int64         `json:"Timestamp,omitempty" `    // 數據採集時間
	StationID   string        `json:"Station_ID,omitempty"`    //案場編號(for中租)
	InverterID  int           `json:"Inverter_NO,omitempty"`   // 逆變器編號(for中租)
	DCA         *float64      `json:"DC_Currnet(A),omitempty"` //(for中租)
	DCV         *float64      `json:"DC_Voltage(V),omitempty"` //(for中租)
	DCP         *float64      `json:"DC_Power(W),omitempty"`   //(for中租)
	ACA         *float64      `json:"AC_Current(A),omitempty"` //(for中租)
	ACV         *float64      `json:"AC_Voltage(V),omitempty"` //(for中租)
	ACP         *float64      `json:"AC_Power(kW),omitempty"`  //(for中租)
	ACPRealTime *float64      `json:"ACP_RealTime,omitempty"`
	ACPDaily    *float64      `json:"Today_Energy(kWh),omitempty"` //(for中租)
	ACPLife     *float64      `json:"Life_Energy(kWh),omitempty"`  //(for中租)
	Freq        *float64      `json:"Frequency(Hz),omitempty"`     //(for中租)
	InvTemp     *float64      `json:"InvTemp,omitempty"`
	PVTemp      *float64      `json:"PVTemp,omitempty"`
	Irr         *float64      `json:"Irr,omitempty"`
	InvEff      *float64      `json:"Inv_Eff,omitempty"`
	RA          *float64      `json:"RA,omitempty"`
	PR          *float64      `json:"PR,omitempty"`
	IrrDurTime  *float64      `json:"IrrDurTime,omitempty"`
	ACPDurTime  *float64      `json:"ACPDurTime,omitempty"`
}

type StationID struct {
	StationID string `json:"Station_ID,omitempty"`
}

type RTStatus struct {
	StationID      string `json:"Station_ID,omitempty"`
	Status         int    `json:"Status,omitempty"`
	LastUploadTime int64  `json:"LastUploadTime,omitempty"`
}

type RateDoc struct {
	Datetime      time.Time `json:"-"`                   // 數據採集時間
	Datestr       string    `json:"timestamp,omitempty"` // 數據採集時間
	StationID     string    `json:"stationid,omitempty"` //案場編號
	Invnum        float64   `json:"-"`
	Acpirrtimesum float64   `json:"-"`
	Cap           float64   `json:"-"`
	Acplife       float64   `json:"acplife"`
	PPR           float64   `json:"ppr"`
	Inveff        float64   `json:"inveff"`
	Irr           float64   `json:"irr"`
	Acpdaily      float64   `json:"acpdaily"`
	PR            float64   `json:"pr"`
	Kkp           float64   `json:"kkp"`
}

type RawData_ map[string]interface{}

const (
	zbtDb            = "zbt"
	RawData          = "RawData"
	Test             = "Test"
	RT               = "RealTime"
	RTInv            = "RTInv"
	RTOverview       = "RTOverview"
	TaskSolarCreated = "TaskSolarCreated"
	TimeFormat       = "2006-01-02 15:04:05"
	TimeFormatapi    = "2006-01-02T15:04:05"
	CurrentConv      = 0.01
	VoltageConv      = 0.1
	WaltConv         = 0.001
	KWhConv          = 0.01
	FreqConv         = 0.01
	OneHundPerc      = 100
	IrrConv          = 0.001
	PVTempConv       = 0.1
	RedisDB_Station  = 4
)

func (sa SolarAPI) getEndpoint(w http.ResponseWriter, req *http.Request) {

	_beforeEndPoint(w, req)

	queries := req.URL.Query()

	var results []interface{}
	mongo := getMongo()

	query := mongo.DB(zbtDb).C(RT).Find(bson.M{})
	total, err := query.Count()
	var limit, page, totalPage = 100, 1, 1

	if err != nil {
		_di.Log.Err(err.Error())
		// TODO:回傳空資料
	} else if total != 0 {

		limitStrAry, ok := queries["limit"]
		if ok {
			_limit, err := strconv.Atoi(limitStrAry[0])
			if err == nil {
				limit = _limit
			}
			if limit > 100 || limit < 1 {
				limit = 100
			}
		}

		pageStrAry, ok := queries["page"]
		if ok {
			_page, err := strconv.Atoi(pageStrAry[0])
			if err == nil {
				page = _page
			}
		}
		totalPage = int(math.Ceil(float64(total) / float64(limit)))

		if page > totalPage {
			page = totalPage
		} else if page < 1 {
			page = 1
		}

		query.Limit(limit).Skip(page - 1).All(&results)
	}

	responseJSON := queryRes{&results, total, totalPage, page, limit}
	json.NewEncoder(w).Encode(responseJSON)
	_afterEndPoint(w, req)
}

func (sa SolarAPI) getoneEndpoint(w http.ResponseWriter, req *http.Request) {

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

	queryMap := util.GetQueryValue(req, []string{"start", "end"}, true)
	startTime := (*queryMap)["start"].(string)

	endTime := (*queryMap)["end"].(string)

	startT, err := strconv.ParseInt(startTime, 10, 64)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	endT, err := strconv.ParseInt(endTime, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rawdata := []RawDataDoc{}
	var result []interface{}
	station := Station{}
	TimeZone := 8
	mongo := getMongo()

	_ = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID, "group": bson.M{"$in": []string{Group}}}).One(&station)
	if err != nil {

		_di.Log.Info("line 452 getoneEndpoint(),Auth error.")
		_di.Log.Info(err.Error())
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)

		return
	}
	TimeZone = station.TimeZone

	err_S := mongo.DB(zbtDb).C(RT).Find(bson.M{"stationid": stationID, "timestamp": bson.M{"$gte": startT, "$lte": endT}}).Sort("timestamp", "inverterid").All(&rawdata)

	if err_S != nil {
		_di.Log.Info("line 464 getoneEndpoint(),rawdata info not found.")
		_di.Log.Info(err_S.Error())
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
		_afterEndPoint(w, req)
		return
	} else {
		for _, row := range rawdata {

			mapresult := make(map[string]interface{})
			mapresult["Timestamp"] = time.Unix(row.Timestamp, 0).In(time.FixedZone("GMT", TimeZone*3600)).Format(TimeFormat)
			mapresult["StationID"] = row.StationID
			mapresult["InverterID"] = row.InverterID
			if row.DC1DCA != nil {
				mapresult["DC1_DCA"] = round(*row.DC1DCA, 2)
			} else {
				mapresult["DC1_DCA"] = nil
			}
			if row.DC1DCV != nil {
				mapresult["DC1_DCV"] = round(*row.DC1DCV, 2)
			} else {
				mapresult["DC1_DCV"] = nil
			}
			if row.DC1DCP != nil {
				mapresult["DC1_DCP"] = round(*row.DC1DCP, 2)
			} else {
				mapresult["DC1_DCP"] = nil
			}
			if row.DC2DCA != nil {
				mapresult["DC2_DCA"] = round(*row.DC2DCA, 2)
			} else {
				mapresult["DC2_DCA"] = nil
			}
			if row.DC2DCV != nil {
				mapresult["DC2_DCV"] = round(*row.DC2DCV, 2)
			} else {
				mapresult["DC2_DCV"] = nil
			}
			if row.DC2DCP != nil {
				mapresult["DC2_DCP"] = round(*row.DC2DCP, 2)
			} else {
				mapresult["DC2_DCP"] = nil
			}
			if row.L1ACA != nil {
				mapresult["L1_ACA"] = round(*row.L1ACA, 2)
			} else {
				mapresult["L1_ACA"] = nil
			}
			if row.L1ACV != nil {
				mapresult["L1_ACV"] = round(*row.L1ACV, 2)
			} else {
				mapresult["L1_ACV"] = nil
			}
			if row.L1ACP != nil {
				mapresult["L1_ACP"] = round(*row.L1ACP, 2)
			} else {
				mapresult["L1_ACP"] = nil
			}
			if row.L2ACA != nil {
				mapresult["L2_ACA"] = round(*row.L2ACA, 2)
			} else {
				mapresult["L2_ACA"] = nil
			}
			if row.L2ACV != nil {
				mapresult["L2_ACV"] = round(*row.L2ACV, 2)
			} else {
				mapresult["L2_ACV"] = nil
			}
			if row.L2ACP != nil {
				mapresult["L2_ACP"] = round(*row.L2ACP, 2)
			} else {
				mapresult["L2_ACP"] = nil
			}
			if row.L3ACA != nil {
				mapresult["L3_ACA"] = round(*row.L3ACA, 2)
			} else {
				mapresult["L3_ACA"] = nil
			}
			if row.L3ACV != nil {
				mapresult["L3_ACV"] = round(*row.L3ACV, 2)
			} else {
				mapresult["L3_ACV"] = nil
			}
			if row.L3ACP != nil {
				mapresult["L3_ACP"] = round(*row.L3ACP, 2)
			} else {
				mapresult["L3_ACP"] = nil
			}

			if row.ACPDaily != nil {
				mapresult["ACPDaily"] = round(*row.ACPDaily, 2)
			} else {
				mapresult["ACPDaily"] = nil
			}
			if row.ACPLife != nil {
				mapresult["ACPLife"] = round(*row.ACPLife, 2)
			} else {
				mapresult["ACPLife"] = nil
			}
			if row.L1ACFreq != nil {
				mapresult["L1_AC_Freq"] = round(*row.L1ACFreq, 2)
			} else {
				mapresult["L1_AC_Freq"] = nil
			}
			if row.L2ACFreq != nil {
				mapresult["L2_AC_Freq"] = round(*row.L2ACFreq, 2)
			} else {
				mapresult["L2_AC_Freq"] = nil
			}
			if row.L3ACFreq != nil {
				mapresult["L3_AC_Freq"] = round(*row.L3ACFreq, 2)
			} else {
				mapresult["L3_AC_Freq"] = nil
			}
			if row.InvTemp != nil {
				mapresult["Inverter_Temp"] = round(*row.InvTemp, 2)
			} else {
				mapresult["Inverter_Temp"] = nil
			}
			if row.AvgPVTemp != nil {
				mapresult["PVTemp"] = round(*row.AvgPVTemp, 2)
			} else {
				mapresult["PVTemp"] = nil
			}
			if row.AvgIrr != nil {
				mapresult["Irr"] = round(*row.AvgIrr*1000, 2)
			} else {
				mapresult["Irr"] = nil
			}
			result = append(result, mapresult)
		}
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
		_afterEndPoint(w, req)
	}

}

func (sa SolarAPI) getoneEndpointtest(w http.ResponseWriter, req *http.Request) {

	_beforeEndPoint(w, req)

	vars := mux.Vars(req)
	stationID := vars["id"]

	queryMap := util.GetQueryValue(req, []string{"start", "end"}, true)
	startTime := (*queryMap)["start"].(string)

	endTime := (*queryMap)["end"].(string)

	startT, err := strconv.ParseInt(startTime, 10, 64)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	endT, err := strconv.ParseInt(endTime, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	//rawdata := []RawDataDoc{}
	var result []interface{}
	// station := Station{}
	// TimeZone := 8
	mongo := getMongo()

	// _ = mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID}).One(&station)
	// if err != nil {

	// 	_di.Log.Info("line 452 getoneEndpoint(),Auth error.")
	// 	_di.Log.Info(err.Error())
	// 	responseJSON := simpleRes{&result}
	// 	json.NewEncoder(w).Encode(responseJSON)

	// 	return
	// }
	// TimeZone = station.TimeZone

	c := mongo.DB(zbtDb).C(RT)
	pipe := c.Pipe(MakeRealTimePipeline(stationID, startT, endT))
	err = pipe.All(&result)
	if err != nil {
		//handle error
		w.WriteHeader(http.StatusBadRequest)
		return
	} else {
		responseJSON := simpleRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
	}

	_afterEndPoint(w, req)

}

func MakeRealTimePipeline(stationid string, start int64, end int64) []bson.M {

	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"timestamp": bson.M{
			"$gte": start,
			"$lt":  end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":       1,
			"inverterid":      1,
			"timestamp":       1,
			"uploadtimestamp": 1,
			"datetime":        1,
			"dcpart":          1,
			"acpart":          1,
			"acpdaily":        1,
			"acplife":         1,
			"freq":            1,
			"invtemp":         1,
			"inverterstatus":  1,
			"pvtemp":          1,
			"irr":             1,
			"_id":             0,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"timestamp": 1, "inverterid": 1}})
	return pipiline
}

func (sa SolarAPI) overviewEndpoint(w http.ResponseWriter, req *http.Request) {

	_beforeEndPoint(w, req)

	authID := req.Header.Get("AuthID")
	if authID == "" || !bson.IsObjectIdHex(authID) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("authID error"))
		return
	}
	Group := req.Header.Get("AuthGroup")

	stationID := []StationID{}

	result := []OverviewRTDoc{}
	mongo := getMongo()

	_ = mongo.DB(zbtDb).C(StationS).Find(bson.M{"group": bson.M{"$in": []string{Group}}}).All(&stationID)
	var staarr []string

	for _, sta := range stationID {
		staarr = append(staarr, sta.StationID)
	}

	_ = mongo.DB(zbtDb).C(RTOverview).Find(bson.M{"stationid": bson.M{"$in": staarr}}).Sort("status").All(&result)

	responseJSON := OvRTRes{&result}
	json.NewEncoder(w).Encode(responseJSON)
	_afterEndPoint(w, req)

}

func (sa SolarAPI) invEndpoint(w http.ResponseWriter, req *http.Request) {

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
	station := Station{}

	result := []InvRTDoc{}
	mongo := getMongo()
	err_S := mongo.DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationID, "group": bson.M{"$in": []string{Group}}}).One(&station)
	if err_S != nil {
		_di.Log.Info("line 561: invEndpoint(),Station_ID info not found.")
		_di.Log.Info(err_S.Error())
		responseJSON := InvRTRes{&result}
		json.NewEncoder(w).Encode(responseJSON)
		_afterEndPoint(w, req)
	} else {

		invrow := len(station.Inverter)
		err := mongo.DB(zbtDb).C(RTInv).Find(bson.M{"stationid": stationID}).Sort("inv_status", "inverterid").All(&result)
		if err != nil {
			_di.Log.Info("line 571: invEndpoint(),realtime inverter data  not found.")

		} else if len(result) != invrow {
			_di.Log.Info("line 574: overviewEndpoint(),realtime data row not equal to inv row.")

		} else {

			responseJSON := InvRTRes{&result}
			json.NewEncoder(w).Encode(responseJSON)
			_afterEndPoint(w, req)
		}

	}
}

func (sa SolarAPI) updateDatetime(w http.ResponseWriter, req *http.Request) {

	_beforeEndPoint(w, req)

	rtdoc := []RTDoc{}
	getMongo().DB(zbtDb).C(RT).Find(bson.M{"uploadtimestamp": bson.M{
		"$lt": 1517565041,
	}}).All(&rtdoc)
	//fmt.Println("rtdoc", rtdoc)
	fmt.Println("aa")
	for _, data := range rtdoc {
		fmt.Println("data", data.StationID, data.InverterID, data.UploadTimestamp)
		err := getMongo().DB(zbtDb).C(RT).Update(bson.M{"_id": data.ID}, bson.M{"$set": bson.M{"datetime": time.Unix(data.Timestamp, 0).In(time.FixedZone("GMT", 8*3600))}})
		if err != nil {
			fmt.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("authID error"))
			return
		}
	}
	w.Write([]byte("okay"))
	_afterEndPoint(w, req)
	return
}

// TODO: Sort function
// type arrRTDoc []InvRTDoc

// func (a arrRTDoc) Len() int      { return len(a) }
// func (a arrRTDoc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
// func (a arrRTDoc) Less(i, j int) bool {
// 	if a[i].Inv_Status < a[j].Inv_Status {
// 		return true
// 	}
// 	if a[i].Inv_Status > a[j].Inv_Status {
// 		return false
// 	}
// 	return a[i].InverterID < a[j].InverterID
// }

func (sa SolarAPI) getOneEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	fmt.Println("getone")
	_afterEndPoint(w, req)
}

func (w *RTDoc) CreateEvent(alerts *[]alert.Alert) (bool, error) {
	timeNow := time.Now()
	StationDoc := &Station{}

	err := getMongo().DB(zbtDb).C(StationS).Find(bson.M{"stationid": w.StationID}).One(&StationDoc)

	if err != nil {
		_di.Log.Err(err.Error())
	}

	for _, al := range *alerts {
		event := EventDoc{
			ID:            bson.NewObjectId(),
			AlertType:     al.AlertType,
			EventTimeUnix: timeNow.Unix(),

			EventTimeFomat: timeNow.In(time.FixedZone("GMT", StationDoc.TimeZone*3600)).Format(TimeFormat),
			EventTitle:     al.Title,
			StationID:      w.StationID,
			InverterID:     w.InverterID,
			Message:        al.Message,
			Status:         StatusSysAlert,
			Regions:        []string{StationDoc.CityID, StationDoc.TownID},
			Permission:     &[]string{"admin"},
		}
		err := getMongo().DB(zbtDb).C(EventC).Insert(&event)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (w *RTDoc) GenObjectId() {
	if bson.ObjectId("") == w.ID {

		w.ID = getRTDocObjectId(w.StationID, w.InverterID, w.Timestamp)
	}
}

func (w *RawDataInit) GenObjectId() {
	if bson.ObjectId("") == w.ID {

		w.ID = getRTDocObjectId(w.StationID, w.InverterID, w.Timestamp)
	}
}

func getRTDocObjectId(StationID string, InverterID int, Timestamp int64) bson.ObjectId {
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
	b[11] = uint8(InverterID)
	return bson.ObjectId(b[:])
}

func (rd *RTDoc) GetById(id string) (*RTDoc, error) {
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(RT).FindId(id).One(rd)
	if err != nil {
		_di.Log.Info(err.Error())
		return nil, err
	}
	return rd, nil
}

func (rd *RTDoc) GetByIdRT(id string) (*RTDoc, error) {
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(RT).FindId(id).One(rd)
	if err != nil {
		_di.Log.Info(err.Error())
		return nil, err
	}
	return rd, nil
}

func (rd *RTDoc) Update(update interface{}) (bool, error) {
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(RT).UpdateId(rd.ID.Hex(), update)
	if err != nil {
		_di.Log.Err(err.Error())
		return false, err
	}
	return true, nil
}

func (rd *RTDoc) UpdateRT(update interface{}) (bool, error) {
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(RT).UpdateId(rd.ID.Hex(), update)
	if err != nil {
		_di.Log.Err(err.Error())
		return false, err
	}
	return true, nil
}

func (rd *RTDoc) UpdateRawdata(update interface{}) (bool, error) {
	mongo := getMongo()
	err := mongo.DB(zbtDb).C(RawData).UpdateId(rd.ID.Hex(), update)
	if err != nil {
		_di.Log.Err(err.Error())
		return false, err
	}
	return true, nil
}

//TODO: 取上一筆資料功能
/*
func (rd RTDoc) GetPrevRec() (*RTDoc, error) {
	nowTime := time.Unix(rd.Timestamp, 0)
	var err error
	for i := 1; i <= 10; i++ {
		preTime := nowTime.Add(-time.Second * time.Duration(i*30))
		preID := getRTDocObjectId(rd.StationID, rd.InverterID, preTime.Unix())
		preSD := RTDoc{}
		_, err = preSD.GetById(preID.Hex())
		if err == nil {
			return &preSD, nil
		}
	}
	return nil, err
}
*/
//query the last time data
func (rd RTDoc) GetPrevRecV2() (*RTDoc, error) {
	//fmt.Println("GetPrevRecV2")
	preSD := RTDoc{}
	mongo := getMongo()
	t_end := rd.Timestamp
	//fmt.Println("t_end", t_end)
	t_start := rd.Timestamp - 1200 //20 min
	//fmt.Println("t_start", t_start)
	err := mongo.DB(zbtDb).C(RT).Find(bson.M{"stationid": rd.StationID, "inverterid": rd.InverterID, "timestamp": bson.M{"$gte": t_start, "$lt": t_end}}).Sort("-timestamp").Limit(1).One(&preSD)

	if err != nil {

		return nil, err
	}
	return &preSD, nil

}

func (sa SolarAPI) createEndpoint(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	pkg := Package{}

	err_decode := json.NewDecoder(req.Body).Decode(&pkg)
	if err_decode != nil {
		fmt.Println(err_decode)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorRes{err_decode.Error()})
		return
	}
	mongo := getMongo()

	var StationID string
	Resfrom := ResDoc{}

	//StationID = pkg.Rows[0].StationID
	args := []tasks.Arg{}
	for _, rd := range pkg.Rows {
		//Rawdata
		//fmt.Println("inter", inter)
		//rd := &RTDoc{}
		// err_conv := rd.FillStruct(inter)
		// if err_conv != nil {
		// 	fmt.Println(err_conv)
		// 	json.NewEncoder(w).Encode(errorRes{err_conv.Error()})
		// 	return
		// }
		//StationID = rd.StationID
		rd.GenObjectId()
		//fmt.Println("rd", rd)
		rd.Datetime = time.Unix(rd.Timestamp, 0).In(time.FixedZone("GMT", 8*3600))
		rtdata := rd.GetRTRec()

		args = append(args, tasks.Arg{Type: "string", Value: rtdata.ID.Hex()})
		//err := getMongo().DB(zbtDb).C(RT).Upsert(bson.M{"_id": rtdata.ID}, rtdata)
		err := getMongo().DB(zbtDb).C(RT).Insert(rtdata)
		if err != nil {

			// 回傳錯誤息
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(errorRes{err.Error()})
			return
		} else {
			//  判斷alert
			_di.Log.Info("ProcessAlert Start")
			err = rtdata.ProcessAlert()
			if err != nil {
				_di.Log.Err(err.Error())
			}
			_di.Log.Info("ProcessAlert End")

		}
	}

	result, err := queue.SendTask(TaskSolarCreated, args)
	if err != nil {
		//_di.Log.Err(err.Error())
	} else {
		_di.Log.Info(fmt.Sprintf("Add Queue. UUID is %s. State is %s", result.TaskUUID, result.State))
	}
	// response

	err = mongo.DB(zbtDb).C(SolarRC).Find(bson.M{"Station_ID": StationID}).One(&Resfrom)

	if err != nil {
		json.NewEncoder(w).Encode(errorRes{"Add success but config of this Station_ID not found"})
		return
	} else if Resfrom.ConfigFlag == "False" {

		json.NewEncoder(w).Encode(ResDoc{ConfigFlag: "False", UploadData: "True"})
		return
	}

	timeNowSecs := time.Now().Unix()
	Resfrom.UploadData = "True"
	Resfrom.DateTime = timeNowSecs
	json.NewEncoder(w).Encode(Resfrom)
	_afterEndPoint(w, req)
	return

}

func (rd *RTDoc) ProcessAlert() error {
	_di.Log.Debug("ProcessAlert")
	objID := GetStationDocObjectId(rd.StationID)

	invAlertConf, err := FindInvAlertConfByID(objID)

	if err != nil {
		_di.Log.Debug("line 821 ProcessAlert(),Inv alert conf of ID not found.")
		return err
	}

	//return alertList []alert.Alert
	alerts := returnnil()
	//TODO: Add TimeZone UTC +8

	time_ := time.Unix(rd.Timestamp, 0).In(time.FixedZone("GMT", 8*3600))
	hour_ := time_.Hour()
	fmt.Println("time", time_, "hour", hour_)

	if rd.AvgIrr != nil {
		if *rd.AvgIrr > 0.1 && hour_ > 8 && hour_ < 17 {
			_di.Log.Debug("Validate")
			alerts = invAlertConf.Validate(rd)
		}
	}
	fmt.Println(alerts)
	//alerts := invAlertConf.Validate(rd)
	if alerts == nil || len(*alerts) < 1 {
		return nil
	}

	rd.HasAlert = true
	_di.Log.Debug("event")
	var notifyAlert []alert.Alert

	for _, alert := range *alerts {

		//&alert ->alert config ,string(objID)->此Station_ID

		if isAlert := countInRedis(&alert, rd.StationID, rd.InverterID); isAlert {

			notifyAlert = append(notifyAlert, alert)
		}
	}

	if len(notifyAlert) == 0 {
		return nil
	}
	_, err = rd.CreateEvent(&notifyAlert)

	return err

}

func countInRedis(al *alert.Alert, StationID string, InverterID int) bool {
	redis := _di.Redis.DB(db.RedisDB_Alert).GetClient()
	//"[%d]:%s", al.AlertType, al.Title
	key := fmt.Sprintf("%s:%d:%s", StationID, InverterID, al.ToKeyStr())
	//Incr 針對key加一 ,Decr 針對key減一
	value, err := redis.Incr(key).Result()
	if err != nil {
		_di.Log.Err(err.Error())

	}
	_di.Log.Debug(fmt.Sprintf("Alert key: %s . Count is %d", key, value))
	//判斷Minute時間裡超過Times的次數 ,時間超過Minute Duration ,key自動被清除
	redis.Expire(key, time.Duration(al.Minute)*time.Minute)
	//次數到達Times 回傳true ,反之回傳false
	return uint8(value) == al.Times
}

func returnnil() *[]alert.Alert {
	return nil

}

func (rd RTDoc) GetRTRec() *RTDoc {

	//TODO: Get Inv Cap

	DCrow := 0

	for _, DCline := range rd.DCPart {

		if DCline.Index <= 8 && DCline.Index > 0 {

			switch DCline.Index {
			case 1:
				rd.DC1DCA = DCline.DCA
				rd.DC1DCV = DCline.DCV
				rd.DC1DCP = DCline.DCP
			case 2:
				rd.DC2DCA = DCline.DCA
				rd.DC2DCV = DCline.DCV
				rd.DC2DCP = DCline.DCP
			case 3:
				rd.DC3DCA = DCline.DCA
				rd.DC3DCV = DCline.DCV
				rd.DC3DCP = DCline.DCP
			case 4:
				rd.DC4DCA = DCline.DCA
				rd.DC4DCV = DCline.DCV
				rd.DC4DCP = DCline.DCP
			case 5:
				rd.DC5DCA = DCline.DCA
				rd.DC5DCV = DCline.DCV
				rd.DC5DCP = DCline.DCP
			case 6:
				rd.DC6DCA = DCline.DCA
				rd.DC6DCV = DCline.DCV
				rd.DC6DCP = DCline.DCP
			case 7:
				rd.DC7DCA = DCline.DCA
				rd.DC7DCV = DCline.DCV
				rd.DC7DCP = DCline.DCP
			case 8:
				rd.DC8DCA = DCline.DCA
				rd.DC8DCV = DCline.DCV
				rd.DC8DCP = DCline.DCP

			}

			//check DCA value is not null
			if DCline.DCA != nil {
				//ACA
				if rd.DCA == nil {
					rd.DCA = new(float64)
					*rd.DCA = 0.0
				}
				*rd.DCA += *DCline.DCA
			}

			//check DCV value is not null
			if DCline.DCV != nil {
				DCrow++
				//DCV
				if rd.DCV == nil {
					rd.DCV = new(float64)
					*rd.DCV = 0.0
				}
				*rd.DCV += *DCline.DCV
			}

			//check DCV value is not null
			if DCline.DCP != nil {
				//DCP
				if rd.DCP == nil {
					rd.DCP = new(float64)
					*rd.DCP = 0.0
				}
				*rd.DCP += *DCline.DCP
			}

			//check DCPLife value is not null
			if DCline.DCPLife != nil {
				//DCPLife
				if rd.DCPLife == nil {
					rd.DCPLife = new(float64)
					*rd.DCPLife = 0.0
				}
				*rd.DCPLife += *DCline.DCPLife
			}
		}
	}
	//fmt.Println("DCrow", DCrow)
	if DCrow > 0 {
		*rd.DCV /= float64(DCrow)
		rd.DCV = round(*rd.DCV, 2)
	}
	//AC part
	ACVrow := 0
	ACFreqrow := 0
	for _, ACline := range rd.ACPart {

		if ACline.Index <= 3 && ACline.Index > 0 {

			switch ACline.Index {
			case 1:
				rd.L1ACA = ACline.ACA
				rd.L1ACV = ACline.ACV
				rd.L1ACP = ACline.ACP
				rd.L1ACFreq = ACline.Freq
			case 2:
				rd.L2ACA = ACline.ACA
				rd.L2ACV = ACline.ACV
				rd.L2ACP = ACline.ACP
				rd.L2ACFreq = ACline.Freq
			case 3:
				rd.L3ACA = ACline.ACA
				rd.L3ACV = ACline.ACV
				rd.L3ACP = ACline.ACP
				rd.L3ACFreq = ACline.Freq
			}

			//check ACA value is not null
			if ACline.ACA != nil {
				//ACA
				if rd.ACA == nil {
					rd.ACA = new(float64)
					*rd.ACA = 0.0
				}
				*rd.ACA += *ACline.ACA
			}

			//check ACV value is not null
			if ACline.ACV != nil {
				ACVrow++
				//ACV
				if rd.ACV == nil {
					rd.ACV = new(float64)
					*rd.ACV = 0.0
				}
				*rd.ACV += *ACline.ACV
			}

			//check ACP value is not null
			if ACline.ACP != nil {
				//ACP
				if rd.ACP == nil {
					rd.ACP = new(float64)
					*rd.ACP = 0.0
				}
				*rd.ACP += *ACline.ACP
			}

			//check Freq value is not null
			if ACline.Freq != nil {
				ACFreqrow++
				//Freq
				if rd.Freq == nil {
					rd.Freq = new(float64)
					*rd.Freq = 0.0
				}
				*rd.Freq += *ACline.Freq
			}
		}

	}
	if ACVrow > 0 {
		*rd.ACV /= float64(ACVrow)
		rd.ACV = round(*rd.ACV, 2)
	}
	if ACFreqrow > 0 {
		*rd.Freq /= float64(ACFreqrow)
		rd.Freq = round(*rd.Freq, 2)
	}
	pvtemprow := 0
	for _, tmp := range rd.PVTemp {

		//check tmp value is not null
		if tmp.Value != nil {
			pvtemprow++
			//AvgPVTemp
			if rd.AvgPVTemp == nil {
				rd.AvgPVTemp = new(float64)
				*rd.AvgPVTemp = 0.0
			}
			*rd.AvgPVTemp += *tmp.Value
		}
	}
	if pvtemprow > 0 {
		*rd.AvgPVTemp /= float64(pvtemprow)
		rd.AvgPVTemp = round(*rd.AvgPVTemp, 2)
	}
	irrrow := 0
	for _, irr := range rd.Irr {
		//check irr value is not null
		if irr.Value != nil {
			irrrow++
			//AvgIrr
			if rd.AvgIrr == nil {
				rd.AvgIrr = new(float64)
				*rd.AvgIrr = 0.0
			}
			*rd.AvgIrr += *irr.Value * 0.001
		}

	}
	if irrrow > 0 {
		*rd.AvgIrr /= float64(irrrow)
		rd.AvgIrr = round(*rd.AvgIrr, 4)

	}
	//rd.InvEff = new(float64)
	//*rd.InvEff = 0.0
	//fmt.Println("rd.DCP", *rd.DCP, rd.DCP)

	if rd.DCP != nil && rd.ACP != nil {
		//InvEff
		if rd.InvEff == nil {
			rd.InvEff = new(float64)
			*rd.InvEff = 0.0
		}
		if *rd.DCP != 0.0 {
			rd.InvEff = round(*rd.ACP / *rd.DCP * 100, 2)
		}

	}
	IrrLimit := 0.1
	ACPLimit := 0.0
	info, err := getStationInRedis(rd.StationID, rd.InverterID)
	fmt.Println("line1017 err", err)
	fmt.Println("line1017 info", info)
	//先define *float 初值
	//ACPRealTime
	// rd.ACPRealTime = new(float64)
	// *rd.ACPRealTime = 0.0
	// //IrrDurTime
	// rd.IrrDurTime = new(float64)
	// *rd.IrrDurTime = 0.0
	// //IrrDurTime
	// rd.ACPDurTime = new(float64)
	// *rd.ACPDurTime = 0.0
	// //PR
	// rd.PR = new(float64)
	// *rd.PR = 0.0
	// //RA
	// rd.RA = new(float64)
	// *rd.RA = 0.0
	//Cap
	rd.Cap = new(float64)
	*rd.Cap = 0.0

	if len(info) == 3 && err == nil {
		rd.Cap = info[0]
		IrrLimit = *info[1]
		ACPLimit = *info[2]
		preRTDoc, _ := rd.GetPrevRecV2()

		if preRTDoc != nil {
			if rd.ACPLife != nil && preRTDoc.ACPLife != nil {
				//ACPRealTime
				if rd.ACPRealTime == nil {
					rd.ACPRealTime = new(float64)
					*rd.ACPRealTime = 0.0
				}
				rd.ACPRealTime = round(*rd.ACPLife-*preRTDoc.ACPLife, 4)
			} else if rd.ACP != nil {
				if rd.ACP != nil {
					//ACPRealTime
					if rd.ACPRealTime == nil {
						rd.ACPRealTime = new(float64)
						*rd.ACPRealTime = 0.0
					}
					*rd.ACPRealTime = *rd.ACP * 0.001
				}
			}
			rd.PreDataID = &preRTDoc.ID

			if preRTDoc.AvgIrr != nil && rd.AvgIrr != nil {
				//IrrDurTime
				if rd.IrrDurTime == nil {
					rd.IrrDurTime = new(float64)
					*rd.IrrDurTime = 0.0
				}
				if rd.IrrRealTime == nil {
					rd.IrrRealTime = new(float64)
					*rd.IrrRealTime = 0.0
				}

				if *preRTDoc.AvgIrr > IrrLimit && *rd.AvgIrr > IrrLimit {
					*rd.IrrDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
				}

				if *preRTDoc.AvgIrr > 0.0 && *rd.AvgIrr > 0.0 {

					*rd.IrrRealTime = ((*rd.AvgIrr + *preRTDoc.AvgIrr) * *rd.IrrDurTime) / 2
				} else {
					*rd.IrrRealTime = 0
				}

			}
			if preRTDoc.ACP != nil && rd.ACP != nil {
				//ACPDurTime
				if rd.ACPDurTime == nil {
					rd.ACPDurTime = new(float64)
					*rd.ACPDurTime = 0.0
				}
				if *preRTDoc.ACP > ACPLimit && *rd.ACP > ACPLimit {
					*rd.ACPDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
				}
			}
			if rd.AvgIrr == nil || rd.Cap == nil || rd.DCP == nil {
				rd.RA = nil
			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
				rd.RA = new(float64)
				*rd.RA = 0.0
			} else {
				//RA
				if rd.RA == nil {
					rd.RA = new(float64)
					*rd.RA = 0.0
				}
				rd.RA = round((*rd.DCP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
			}
			if rd.AvgIrr == nil || rd.Cap == nil || rd.ACP == nil {
				rd.PR = nil
			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
				rd.PR = new(float64)
				*rd.PR = 0.0
			} else {
				//PR
				if rd.PR == nil {
					rd.PR = new(float64)
					*rd.PR = 0.0
				}
				rd.PR = round((*rd.ACP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
			}
			return &rd
		} else {
			if rd.ACP != nil {
				//ACPRealTime
				if rd.ACPRealTime == nil {
					rd.ACPRealTime = new(float64)
					*rd.ACPRealTime = 0.0
				}
				*rd.ACPRealTime = *rd.ACP * 0.001
			}

			rd.PreDataID = nil
			rd.IrrDurTime = nil
			rd.ACPDurTime = nil
			rd.IrrRealTime = nil
			if rd.AvgIrr == nil || rd.Cap == nil || rd.DCP == nil {
				rd.RA = nil
			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
				rd.RA = new(float64)
				*rd.RA = 0.0
			} else {
				//RA
				if rd.RA == nil {
					rd.RA = new(float64)
					*rd.RA = 0.0
				}
				rd.RA = round((*rd.DCP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
			}
			if rd.AvgIrr == nil || rd.Cap == nil || rd.ACP == nil {
				rd.PR = nil
			} else if *rd.AvgIrr == 0.0 || *rd.Cap == 0.0 {
				rd.PR = new(float64)
				*rd.PR = 0.0
			} else {
				//PR
				if rd.PR == nil {
					rd.PR = new(float64)
					*rd.PR = 0.0
				}
				rd.PR = round((*rd.ACP/(1000*(*rd.AvgIrr)*(*rd.Cap)))*100.0, 2)
			}

		}
		return &rd
	} else {

		rd.Cap = nil
		rd.PR = nil
		rd.RA = nil
		preRTDoc, _ := rd.GetPrevRecV2()
		if preRTDoc != nil {
			if rd.ACPLife != nil && preRTDoc.ACPLife != nil {
				//ACPRealTime
				if rd.ACPRealTime == nil {
					rd.ACPRealTime = new(float64)
					*rd.ACPRealTime = 0.0
				}
				rd.ACPRealTime = round(*rd.ACPLife-*preRTDoc.ACPLife, 2)
			}
			rd.PreDataID = &preRTDoc.ID

			if preRTDoc.AvgIrr != nil && rd.AvgIrr != nil {
				//IrrDurTime
				if rd.IrrDurTime == nil {
					rd.IrrDurTime = new(float64)
					*rd.IrrDurTime = 0.0
				}
				if rd.IrrRealTime == nil {
					rd.IrrRealTime = new(float64)
					*rd.IrrRealTime = 0.0
				}

				if *preRTDoc.AvgIrr > IrrLimit && *rd.AvgIrr > IrrLimit {
					*rd.IrrDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
				}

				if *preRTDoc.AvgIrr > 0.0 && *rd.AvgIrr > 0.0 {
					*rd.IrrRealTime = ((*rd.AvgIrr + *preRTDoc.AvgIrr) * *rd.IrrDurTime) / 2
				} else {
					*rd.IrrRealTime = 0
				}
			}
			if preRTDoc.ACP != nil && rd.ACP != nil {
				//ACPDurTime
				if rd.ACPDurTime == nil {
					rd.ACPDurTime = new(float64)
					*rd.ACPDurTime = 0.0
				}

				if *preRTDoc.ACP > ACPLimit && *rd.ACP > ACPLimit {
					*rd.ACPDurTime = float64(rd.Timestamp - preRTDoc.Timestamp)
				}
			}
			return &rd
		} else {
			rd.ACPRealTime = nil
			rd.PreDataID = nil
			rd.IrrDurTime = nil
			rd.ACPDurTime = nil
			rd.IrrRealTime = nil
		}
		return &rd
	}

}

func getStationInRedis(stationid string, invid int) ([]*float64, error) {
	var result []*float64
	key := fmt.Sprintf("%s:%d:list", stationid, invid)

	redisClient := getRedis(RedisDB_Station).GetClient()
	length, err := redisClient.LLen(key).Result()

	if err != nil {
		_di.Log.Err(err.Error())
		//panic(err)
	}
	if length == 3 {

		for i := 0; i < 3; i++ {

			val, err := redisClient.LIndex(key, int64(i)).Result()

			if err != nil {

				return nil, err
			} else {

				f, err := strconv.ParseFloat(val, 64)

				if err != nil {
					return nil, err
				} else {
					result = append(result, &f)
				}
			}
		}

		return result, nil
	} else {
		if length > 3 {
			for j := 0; j < int(length); j++ {
				redisClient.LPop(key)
			}
		}

		station := Station{}
		err := getMongo().DB(zbtDb).C(StationS).Find(bson.M{"stationid": stationid}).One(&station)
		if err != nil {
			return result, err
		} else {
			for _, inv := range station.Inverter {

				if invid == inv.ID {
					result = append(result, &inv.ModuleCap)
					redisClient.RPush(key, inv.ModuleCap)

				}
			}
			result = append(result, &station.Conf.IrrLimit)
			redisClient.RPush(key, station.Conf.IrrLimit)
			result = append(result, &station.Conf.ACPLimit)
			redisClient.RPush(key, station.Conf.ACPLimit)

			return result, nil
		}
	}

}
func (s RTDoc) ToJsonStr() string {
	return toJSONStr(s)
}

func (s RTDoc) ToJsonByte() []byte {
	return toJSONByte(s)
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

func SetField(obj interface{}, name string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		fmt.Println("val.Type()", val.Type())
		fmt.Println("structFieldType", structFieldType)
		return errors.New("Provided value type " + name + " didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}

func (s *RTDoc) FillStruct(m map[string]interface{}) error {
	for k, v := range m {
		err := SetField(s, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// func (doc *RTDoc) RTDocStrtoRTDoc(docstr *RTDocStr) error {

// }

func stringtofloat(str string) (*float64, error) {
	// "var float float64" up here somewhere
	value, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, err
	}
	float := float64(value)
	return &float, nil
}

func (sa SolarAPI) getrate(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	queries := req.URL.Query()
	var stationid string
	var starttime string
	var endtime string
	id, ok := queries["stationid"]
	//fmt.Println(id, ok)
	if ok {
		stationid = id[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	start_, ok := queries["starttime"]
	fmt.Println(start_, ok)
	if ok {
		starttime = start_[0]
	} else {
		w.Write([]byte("fail starttime"))
		return
	}

	end_, ok := queries["endtime"]
	fmt.Println(end_, ok)
	if ok {
		endtime = end_[0]
	} else {
		w.Write([]byte("fail endtime"))
		return
	}

	//fmt.Println(stationid, starttime, endtime)
	start := StrToTime(starttime)
	//fmt.Println("time1", start)

	end := StrToTime(endtime)
	//fmt.Println("time", start, end)

	pipiline := []bson.M{
		bson.M{"$match": bson.M{"stationid": stationid}},
		bson.M{"$match": bson.M{"datetime": bson.M{
			"$gte": start,
			"$lte": end,
		}}},
	}

	pipiline = append(pipiline, bson.M{
		"$group": bson.M{
			"_id": bson.M{
				"stationid": "$stationid",
				"datestr":   "$datestr",
				"datetime":  "$datetime",
			},
			"stationid":     bson.M{"$first": "$stationid"},
			"invnum":        bson.M{"$sum": 1},
			"acpirrtimesum": bson.M{"$sum": bson.M{"$cond": []interface{}{bson.M{"$gte": []interface{}{"$irracptime", 100.0}}, 1, 0}}},
			"acplife":       bson.M{"$sum": "$acplife"},
			"inveff":        bson.M{"$avg": "$inveff"},
			"irr":           bson.M{"$avg": "$irr"},
			"acpdaily":      bson.M{"$sum": "$acpdaily"},
			"cap":           bson.M{"$sum": "$cap"},
		},
	})
	pipiline = append(pipiline, bson.M{
		"$project": bson.M{
			"stationid":     1,
			"datestr":       "$_id.datestr",
			"datetime":      "$_id.datetime",
			"invnum":        1,
			"acpirrtimesum": 1,
			"acplife":       1,
			"inveff":        1,
			"irr":           1,
			"acpdaily":      1,
			"cap":           1,
		},
	})
	pipiline = append(pipiline, bson.M{"$sort": bson.M{"_id": 1}})
	c := getMongo().DB(zbtDb).C("Day")
	pipe := c.Pipe(pipiline)
	//fmt.Println(MakeHourPipeline())
	resp := []RateDoc{}
	result := []RateDoc{}
	err := pipe.All(&resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorRes{err.Error()})
		return
	} else {
		for _, row := range resp {

			//fmt.Println(row.Acpirrtimesum, row.Invnum)
			if row.Invnum > 0.0 {
				row.PPR = (row.Acpirrtimesum / row.Invnum) * 100
			} else {
				row.PPR = 0.0
			}
			if row.Cap > 0 {
				row.Kkp = row.Acpdaily / row.Cap
			} else {
				row.Kkp = 0
			}
			if row.Cap > 0 && row.Irr > 0 {
				row.PR = (row.Acpdaily / (row.Cap * row.Irr)) * 100
			} else {
				row.PR = 0
			}

			result = append(result, row)

		}
		//fmt.Println(result)
		//responseJSON := simpleRes{&resp}
		json.NewEncoder(w).Encode(result)
	}

	_afterEndPoint(w, req)
	return

}

func (sa SolarAPI) dailydata(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	queries := req.URL.Query()
	var stationid string
	var starttime string
	var endtime string
	id, ok := queries["stationid"]
	//fmt.Println(id, ok)
	if ok {
		stationid = id[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	start_, ok := queries["starttime"]
	fmt.Println(start_, ok)
	if ok {
		starttime = start_[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	end_, ok := queries["endtime"]
	fmt.Println(end_, ok)
	if ok {
		endtime = end_[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	//fmt.Println(stationid, starttime, endtime)
	start := StrToTime(starttime)
	//fmt.Println("time1", start)

	end := StrToTime(endtime)
	//fmt.Println("time", start, end)
	dailydata := []daily{}
	c := getMongo().DB(zbtDb).C("Day")
	err := c.Find(bson.M{"stationid": stationid, "datetime": bson.M{
		"$gte": start,
		"$lte": end,
	}}).Sort("datetime", "inverterid").All(&dailydata)
	if err != nil {
		_di.Log.Err(err.Error())
		w.Write([]byte("error")) //return nil
		return
	}
	json.NewEncoder(w).Encode(dailydata)
	_afterEndPoint(w, req)
	return

}

func (sa SolarAPI) createEndpointv2(w http.ResponseWriter, req *http.Request) {
	// rawdata := RawData_{}
	// _ = json.NewDecoder(req.Body).Decode(&rawdata)
	// err := rawdata.Save()
	// if err != nil {
	// 	w.WriteHeader(http.StatusInternalServerError)
	// 	_di.Log.Err(err.Error())
	// 	return
	// }
	// w.Write([]byte("success"))

	pkg := PackageRD{}

	err_decode := json.NewDecoder(req.Body).Decode(&pkg)
	if err_decode != nil {
		fmt.Println(err_decode)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorRes{err_decode.Error()})
		return
	}
	//var strarr []string
	//args := []tasks.Arg{}
	for _, rd := range pkg.Rows {
		rd.GenObjectId()
		rd.Datetime = time.Unix(rd.Timestamp, 0).In(time.FixedZone("GMT", 8*3600))
		err := rd.Save()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_di.Log.Err(err.Error())
			return
		}
		//args = append(args, tasks.Arg{Type: "string", Value: rd.ID.Hex()})
		//strarr = append(strarr, rd.ID.Hex())

	}

	// result, err := queue.SendTask(TaskSolarCreated, args)
	// if err != nil {
	// 	_di.Log.Err(err.Error())
	// } else {
	// 	_di.Log.Info(fmt.Sprintf("Add Queue. UUID is %s. State is %s", result.TaskUUID, result.State))
	// }

	w.Write([]byte("success"))

}

func (sa SolarAPI) testelastic(w http.ResponseWriter, req *http.Request) {
	_beforeEndPoint(w, req)
	queries := req.URL.Query()
	var stationid string
	var starttime string
	var endtime string
	id, ok := queries["stationid"]
	//fmt.Println(id, ok)
	if ok {
		stationid = id[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	start_, ok := queries["starttime"]
	fmt.Println(start_, ok)
	if ok {
		starttime = start_[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	end_, ok := queries["endtime"]
	fmt.Println(end_, ok)
	if ok {
		endtime = end_[0]
	} else {
		w.Write([]byte("fail stationid"))
		return
	}

	//fmt.Println(stationid, starttime, endtime)
	start := StrToTime(starttime)
	//fmt.Println("time1", start)

	end := StrToTime(endtime)
	//fmt.Println("time", start, end)
	realdata := []RTDoc{}
	c := getMongo().DB(zbtDb).C("RealTime")
	err := c.Find(bson.M{"stationid": stationid, "datetime": bson.M{
		"$gte": start,
		"$lte": end,
	}}).Sort("datetime", "inverterid").All(&realdata)
	if err != nil {
		_di.Log.Err(err.Error())
		w.Write([]byte("error")) //return nil
		return
	}

	if err != nil {
		panic(err)
	}

	session, err := mgo.Dial("localhost:27017")
	defer session.Close()
	for _, data := range realdata {
		d := session.DB(zbtDb).C("RealTime")
		d.Insert(data)
	}
	_afterEndPoint(w, req)
	return

}

// func (sa SolarAPI) getrate(w http.ResponseWriter, req *http.Request) {
// 	_beforeEndPoint(w, req)
// 	m := make(map[string]string)
// 	req.ParseForm()
// 	if len(req.Form) > 0 {
// 		for k, v := range req.Form {
// 			m[k] = v[0]
// 		}
// 	}
// 	if m["stationid"] == "" {
// 		w.Write([]byte("fail stationid"))
// 		return
// 	}
// 	if m["starttime"] == "" {
// 		w.Write([]byte("fail starttime"))
// 		return
// 	}
// 	if m["endtime"] == "" {
// 		w.Write([]byte("fail endtime"))
// 		return
// 	}
// 	stationid := m["stationid"]
// 	starttime := m["starttime"]
// 	endtime := m["endtime"]
// 	fmt.Println(stationid, starttime, endtime)
// 	/*vars := mux.Vars(req)
// 	stationid := vars["stationid"]
// 	starttime := vars["starttime"]
// 	endtime := vars["endtime"]*/
// 	// w.Write([]byte(stationid))
// 	// w.Write([]byte(starttime))
// 	// w.Write([]byte(endtime))
// 	//w.Write([]byte(stationid))
// 	invnum := invnumberstruct{}
// 	count := 0.0
// 	count2 := 0.0
// 	mongo := getMongo()
// 	err := mongo.DB(zbtDb).C("Day").Find(bson.M{"stationid": stationid}).Sort("-inverterid").Limit(1).One(&invnum)
// 	if err != nil {
// 		_di.Log.Err(err.Error())
// 		w.Write([]byte("error")) //return nil
// 	}
// 	fmt.Println(invnum.Inverterid)

// 	//num, err := strconv.Atoi(invnum.Inverterid)
// 	for i := 1; i <= invnum.Inverterid; i++ {

// 		ratearr := []invnumberstruct{}
// 		mongo := getMongo()
// 		err := mongo.DB(zbtDb).C("Day").Find(bson.M{"stationid": stationid, "inverterid": i, "datetime": bson.M{
// 			"$gte": StrToTime(starttime),
// 			"$lt":  StrToTime(endtime),
// 		}}).All(&ratearr)
// 		fmt.Println(StrToTime(starttime))
// 		fmt.Println(StrToTime(endtime))
// 		if err != nil {
// 			fmt.Println("err", err)
// 			return
// 			//return nil
// 		}
// 		fmt.Println("ratearr", ratearr)
// 		for _, number := range ratearr {
// 			fmt.Println("number", number)
// 			if number.Acpdurtime != nil && number.Irrdurtime != nil {

// 				if *number.Acpdurtime <= 0 || *number.Irrdurtime <= 0 {
// 					count2 = count2 + 1
// 					continue
// 				} else {
// 					if *number.Acpdurtime / *number.Irrdurtime > 0 {
// 						count = count + 1
// 					}

// 				}
// 			} else {
// 				count2 = count2 + 1
// 			}

// 		}

// 	}

// 	rate := count / (count + count2) * 100.0

// 	result := ratestruct{}
// 	result.Success = "Success"
// 	result.Rate = rate
// 	result.StationID = m["stationid"]
// 	fmt.Println("reault", result)
// 	json.NewEncoder(w).Encode(result)

// 	_afterEndPoint(w, req)
// 	return
// 	/*mongo := getMongo()
// 	err := mongo.DB(zbtDb).C("Day").Find(bson.M{}).One(&invnum)
// 	if err != nil {
// 		w.Write([]byte("ff"))
// 		return
// 	}*/

// 	// w.WriteHeader(http.StatusBadRequest)
// 	// w.Write([]byte(string(invnum.Num)))
// 	// fmt.Println(invnum.Inverterid)

// }

func StrToTime(st string) time.Time {
	t, _ := time.ParseInLocation(f_datetime, st, time.Local)
	return t.In(time.FixedZone("GMT", 8*3600))
}

const (
	f_datetime = "2006-01-02T15:04:05"
)

type invnumberstruct struct {
	Inverterid int      `json:"inverterid,omitempty"`
	Irrdurtime *float64 `json:"irrdurtime,omitempty"`
	Acpdurtime *float64 `json:"acpdurtime,omitempty"`
}

type Rate_in struct {
	Rows *ratestruct `json:"result,omitempty"`
}

type ratestruct struct {
	Success   string  `json:"success,omitempty"`
	StationID string  `json:"stationid,omitempty"`
	Rate      float64 `json:"rate"`
}

type daily struct {
	StationID string `json:"stationid,omitempty"`
	Datestr   string `json:"datetime,omitempty"`
	// Datetime   time.Time `json:"datetime,omitempty"`
	Inverterid int      `json:"inverterid,omitempty"`
	Irrdurtime *float64 `json:"irrdurtime,omitempty"`
	Acpdurtime *float64 `json:"acpdurtime,omitempty"`
	Irracptime *float64 `json:"irracptime,omitempty"`
}

// func (s *RTDoc) RTDocStrtoRTDoc(m map[string]interface{}) error {
// 	for k, v := range m {
// 		err := ConvField(s, k, v)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func ConvField(obj interface{}, name string, value interface{}) error {
// 	structValue := reflect.ValueOf(obj).Elem()
// 	structFieldValue := structValue.FieldByName(name)

// 	if !structFieldValue.IsValid() {
// 		return fmt.Errorf("No such field: %s in obj", name)
// 	}

// 	if !structFieldValue.CanSet() {
// 		return fmt.Errorf("Cannot set %s field value", name)
// 	}

// 	structFieldType := structFieldValue.Type()
// 	val := reflect.ValueOf(value)
// 	switch structFieldType := structFieldValue.(type) {
// 	case float64:
// 		switch val {

// 		}
// 	}
// 	val := reflect.ValueOf(value)

// 	if structFieldType != val.Type() {
// 		fmt.Println("val.Type()", val.Type())
// 		fmt.Println("structFieldType", structFieldType)
// 		return errors.New("Provided value type " + name + " didn't match obj field type")
// 	}

// 	structFieldValue.Set(val)
// 	return nil
// }
