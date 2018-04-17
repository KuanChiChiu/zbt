package cron

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"
	doc "zbt.dforcepro.com/doc"
)

const (
	FtpIp     = "localhost"
	FtpPort   = "9789"
	TmpFolder = "./RawReport/"
)

//check data from db
//	query data by stationID
//  sort dada by inverter ID, timestamp respectively and insert them into the csv

//Write them to csv

func FormReportCSV() error {
	var idList []string
	rawData := []doc.ReportDoc{}
	fmt.Println("Yo")
	//Get now time
	startPoint := time.Now().Unix() - 86400 - 28800
	if startPoint%86400 > 43200 {
		startPoint += (86400 - startPoint%86400)
	} else if startPoint%86400 > 0 && startPoint%86400 < 43200 {
		startPoint -= startPoint % 86400
	}
	//Query all data within that time
	mongoDb := doc.GetMongo()
	queryRes := mongoDb.DB("zbt").C(doc.RT).Find(bson.M{})
	err := queryRes.Distinct("stationid", &idList)
	var inverterData []string
	fmt.Println("idList len : ", len(idList))
	for _, stationID := range idList {
		//Sort and get inverter raw data
		err = mongoDb.DB("zbt").C(doc.RT).Find(bson.M{"stationid": bson.M{"$eq": stationID}}).Sort("inverterid", "timestamp").All(&rawData)
		if err != nil {
			return err
		}
		//Open CSV
		nameBuf := fmt.Sprintln(stationID, "_", time.Unix(rawData[len(rawData)-1].Timestamp, 8).Format("200601021504"))
		fileName := nameBuf[:len(nameBuf)-1]
		fileName = strings.Replace(fileName, " ", "", -1)
		file, err := os.Create(TmpFolder + fileName)
		//Is it opened ?
		if err != nil {
			fmt.Println("Fail to open file", err)
			return err
		}
		//Close file
		defer file.Close()
		//Add the header line to the csv
		w := csv.NewWriter(file)
		// Write any buffered data to the underlying writer (standard output).
		defer w.Flush()
		if err = w.Write([]string{
			"Station_ID", "Inverter_NO", "Data_Time", "DC_Voltage(V)",
			"DC_Current(A)", "DC_Power(kW)", "AC_Voltage(V)",
			"AC_Current(A)", "Frequency(Hz)", "AC_Power(kW)",
			"Today_Energy(kWh)", "Life_Energy(kWh)"}); err != nil {
			log.Fatalln("error writing record to csv:", err)
		}
		//bson.M{"timestamp": bson.M{"$gt": startPoint, "$lt": (startPoint + 86400)}}
		for i := 0; i < len(rawData); i++ {
			fmt.Println(rawData[i])
			inverterData = append(inverterData, rawData[i].StationID)
			//fmt.Println(rawData[i].StationID)
			inverterData = append(inverterData, "Inverter "+strconv.FormatInt(int64(rawData[i].InverterID), 10))
			//fmt.Println(rawData[i].Timestamp)
			inverterData = append(inverterData, time.Unix(rawData[i].Timestamp, 8).Format("2006/01/02 15:04"))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].DCV, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].DCA, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].DCP, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].ACV, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].ACA, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].Freq, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].ACP, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].ACPDaily, 'f', 4, 64))
			inverterData = append(inverterData, strconv.FormatFloat(*rawData[i].ACPLife, 'f', 4, 64))
			if err = w.Write(inverterData); err != nil {
				return err
			}
			inverterData = inverterData[:0]
		}
	}
	return nil
}
