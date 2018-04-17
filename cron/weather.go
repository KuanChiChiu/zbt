package cron

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"dforcepro.com/cron"
	"gopkg.in/mgo.v2/bson"
	"zbt.dforcepro.com/doc"
)

type Weather bool

func (mycron Weather) Enable() bool {
	return bool(mycron)
}

func (mycron Weather) GetJobs() []cron.JobSpec {
	return []cron.JobSpec{
		cron.JobSpec{
			Spec: "0 0 * * * *",
			Job:  mycron.getweather,
		},
	}
}

func (mycron Weather) getweather() {
	mark := make(map[string]string)
	fmt.Println("hello")
	mark["A"] = "F-D0047-063"
	mark["B"] = "F-D0047-075"
	mark["C"] = "F-D0047-051"
	mark["D"] = "F-D0047-079"
	mark["E"] = "F-D0047-067"
	mark["F"] = "F-D0047-071"
	mark["G"] = "F-D0047-003"
	mark["H"] = "F-D0047-007"
	mark["I"] = "F-D0047-057"
	mark["J"] = "F-D0047-009"
	mark["K"] = "F-D0047-015"
	mark["M"] = "F-D0047-023"
	mark["N"] = "F-D0047-017"
	mark["O"] = "F-D0047-055"
	mark["P"] = "F-D0047-027"
	mark["Q"] = "F-D0047-029"
	mark["T"] = "F-D0047-033"
	mark["U"] = "F-D0047-043"
	mark["V"] = "F-D0047-037"
	mark["W"] = "F-D0047-087"
	mark["X"] = "F-D0047-047"
	mark["Z"] = "F-D0047-083"

	fmt.Println("getweather")
	cityarray := []citys{}

	mongo := doc.GetMongo()
	c := mongo.DB(zbtDb).C("City")
	c.Find(bson.M{}).All(&cityarray)
	/*cityarray := []citys{}

	c := session.DB(zbtDb).C("City")

	c.Find(bson.M{}).All(&cityarray)
	fmt.Println(len(cityarray))*/
	for _, manycity := range cityarray {
		fmt.Println("start")
		id := manycity.Cityid
		townsarray := []towns{}
		c := mongo.DB(zbtDb).C("Town")
		fmt.Println(manycity.Cityid)
		err := c.Find(bson.M{"cityid": bson.M{"$in": []string{manycity.Cityid}}}).All(&townsarray)
		fmt.Println(err)
		fmt.Println(len(townsarray))
		for _, manytown := range townsarray {
			fmt.Println("start")
			response, err := http.Get("http://opendata.cwb.gov.tw/api/v1/rest/datastore/" + mark[id] + "?locationName=" + manytown.Town + "&elementName=PoP,T,Wx,RH,WeatherDescription&sort=time&Authorization=CWB-2FA1D452-8CE2-4EDC-BCDD-B550B36061E1")
			fmt.Println("end")
			if err != nil {
				fmt.Printf("%s", err)
				os.Exit(1)
			} else {

				defer response.Body.Close()

				a := Rawdata{}
				//json.Unmarshal(contents, &a)
				contents, err := ioutil.ReadAll(response.Body)
				if err != nil {
					fmt.Printf("%s", err)
					os.Exit(1)
				}
				fmt.Printf("%s\n", contents)
				err = json.Unmarshal(contents, &a)

				finaldata := realdata{}
				finaldata.City = id
				finaldata.Town = manytown.Town
				finaldata.Townid = manytown.Townid

				str := a.Rawdata.Locations[0].Location[0].WeatherElement[0].Time[0].Startime
				str = strings.Replace(str, " ", "T", -1) + ".371Z"
				t, _ := time.Parse(time.RFC3339, str)
				finaldata.Startime = t.Unix()

				strt := a.Rawdata.Locations[0].Location[0].WeatherElement[0].Time[0].EndTime
				strt = strings.Replace(strt, " ", "T", -1) + ".371Z"
				t2, _ := time.Parse(time.RFC3339, strt)
				finaldata.EndTime = t2.Unix()
				fmt.Println(a.Rawdata.Locations[0].Location[0].WeatherElement[0].Time[0].ElementValue)
				for _, weather := range a.Rawdata.Locations[0].Location[0].WeatherElement {

					fmt.Println(weather.Time[0].ElementValue)

					finaldata.ElementName = weather.ElementName
					finaldata.ElementValue = weather.Time[0].ElementValue
					//finaldata.WeatherElementreal = append(finaldata.WeatherElementreal, struc)
					c := mongo.DB(zbtDb).C("Weather")
					//c.Insert(finaldata)
					c.Upsert(bson.M{"city": finaldata.City, "town": finaldata.Town, "townid": finaldata.Townid, "elementname": finaldata.ElementName, "startime": finaldata.Startime, "endtime": finaldata.EndTime}, finaldata)
				}

				//c.Update(bson.M{"town": finaldata.Town}, bson.M{"$set": finaldata})
				fmt.Println("finish")
			}

		}

	}

}

type citys struct {
	City   string `json:"city,omitempty"`
	Cityid string `json:"cityid,omitempty"`
}

type towns struct {
	Town   string `json:"town,omitempty"`
	Cityid string `json:"cityid,omitempty"`
	Townid string `json:"townid,omitempty"`
}

type Rawdata struct {
	Rawdata Record `json:"records,omitempty"`
}

type Record struct {
	ContentDescription string      `json:"contentDescription,omitempty"`
	Locations          []Locations `json:"locations,omitempty"`
}

type Locations struct {
	DatasetDescription string     `json:"datasetDescription,omitempty"`
	Location           []Location `json:"location,omitempty"`
}

type Location struct {
	WeatherElement []WeatherElement `json:"weatherElement,omitempty"`
}

type WeatherElement struct {
	ElementName string `json:"elementName,omitempty"`
	Time        []Time `json:"time,omitempty"`
}

type Time struct {
	Startime     string `json:"startTime,omitempty"`
	EndTime      string `json:"endTime,omitempty"`
	ElementValue string `json:"elementValue,omitempty"`
}

type realdata struct {
	City         string `json:"city,omitempty"`
	Town         string `json:"town,omitempty"`
	Townid       string `json:"townid,omitempty"`
	ElementName  string `json:"elementName,omitempty"`
	Startime     int64  `json:"startTime,omitempty"`
	EndTime      int64  `json:"endTime,omitempty"`
	ElementValue string `json:"elementValue,omitempty"`
}
