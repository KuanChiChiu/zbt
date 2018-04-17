package search

import (
	"fmt"
	"time"

	"dforcepro.com/resource"
	"gopkg.in/mgo.v2/bson"
)

var (
	wsDi *resource.Di
)

type SearchResult map[string]interface{}

type SearchMeta struct {
	Update int64 `json:"update,omitempty"`
}

type Search interface {
	Put()
	Get(id string)
}

func GetSearchMetaChange() bson.M {
	return bson.M{"$set": bson.M{"_search.update": time.Now().Unix()}}
}

func Init(c *resource.Di) {

	wsDi = c
	_, err := wsDi.Elastic.CreateIndex(SolarFormIndex, getMappings(false))
	if err != nil {
		wsDi.Log.Info(fmt.Sprintf(`create index "%s" fail: %s`, SolarFormIndex, err.Error()))
	} else {
		wsDi.Log.Info(fmt.Sprintf(`create index "%s" success.`, SolarFormIndex))
	}
}
