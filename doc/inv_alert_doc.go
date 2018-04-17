package doc

import (
	"errors"

	"dforcepro.com/alert"
	"gopkg.in/mgo.v2/bson"
)

const (
	InvAlertC = "InverterAlert"
)

type InverterAlert struct {
	ID bson.ObjectId `json:"ID,omitempty" bson:"_id"` // Station_ID + InverterID
	//InvID       bson.ObjectId       `json:"InverterID,omitempty"`            // InverterID
	FieldAlerts *[]alert.FieldAlert `json:"Field_Alerts, omitempty"`
}

func NewInverterAlert(id bson.ObjectId) (*InverterAlert, error) {
	conf := alert.ConfTpl.GetConf("SolarDoc")
	if conf == nil {
		return nil, errors.New("not exist: SolarDoc")
	}
	return &InverterAlert{ID: id, FieldAlerts: conf}, nil
}

func FindInvAlertConfByID(id bson.ObjectId) (*InverterAlert, error) {
	inverterAlert := InverterAlert{}
	err := getMongo().DB(zbtDb).C(InvAlertC).Find(bson.M{"_id": id}).One(&inverterAlert)
	if err != nil {
		return nil, err
	}
	return &inverterAlert, nil
}

func (ia *InverterAlert) Save() error {
	return getMongo().DB(zbtDb).C(InvAlertC).Insert(ia)
}

func (ia *InverterAlert) Validate(inter interface{}) *[]alert.Alert {
	var myalert *alert.Alert
	var alertList []alert.Alert
	for _, fAlert := range *ia.FieldAlerts {
		// return &Alert{AlertType: alertType, Message: message, Minute: ac.Minute, Times: ac.Times}

		myalert = fAlert.Validate(inter)
		if myalert != nil {
			alertList = append(alertList, *myalert)
		}
	}
	if len(alertList) == 0 {
		return nil
	}
	return &alertList
}
