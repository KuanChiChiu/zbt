package task

import (
	"zbt.dforcepro.com/doc"
	"zbt.dforcepro.com/search"
)

type GoToSearchForSolar bool

func (gts GoToSearchForSolar) Enable() bool {
	return bool(gts)
}

func (gts GoToSearchForSolar) Process(sd *doc.RTDoc) error {

	solarSearch := search.SolarSearch{RTDoc: sd}

	if success, err := solarSearch.Put(); !success {
		return err
	}

	if success, err := solarSearch.UpdateRT(search.GetSearchMetaChange()); !success {
		return err
	}

	return nil
}
