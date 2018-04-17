package report

import (
	"dforcepro.com/resource"
	"dforcepro.com/resource/db"
)

var _di *resource.Di

func SetDI(c *resource.Di) {
	_di = c
}

func getMongo() db.Mongo {
	return _di.Mongodb
}
