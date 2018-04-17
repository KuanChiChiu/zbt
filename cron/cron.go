package cron

import (
	"dforcepro.com/resource"
)

var _di *resource.Di

func SetDI(c *resource.Di) {
	_di = c
}
