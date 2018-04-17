package task

import (
	"dforcepro.com/resource"
)

var (
	_di *resource.Di
)

func SetDi(di *resource.Di) {
	_di = di
}
