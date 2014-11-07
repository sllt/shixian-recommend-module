package model

import (
	"fmt"
)

var GlobalConf = NewConf()

type Conf struct {
	AppName       string
	LogName       string
	MemcachedHost string
}

func (this *Conf) String() string {
	if this == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Conf(%+v)", *this)
}

func NewConf() *Conf {
	return &Conf{}
}
