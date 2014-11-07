package model

import (
	"fmt"
)

type User struct {
	Id           int64  `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	LastSignInAt int64  `json:"last_sign_in_at"`
}

func (this *User) String() string {
	if this == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[User](%+v)", *this)
}
