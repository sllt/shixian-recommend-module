package model

import (
	"fmt"
)

type UserRecommend struct {
	Id          int64   `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Score       float64 `json:"score"`
}

func (this *UserRecommend) String() string {
	if this == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[UserRecommend](%+v)", *this)
}

type ProjectRecommend struct {
	Id    int64   `json:"id"`
	Title string  `json:"title"`
	Score float64 `json:"score"`
}

func (this *ProjectRecommend) String() string {
	if this == nil {
		return "<nil>"
	}
	return fmt.Sprintf("[ProjectRecommend](%+v)", *this)
}
