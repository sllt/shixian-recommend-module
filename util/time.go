package util

import (
	"time"
)

const FORMAT_DATE string = "2006-01-02"
const FORMAT_DATE_TIME string = "2006-01-02 15:04:05"
const FORMAT_TIME string = "20060102"

func UnixMSec(off int) int64 {
	return (time.Now().Unix() + int64(off)) * 1000
}

func UnixSec(off int) int64 {
	return (time.Now().Unix() + int64(off))
}

func GetDateNow() string {
	return time.Now().Format(FORMAT_DATE)
}

func GetDate(t time.Time) string {
	return t.Format(FORMAT_DATE)
}

func GetDateTime(t time.Time) string {
	return t.Format(FORMAT_DATE_TIME)
}

func GetDayTime(t time.Time) string {
	now := t.Format(FORMAT_DATE_TIME)
	year := now[0:4]
	month := now[5:7]
	day := now[8:10]

	ret := year + month + day
	return ret
}

func GetDayTimeNow() string {
	now := time.Now().Format(FORMAT_DATE_TIME)
	year := now[0:4]
	month := now[5:7]
	day := now[8:10]

	ret := year + month + day
	return ret
}

func GetHourTime(t time.Time) string {
	now := t.Format(FORMAT_DATE_TIME)
	year := now[0:4]
	month := now[5:7]
	day := now[8:10]
	hour := now[11:13]

	ret := year + month + day + hour
	return ret
}

func GetHourTimeOnly(t time.Time) string {
	now := t.Format(FORMAT_DATE_TIME)
	hour := now[11:13]

	return hour
}

func GetHourTimeNow() string {
	now := time.Now().Format(FORMAT_DATE_TIME)
	year := now[0:4]
	month := now[5:7]
	day := now[8:10]
	hour := now[11:13]

	ret := year + month + day + hour
	return ret
}

func GetMinTime(t time.Time) string {
	now := t.Format(FORMAT_DATE_TIME)
	year := now[0:4]
	month := now[5:7]
	day := now[8:10]
	hour := now[11:13]
	min := now[14:16]

	ret := year + month + day + hour + min
	return ret
}

func GetMinTimeOnly(t time.Time) string {
	now := t.Format(FORMAT_DATE_TIME)
	min := now[14:16]

	return min
}

func GetMinTimeNow() string {
	now := time.Now().Format(FORMAT_DATE_TIME)
	year := now[0:4]
	month := now[5:7]
	day := now[8:10]
	hour := now[11:13]
	min := now[14:16]

	ret := year + month + day + hour + min
	return ret
}

func ParseDate(value string) time.Time {
	t, _ := time.Parse(FORMAT_DATE, value)
	return t
}

func ParseDateTime(value string) time.Time {
	t, _ := time.Parse(FORMAT_DATE_TIME, value)
	return t
}
