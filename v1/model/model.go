//go:generate easyjson $GOFILE
package model

//easyjson:json
type Sample struct {
	Tag    string
	Values []float64
	TS     int64 //timestamp in nanoseconds
}
