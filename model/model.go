//go:generate easyjson $GOFILE
//go:generate msgp
package model

//easyjson:json
type Sample struct {
	Tag string
	// TODO convert to array
	Values [10]float64
	TS     int64 //timestamp in nanoseconds
}

//easyjson:json
type Samples []Sample
