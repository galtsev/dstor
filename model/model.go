//go:generate easyjson $GOFILE
//go:generate msgp
package model

//easyjson:json
type Sample struct {
	Tag    string
	Values []float64
	TS     int64 //timestamp in nanoseconds
}

//easyjson:json
type Samples []Sample
