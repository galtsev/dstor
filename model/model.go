//go:generate easyjson -snake_case $GOFILE
//go:generate msgp
package model

//easyjson:json
type Sample struct {
	Tag string `msg:"tag"`
	// TODO convert to array
	Values [10]float64 `msg:"values"`
	// timestamp in nanoseconds
	TS int64 `msg:"ts"`
}

//easyjson:json
type Samples []Sample
