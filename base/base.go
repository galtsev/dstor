package base

const (
	DATE_FORMAT = "2006-01-02 15:04"
)

var (
	FIELD_NAMES = []string{"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"}
	EMPTY_TAGS  = map[string]string{}
)

func Check(err error) {
	if err != nil {
		panic(err)
	}
}
