package command

import (
	"bytes"
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/phttp"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

func QueryReporter(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	startStr := fs.String("start", "", "Start of reporting period, YYYY-MM-DD HH:MM")
	endStr := fs.String("end", "", "End of reporting period, YYYY-MM-DD HH:MM")
	tag := fs.String("tag", "", "tag to report")
	cfg := pimco.LoadConfigEx(fs, args...)
	fmt.Println(cfg)
	fmt.Printf("Report for period from %s to %s\n", *startStr, *endStr)
	reqData := phttp.ReportRequest{
		Tag:   *tag,
		Start: *startStr,
		End:   *endStr,
	}
	body, err := json.Marshal(reqData)
	Check(err)
	url := fmt.Sprintf("http://%s/report", cfg.ReportingServer.Addr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	Check(err)
	var respData []pimco.ReportLine
	respBody, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(respBody))
	Check(err)
	Check(json.Unmarshal(respBody, &respData))
	for _, line := range respData {
		fmt.Println(time.Unix(0, line.TS).UTC(), line.Values)
	}

}
