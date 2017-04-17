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
	bench := fs.Int("bench", 0, "Run N times, report accumulated time")
	tag := fs.String("tag", "", "tag to report")
	cfg := pimco.LoadConfigEx(fs, args...)
	fmt.Println(cfg)
	fmt.Printf("Report for period from %s to %s\n", *startStr, *endStr)
	if *bench == 0 {
		getReport(*startStr, *endStr, *tag, cfg, true)
	} else {
		for i := 0; i < *bench; i++ {
			getReport(*startStr, *endStr, *tag, cfg, false)
		}

	}
}

func getReport(start, end, tag string, cfg pimco.Config, show bool) {
	reqData := phttp.ReportRequest{
		Tag:   tag,
		Start: start,
		End:   end,
	}
	body, err := json.Marshal(reqData)
	Check(err)
	url := fmt.Sprintf("http://%s/report", cfg.ReportingServer.Addr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	Check(err)
	if resp.StatusCode >= 300 {
		panic(fmt.Errorf("Bad response code %d: %s", resp.StatusCode, resp.Status))
	}
	if show {
		var respData []pimco.ReportLine
		respBody, err := ioutil.ReadAll(resp.Body)
		fmt.Println(string(respBody))
		Check(err)
		Check(json.Unmarshal(respBody, &respData))
		for _, line := range respData {
			fmt.Println(time.Unix(0, line.TS).UTC(), line.Values)
		}
	}
}
