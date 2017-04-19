package api

import (
	"bytes"
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"dan/pimco/phttp"
	"dan/pimco/serializer"
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"net/http"
	"time"
)

type Client struct {
	batch     []model.Sample
	writeURL  string
	reportURL string
	szr       serializer.Serializer
}

func NewClient(cfg pimco.ClientConfig) *Client {
	client := Client{
		writeURL:  fmt.Sprintf("http://%s/write", cfg.Host),
		reportURL: fmt.Sprintf("http://%s/report", cfg.Host),
		szr:       serializer.NewSerializer("easyjson"),
	}
	return &client
}

func (c *Client) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	reqData := phttp.ReportRequest{
		Tag:   tag,
		Start: start.Format(DATE_FORMAT_LONG),
		End:   stop.Format(DATE_FORMAT_LONG),
	}
	body, err := json.Marshal(reqData)
	Check(err)
	resp, err := http.Post(c.reportURL, "application/json", bytes.NewBuffer(body))
	Check(err)
	if resp.StatusCode >= 300 {
		panic(fmt.Errorf("Bad response code %d: %s", resp.StatusCode, resp.Status))
	}
	var respData []pimco.ReportLine
	respBody, err := ioutil.ReadAll(resp.Body)
	Check(err)
	Check(json.Unmarshal(respBody, &respData))
	return respData
}

func (c *Client) Add(sample *model.Sample) {
	c.batch = append(c.batch, *sample)
}

func (c *Client) Flush() {
	if len(c.batch) == 0 {
		return
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("POST")
	req.SetRequestURI(c.writeURL)
	req.SetBody(c.szr.Marshal(model.Samples(c.batch)))
	Check(fasthttp.Do(req, resp))
	if resp.StatusCode() >= 300 {
		panic(fmt.Errorf("Bad response: %d", resp.StatusCode()))
	}
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	c.batch = c.batch[:0]
}

func (c *Client) Close() {
	c.Flush()
}
