package api

import (
	"encoding/json"
	"fmt"
	"github.com/galtsev/dstor"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/phttp"
	"github.com/galtsev/dstor/serializer"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	batch     []model.Sample
	writeURL  string
	reportURL string
	szr       serializer.Serializer
}

func NewClient(cfg conf.ClientConfig) *Client {
	client := Client{
		writeURL:  fmt.Sprintf("http://%s/batch", cfg.Host),
		reportURL: fmt.Sprintf("http://%s/api", cfg.Host),
		szr:       serializer.NewSerializer("easyjson"),
	}
	return &client
}

func timeToArg(t time.Time) string {
	return strconv.Itoa(int(t.UnixNano()))
}

func (c *Client) Report(tag string, start, stop time.Time) []dstor.ReportLine {
	args := url.Values{}
	args.Set("tag", tag)
	args.Set("start", timeToArg(start))
	args.Set("end", timeToArg(stop))
	url, _ := url.Parse(c.reportURL)
	url.RawQuery = args.Encode()
	// reqData := phttp.ReportRequest{
	// 	Tag:   tag,
	// 	Start: start.Format(DATE_FORMAT_LONG),
	// 	End:   stop.Format(DATE_FORMAT_LONG),
	// }
	// body, err := json.Marshal(reqData)
	// Check(err)
	// resp, err := http.Post(c.reportURL, "application/json", bytes.NewBuffer(body))
	resp, err := http.Get(url.String())
	Check(err)
	if resp.StatusCode >= 400 {
		panic(fmt.Errorf("Bad response code %d: %s", resp.StatusCode, resp.Status))
	}
	var respData phttp.ReportResponse
	respBody, err := ioutil.ReadAll(resp.Body)
	Check(err)
	Check(json.Unmarshal(respBody, &respData))
	return respData.Samples
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
