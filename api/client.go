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
	"sync"
	"time"
)

type Client struct {
	cfg       conf.ClientConfig
	writeURL  string
	reportURL string
	szr       serializer.Serializer
	ch        chan model.Sample
	wg        sync.WaitGroup
}

func NewClient(cfg conf.ClientConfig) *Client {
	client := Client{
		cfg:       cfg,
		writeURL:  fmt.Sprintf("http://%s/batch", cfg.Host),
		reportURL: fmt.Sprintf("http://%s/api", cfg.Host),
		szr:       serializer.NewSerializer("easyjson"),
		ch:        make(chan model.Sample, 1000),
	}
	for i := 0; i < cfg.Concurrency; i++ {
		client.wg.Add(1)
		go client.sendLoop()
	}
	return &client
}

func (c *Client) sendBatch(samples []model.Sample, client *fasthttp.Client) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("POST")
	req.SetRequestURI(c.writeURL)
	req.SetBody(c.szr.Marshal(model.Samples(samples)))
	Check(client.Do(req, resp))
	if resp.StatusCode() >= 300 {
		panic(fmt.Errorf("Bad response: %d", resp.StatusCode()))
	}

}

func (c *Client) sendLoop() {
	client := &fasthttp.Client{}
	var samples []model.Sample
	for sample := range c.ch {
		samples = append(samples, sample)
		more := true
		for more {
			select {
			case s := <-c.ch:
				samples = append(samples, s)
			default:
				more = false
			}
			if len(samples) >= 200 {
				more = false
			}
		}
		c.sendBatch(samples, client)
		samples = samples[:0]
	}
	c.wg.Done()
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

func (c *Client) AddSample(sample *model.Sample) {
	c.ch <- *sample
}

func (c *Client) Close() {
	close(c.ch)
	c.wg.Wait()
}
