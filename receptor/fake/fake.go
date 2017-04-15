package fake

import (
	"context"
	. "dan/pimco/base"
	"dan/pimco/model"
	"github.com/mailru/easyjson"

	"github.com/valyala/fasthttp"
)

func saveLoop(ctx context.Context, ch chan model.Sample) {
	for _ = range ch {
	}
}

func Run(args []string) {
	ch := make(chan model.Sample, 1000)
	ctx := context.Background()
	go saveLoop(ctx, ch)
	err := fasthttp.ListenAndServe("localhost:9876", makeHandler(ch))
	Check(err)
}
