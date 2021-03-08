package main

import (
	"context"

	"github.com/inconshreveable/log15"
)

type ctxKey string

var (
	lgrContextKey = ctxKey("lgr")
)

func LgrFromContext(ctx context.Context) log15.Logger {
	lgrI := ctx.Value(lgrContextKey)
	if lgrI == nil {
		return log15.New()
	}
	return lgrI.(log15.Logger).New()
}

func WithLgrContext(ctx context.Context, lgr log15.Logger) context.Context {
	return context.WithValue(ctx, lgrContextKey, lgr)
}
