package apicollectionv1

import (
	"context"
	"net/http"

	"github.com/fulldump/inceptiondb/service"
)

const ContextServicerKey = "ed0fa170-5593-11ed-9d60-9bdc940af29d"
const ContextForwarderKey = "7f6b62e2-8d4f-4ea1-9b8d-6e959c1d4e6c"

type Forwarder interface {
	Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) (bool, error)
}

func SetServicer(ctx context.Context, s service.Servicer) context.Context {
	return context.WithValue(ctx, ContextServicerKey, s)
}

func GetServicer(ctx context.Context) service.Servicer {
	return ctx.Value(ContextServicerKey).(service.Servicer) // TODO: can raise panic :D
}

func SetForwarder(ctx context.Context, f Forwarder) context.Context {
	if f == nil {
		return ctx
	}
	return context.WithValue(ctx, ContextForwarderKey, f)
}

func GetForwarder(ctx context.Context) Forwarder {
	forwarder, _ := ctx.Value(ContextForwarderKey).(Forwarder)
	return forwarder
}
