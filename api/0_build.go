package api

import (
	"context"
	"net/http"

	"github.com/fulldump/box"
	"github.com/fulldump/box/boxopenapi"

	"github.com/fulldump/inceptiondb/api/apicollectionv1"
	"github.com/fulldump/inceptiondb/api/apireplicationv1"
	"github.com/fulldump/inceptiondb/database"
	"github.com/fulldump/inceptiondb/replication"
	"github.com/fulldump/inceptiondb/service"
	"github.com/fulldump/inceptiondb/statics"
)

func Build(s service.Servicer, staticsDir, version string, db *database.Database, manager *replication.Manager, forwarder apicollectionv1.Forwarder) *box.B { // TODO: remove datadir

	b := box.NewBox()

	v1 := b.Resource("/v1")
	v1.WithInterceptors(box.SetResponseHeader("Content-Type", "application/json"))

	collectionResource := apicollectionv1.BuildV1Collection(v1, s)
	interceptors := []box.I{injectServicer(s)}
	if forwarder != nil {
		interceptors = append(interceptors, injectForwarder(forwarder))
	}
	collectionResource.WithInterceptors(interceptors...)

	apireplicationv1.Build(v1).
		WithInterceptors(
			injectReplication(db, manager),
		)

	b.Resource("/v1/*").
		WithActions(box.AnyMethod(func(w http.ResponseWriter) interface{} {
			w.WriteHeader(http.StatusNotImplemented)
			return PrettyError{
				Message:     "not implemented",
				Description: "this endpoint does not exist, please check the documentation",
			}
		}))

	b.Resource("/release").
		WithActions(box.Get(func() string {
			return version
		}))

	spec := boxopenapi.Spec(b)
	spec.Info.Title = "InceptionDB"
	spec.Info.Description = "A durable in-memory database to store JSON documents."
	spec.Info.Contact = &boxopenapi.Contact{
		Url: "https://github.com/fulldump/inceptiondb/issues/new",
	}
	b.Handle("GET", "/openapi.json", func(r *http.Request) any {

		spec.Servers = []boxopenapi.Server{
			{
				Url: "https://" + r.Host,
			},
			{
				Url: "http://" + r.Host,
			},
		}

		return spec
	})

	// Mount statics
	b.Resource("/*").
		WithActions(
			box.Get(statics.ServeStatics(staticsDir)).WithName("serveStatics"),
		)

	return b
}

func injectServicer(s service.Servicer) box.I {
	return func(next box.H) box.H {
		return func(ctx context.Context) {
			next(apicollectionv1.SetServicer(ctx, s))
		}
	}
}

func injectForwarder(f apicollectionv1.Forwarder) box.I {
	return func(next box.H) box.H {
		return func(ctx context.Context) {
			next(apicollectionv1.SetForwarder(ctx, f))
		}
	}
}

func injectReplication(db *database.Database, manager *replication.Manager) box.I {
	return func(next box.H) box.H {
		return func(ctx context.Context) {
			ctx = apireplicationv1.SetDatabase(ctx, db)
			ctx = apireplicationv1.SetManager(ctx, manager)
			next(ctx)
		}
	}
}
