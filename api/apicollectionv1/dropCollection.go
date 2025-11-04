package apicollectionv1

import (
	"context"
	"net/http"

	"github.com/fulldump/box"
)

func dropCollection(ctx context.Context, w http.ResponseWriter) error {

	if forwarded, err := forwardWrite(ctx, w, nil); forwarded {
		return err
	}

	s := GetServicer(ctx)

	collectionName := box.GetUrlParameter(ctx, "collectionName")

	return s.DeleteCollection(collectionName) // TODO: wrap error?
}
