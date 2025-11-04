package apicollectionv1

import (
	"context"
	"fmt"
	"net/http"

	"github.com/fulldump/box"
)

func forwardWrite(ctx context.Context, w http.ResponseWriter, r *http.Request) (bool, error) {
	forwarder := GetForwarder(ctx)
	if forwarder == nil {
		return false, nil
	}

	if r == nil {
		r = box.GetRequest(ctx)
		if r == nil {
			return false, fmt.Errorf("request not available in context")
		}
	}

	return forwarder.Forward(ctx, w, r)
}
