package apicollectionv1

import (
	"context"
	json2 "encoding/json/v2"
	"net/http"

	"github.com/fulldump/box"

	"github.com/fulldump/inceptiondb/collection"
)

func find(ctx context.Context, w http.ResponseWriter, r *http.Request) error {

	buf := getRequestBuffer()
	defer putRequestBuffer(buf)

	if _, err := buf.ReadFrom(r.Body); err != nil {
		return err
	}
	requestBody := buf.Bytes()

	input := struct {
		Index *string
	}{}
	err := json2.Unmarshal(requestBody, &input)
	if err != nil {
		return err
	}

	s := GetServicer(ctx)
	collectionName := box.GetUrlParameter(ctx, "collectionName")
	col, err := s.GetCollection(collectionName)
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	return traverse(requestBody, col, func(row *collection.Row) bool {
		w.Write(row.Payload)
		w.Write([]byte("\n"))
		return true
	})
}
