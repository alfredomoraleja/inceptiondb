package apicollectionv1

import (
	"context"
	"io"
	"net/http"

	"github.com/fulldump/box"

	"github.com/fulldump/inceptiondb/collection"
	jsonv2 "github.com/go-json-experiment/json"
)

func remove(ctx context.Context, w http.ResponseWriter, r *http.Request) error {

	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	input := struct {
		Index string `json:"index"`
	}{
		Index: "",
	}
	err = jsonv2.Unmarshal(requestBody, &input)
	if err != nil {
		return err
	}

	s := GetServicer(ctx)
	collectionName := box.GetUrlParameter(ctx, "collectionName")
	col, err := s.GetCollection(collectionName)
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	var result error

	traverse(requestBody, col, func(row *collection.Row) bool {
		err := col.Remove(row)
		if err != nil {
			result = err
			return false
		}

		w.Write(row.Payload)
		w.Write([]byte("\n"))
		return true
	})

	return result
}
