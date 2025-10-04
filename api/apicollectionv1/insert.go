package apicollectionv1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/fulldump/box"

	"github.com/fulldump/inceptiondb/service"
	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

func insert(ctx context.Context, w http.ResponseWriter, r *http.Request) error {

	wc := http.NewResponseController(w)
	wcerr := wc.EnableFullDuplex()
	if wcerr != nil {
		return wcerr
	}

	s := GetServicer(ctx)
	collectionName := box.GetUrlParameter(ctx, "collectionName")
	collection, err := s.GetCollection(collectionName)
	if err == service.ErrorCollectionNotFound {
		collection, err = s.CreateCollection(collectionName)
		if err != nil {
			return err // todo: handle/wrap this properly
		}
		err = collection.SetDefaults(newCollectionDefaults())
		if err != nil {
			return err // todo: handle/wrap this properly
		}
	}
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	decoder := jsontext.NewDecoder(r.Body)

	for i := 0; true; i++ {
		item := map[string]any{}
		err := jsonv2.UnmarshalDecode(decoder, &item)
		if errors.Is(err, io.EOF) {
			if i == 0 {
				w.WriteHeader(http.StatusNoContent)
			}
			return nil
		}
		if err != nil {
			// TODO: handle error properly
			fmt.Println("ERROR:", err.Error())
			if i == 0 {
				w.WriteHeader(http.StatusBadRequest)
			}
			return err
		}
		row, err := collection.Insert(item)
		if err != nil {
			// TODO: handle error properly
			if i == 0 {
				w.WriteHeader(http.StatusConflict)
			}
			return err
		}

		if i == 0 {
			w.WriteHeader(http.StatusCreated)
		}
		w.Write(row.Payload)
		w.Write([]byte("\n"))
	}

	return nil
}
