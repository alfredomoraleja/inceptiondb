package apicollectionv1

import (
	"context"
	stdjson "encoding/json"
	"net/http"

	"github.com/fulldump/box"
	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"

	"github.com/fulldump/inceptiondb/service"
)

type setDefaultsInput map[string]any

func setDefaults(ctx context.Context, w http.ResponseWriter, r *http.Request) error {

	s := GetServicer(ctx)
	collectionName := box.GetUrlParameter(ctx, "collectionName")
	col, err := s.GetCollection(collectionName)
	if err == service.ErrorCollectionNotFound {
		col, err = s.CreateCollection(collectionName)
		if err != nil {
			return err // todo: handle/wrap this properly
		}
		err = col.SetDefaults(newCollectionDefaults())
		if err != nil {
			return err // todo: handle/wrap this properly
		}
	}
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	defaults := col.Defaults

	decoder := jsontext.NewDecoder(r.Body)
	err = jsonv2.UnmarshalDecode(decoder, &defaults)
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	for k, v := range defaults {
		if v == nil {
			delete(defaults, k)
		}
	}

	if len(defaults) == 0 {
		defaults = nil
	}

	err = col.SetDefaults(defaults)
	if err != nil {
		return err
	}

	err = stdjson.NewEncoder(w).Encode(col.Defaults)
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	return nil
}
