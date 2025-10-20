package apicollectionv1

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/fulldump/box"

	"github.com/fulldump/inceptiondb/service"
	"github.com/fulldump/inceptiondb/utils"
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

	incoming := utils.JSONObject{}

	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		return err // todo: handle/wrap this properly
	}

	cleaned := utils.JSONObject{}
	for _, field := range incoming {
		if field.Value == nil {
			continue
		}
		cleaned.Set(field.Key, field.Value)
	}

	var defaultsMap map[string]any
	if len(cleaned) > 0 {
		defaultsMap = cleaned.ToMap()
	}

	err = col.SetDefaults(defaultsMap)
	if err != nil {
		return err
	}

	err = json.NewEncoder(w).Encode(col.Defaults)
	if err != nil {
		return err // todo: handle/wrap this properly
	}

	return nil
}
