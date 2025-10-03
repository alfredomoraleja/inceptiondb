package apicollectionv1

import (
	"fmt"

	"github.com/SierraSoftworks/connor"
	jsonv2 "github.com/go-json-experiment/json"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/utils"
)

func traverse(requestBody []byte, col *collection.Collection, f func(row *collection.Row) bool) error {

	options := &struct {
		Index  *string                `json:"index"`
		Filter map[string]interface{} `json:"filter"`
		Skip   int64                  `json:"skip"`
		Limit  int64                  `json:"limit"`
	}{
		Index:  nil,
		Filter: nil,
		Skip:   0,
		Limit:  1,
	}
	err := jsonv2.Unmarshal(requestBody, &options)
	if err != nil {
		return err
	}

	hasFilter := options.Filter != nil && len(options.Filter) > 0

	skip := options.Skip
	limit := options.Limit
	iterator := func(r *collection.Row) bool {
		if limit == 0 {
			return false
		}

		if hasFilter {
			rowData := map[string]interface{}{}
			jsonv2.Unmarshal(r.Payload, &rowData) // todo: handle error here?

			match, err := connor.Match(options.Filter, rowData)
			if err != nil {
				// todo: handle error?
				// return fmt.Errorf("match: %w", err)
				return false
			}
			if !match {
				return true
			}
		}

		if skip > 0 {
			skip--
			return true
		}
		limit--
		return f(r)
	}

	// Fullscan
	if options.Index == nil {
		traverseFullscan(col, iterator)
		return nil
	}

	index, exists := col.Indexes[*options.Index]
	if !exists {
		return fmt.Errorf("index '%s' not found, available indexes %v", *options.Index, utils.GetKeys(col.Indexes))
	}

	index.Traverse(requestBody, iterator)

	return nil
}

func traverseFullscan(col *collection.Collection, f func(row *collection.Row) bool) error {

	col.TraverseRange(0, 0, func(row *collection.Row) bool {
		return f(row)
	})

	return nil
}
