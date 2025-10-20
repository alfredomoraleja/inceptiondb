package apicollectionv1

import (
	"github.com/fulldump/inceptiondb/utils"
)

type CollectionResponse struct {
	Name     string           `json:"name"`
	Total    int              `json:"total"`
	Indexes  int              `json:"indexes"`
	Defaults utils.JSONObject `json:"defaults"`
}
