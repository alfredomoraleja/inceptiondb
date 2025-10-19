package apireplicationv1

import (
	"github.com/fulldump/box"
)

func Build(v1 *box.R) *box.R {

	resource := v1.Resource("/replication").
		WithActions(
			box.Get(stream),
		)

	return resource
}
