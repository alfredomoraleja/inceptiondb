package mysqliface

import (
	"testing"

	parser "github.com/bytebase/mysql-parser"
)

func TestParse(t *testing.T) {
	// This is just an example on how to use the library github.com/bytebase/mysql-parser
	_ = parser.Keyword{
		Keyword:  "trolololo",
		Reserved: true,
	}
}
