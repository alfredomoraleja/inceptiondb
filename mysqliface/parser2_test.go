package mysqliface

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func TestParserTIDB(t *testing.T) {
	astNode, err := parse("SELECT a, b FROM t")
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", *astNode)
}
