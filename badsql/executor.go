package badsql

import (
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/xwb1989/sqlparser"
)

type Executor struct {
	db *MemoryBackend
}

func NewExecutor() *Executor {
	return &Executor{
		db: NewMemoryBackend(),
	}
}

func (e *Executor) HandleStatement(s string) error {
	stmt, err := sqlparser.Parse(s)
	if err != nil {
		return err
	}

	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		err := e.db.CreateTable(stmt)
		if err != nil {
			return err
		}
	case *sqlparser.Insert:
		err := e.db.Insert(stmt)
		if err != nil {
			return err
		}
	case *sqlparser.Select:
		var res *Results
		res, err := e.db.Select(stmt)
		if err != nil {
			return err
		}

		// Very messy formatting below, will change later.
		data := make([][]string, len(res.Rows))
		for i, row := range res.Rows {
			data[i] = make([]string, len(row))
			for j, cell := range row {
				switch res.Columns[j].Type {
				case IntType:
					data[i][j] = fmt.Sprintf("%d", cell.AsInt())
				case TextType:
					data[i][j] = cell.AsText()
				}
			}
		}

		table := tablewriter.NewWriter(os.Stdout)
		headers := make([]string, len(res.Columns))
		for i, col := range res.Columns {
			headers[i] = col.Name
		}
		table.SetHeader(headers)
		table.AppendBulk(data)
		table.Render()
	default:
		return fmt.Errorf("unimplemented statement")
	}
	return nil
}
