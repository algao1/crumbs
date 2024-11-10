package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/xwb1989/sqlparser"
)

const (
	PromptStr = ">> "
)

func main() {
	db := NewMemoryBackend()
	executor := Executor{db: db}
	scanner := bufio.NewScanner(os.Stdin)

	if len(os.Args) > 1 {
		file, err := os.Open(os.Args[1])
		if err != nil {
			panic(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fmt.Println("executing line:", scanner.Text())
			err := executor.HandleStatement(scanner.Text())
			if err != nil {
				fmt.Println("error:", err)
			}
		}
		return
	}

	fmt.Print(PromptStr)
	for scanner.Scan() {
		err := executor.HandleStatement(scanner.Text())
		if err != nil {
			fmt.Println("error:", err)
		}
		fmt.Print(PromptStr)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error:", err)
	}
}

type Executor struct {
	db *MemoryBackend
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
		var res Results
		res, err := e.db.Select(stmt)
		if err != nil {
			return err
		}

		// Very messy formatting below, will change later.
		data := make([][]string, len(res.Rows))
		for i, row := range res.Rows {
			data[i] = make([]string, len(row))
			for j, cell := range row {
				if res.Columns[j].Type == IntType {
					data[i][j] = fmt.Sprintf("%d", cell.AsInt())
				} else if res.Columns[j].Type == TextType {
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
