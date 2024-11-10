package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/xwb1989/sqlparser"
	"golang.org/x/exp/slices"
)

type MemoryCell []byte

func (c MemoryCell) AsInt() int32 {
	var i int32
	err := binary.Read(bytes.NewBuffer(c), binary.BigEndian, &i)
	if err != nil {
		panic(err)
	}
	return i
}

func (c MemoryCell) AsText() string {
	return string(c)
}

type table struct {
	columns     []string
	columnTypes []ColumnType
	rows        [][]MemoryCell
}

type MemoryBackend struct {
	tables map[string]*table
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		tables: make(map[string]*table),
	}
}

func (b *MemoryBackend) CreateTable(stmt *sqlparser.DDL) error {
	if stmt.Action != sqlparser.CreateStr {
		return fmt.Errorf("only CREATE statement is currently supported")
	}
	tableName := sqlparser.String(stmt.NewName)
	if _, ok := b.tables[tableName]; ok {
		return fmt.Errorf("table already exists")
	}

	t := table{
		columns:     make([]string, 0),
		columnTypes: make([]ColumnType, 0),
		rows:        make([][]MemoryCell, 0),
	}
	for _, col := range stmt.TableSpec.Columns {
		columnType, err := parseColumnType(col.Type)
		if err != nil {
			return fmt.Errorf("unable to parse column type: %w", err)
		}
		t.columns = append(t.columns, col.Name.CompliantName())
		t.columnTypes = append(t.columnTypes, columnType)
	}
	b.tables[tableName] = &t

	return nil
}

func (b *MemoryBackend) Insert(stmt *sqlparser.Insert) error {
	table, ok := b.tables[sqlparser.String(stmt.Table)]
	if !ok {
		return fmt.Errorf("table %s does not exist", stmt.Table.Name.CompliantName())
	}
	if rows, ok := stmt.Rows.(sqlparser.Values); ok {
		for _, row := range rows {
			var cells []MemoryCell
			for i, val := range row {
				switch table.columnTypes[i] {
				case IntType:
					v, err := strconv.ParseInt(sqlparser.String(val), 10, 64)
					if err != nil {
						return fmt.Errorf("unable to parse int type: %w", err)
					}
					cells = append(cells, MemoryCell(intToBytes(int32(v))))
				case TextType:
					cells = append(cells, MemoryCell(sqlparser.String(val)))
				}
			}
			table.rows = append(table.rows, cells)
		}
	}
	return nil
}

func (b *MemoryBackend) Select(stmt *sqlparser.Select) (Results, error) {
	// Let's assume we only have one table at this point.
	tableName := sqlparser.String(stmt.From)
	table, ok := b.tables[tableName]
	if !ok {
		return Results{}, fmt.Errorf("table %s does not exist", tableName)
	}

	r := Results{}
	idxs := make([]int, 0)
	for _, col := range stmt.SelectExprs {
		// Maybe binary search here isn't optimal, oh well.
		idx, ok := slices.BinarySearch(table.columns, sqlparser.String(col))
		if !ok {
			return Results{}, fmt.Errorf("field '%s' does not exist", col)
		}
		r.Columns = append(r.Columns, struct {
			Type ColumnType
			Name string
		}{
			Type: table.columnTypes[idx],
			Name: table.columns[idx],
		})
		idxs = append(idxs, idx)
	}

	for _, row := range table.rows {
		rrow := make([]Cell, 0)
		for _, idx := range idxs {
			rrow = append(rrow, row[idx])
		}
		r.Rows = append(r.Rows, rrow)
	}
	return r, nil
}
