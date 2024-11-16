package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

const (
	unknownColumn = "?column?"
)

var (
	trueMemoryCell  = MemoryCell([]byte{1})
	falseMemoryCell = MemoryCell(nil)

	ErrInvalidCell     = fmt.Errorf("invalid cell")
	ErrInvalidOperands = fmt.Errorf("invalid operands, mismatched types?")
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

func (c MemoryCell) AsBool() bool {
	return len(c) != 0
}

func (c MemoryCell) equals(b MemoryCell) bool {
	if c == nil || b == nil {
		return c == nil && b == nil
	}
	return bytes.Equal(c, b)
}

type table struct {
	columns     []string
	columnTypes []ColumnType
	rows        [][]MemoryCell
}

func (t *table) evaluateInfixOperationCell(rowIndex int, stmt sqlparser.BinaryExpr) (MemoryCell, string, ColumnType, error) {
	l, ln, lt, err := t.evaluateCell(rowIndex, stmt.Left)
	if err != nil {
		return nil, "", 0, err
	}
	r, rn, rt, err := t.evaluateCell(rowIndex, stmt.Right)
	if err != nil {
		return nil, "", 0, err
	}

	// TODO: Not sure if this is the best option.
	colName := ln
	if ln == unknownColumn {
		colName = rn
	}

	switch stmt.Operator {
	case sqlparser.PlusStr:
		if lt != rt {
			return nil, "", 0, ErrInvalidOperands
		}

		switch lt {
		case TextType:
			return MemoryCell([]byte(string(l) + string(r))), colName, TextType, nil
		case IntType:
			return MemoryCell(intToBytes(l.AsInt() + r.AsInt())), colName, IntType, nil
		}
		return nil, "", 0, ErrInvalidOperands
	}

	return nil, "", 0, ErrInvalidCell
}

func (t *table) evaluateInfixComparisonCell(rowIndex int, stmt sqlparser.ComparisonExpr) (MemoryCell, string, ColumnType, error) {
	l, _, lt, err := t.evaluateCell(rowIndex, stmt.Left)
	if err != nil {
		return nil, "", 0, err
	}
	r, _, rt, err := t.evaluateCell(rowIndex, stmt.Right)
	if err != nil {
		return nil, "", 0, err
	}

	switch stmt.Operator {
	case sqlparser.EqualStr:
		if lt != rt {
			return falseMemoryCell, unknownColumn, BoolType, ErrInvalidOperands
		}
		if l.equals(r) {
			return trueMemoryCell, unknownColumn, BoolType, nil
		}
		return falseMemoryCell, unknownColumn, BoolType, nil
	case sqlparser.NotEqualStr:
		if lt != rt {
			return falseMemoryCell, unknownColumn, BoolType, ErrInvalidOperands
		}
		if !l.equals(r) {
			return trueMemoryCell, unknownColumn, BoolType, nil
		}
		return falseMemoryCell, unknownColumn, BoolType, nil
	}

	return nil, "", 0, ErrInvalidCell
}

func (t *table) evaluateAndCell(rowIndex int, stmt sqlparser.AndExpr) (MemoryCell, string, ColumnType, error) {
	l, _, lt, err := t.evaluateCell(rowIndex, stmt.Left)
	if err != nil {
		return nil, "", 0, err
	}
	r, _, rt, err := t.evaluateCell(rowIndex, stmt.Right)
	if err != nil {
		return nil, "", 0, err
	}

	if lt != BoolType || rt != BoolType {
		return nil, "", 0, ErrInvalidCell
	}
	if l.equals(r) && l.equals(trueMemoryCell) {
		return trueMemoryCell, unknownColumn, BoolType, nil
	}
	return falseMemoryCell, unknownColumn, BoolType, nil
}

func (t *table) evaluateOrCell(rowIndex int, stmt sqlparser.OrExpr) (MemoryCell, string, ColumnType, error) {
	l, _, lt, err := t.evaluateCell(rowIndex, stmt.Left)
	if err != nil {
		return nil, "", 0, err
	}
	r, _, rt, err := t.evaluateCell(rowIndex, stmt.Right)
	if err != nil {
		return nil, "", 0, err
	}

	if lt != BoolType || rt != BoolType {
		return nil, "", 0, ErrInvalidCell
	}
	if l.equals(trueMemoryCell) || r.equals(trueMemoryCell) {
		return trueMemoryCell, unknownColumn, BoolType, nil
	}
	return falseMemoryCell, unknownColumn, BoolType, nil
}

func (t *table) evaluateCell(rowIndex int, stmt sqlparser.Expr) (MemoryCell, string, ColumnType, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.SQLVal:
		switch stmt.Type {
		case sqlparser.IntVal:
			i, err := strconv.ParseInt(string(stmt.Val), 10, 64)
			if err != nil {
				return nil, "", 0, fmt.Errorf("unable to parse int: %w", err)
			}
			return MemoryCell(intToBytes(int32(i))), unknownColumn, IntType, nil
		case sqlparser.StrVal:
			return MemoryCell(stmt.Val), unknownColumn, TextType, nil
		case sqlparser.BitVal:
			return MemoryCell(stmt.Val), unknownColumn, BoolType, nil
		}
	case *sqlparser.ColName:
		for i, colName := range t.columns {
			if colName == sqlparser.String(stmt) {
				return t.rows[rowIndex][i], colName, t.columnTypes[i], nil
			}
		}
	case *sqlparser.ComparisonExpr:
		return t.evaluateInfixComparisonCell(rowIndex, *stmt)
	case *sqlparser.BinaryExpr:
		return t.evaluateInfixOperationCell(rowIndex, *stmt)
	case *sqlparser.AndExpr:
		return t.evaluateAndCell(rowIndex, *stmt)
	case *sqlparser.OrExpr:
		return t.evaluateOrCell(rowIndex, *stmt)
	}

	return nil, "", 0, fmt.Errorf("unsupported expression: %s", stmt)
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
	// TODO: Clean this up, and have better errors.
	if rows, ok := stmt.Rows.(sqlparser.Values); ok {
		for rowIndex, row := range rows {
			var cells []MemoryCell
			for i, val := range row {
				v, _, ct, err := table.evaluateCell(rowIndex, val)
				if err != nil {
					return fmt.Errorf("unable to evaluate cell: %w", err)
				}
				if ct != table.columnTypes[i] {
					return fmt.Errorf("mismatched types: %s != %s", ct, table.columnTypes[i])
				}
				cells = append(cells, v)
			}

			if len(cells) != len(table.columns) {
				return fmt.Errorf("mismatched number of fields: %d != %d", len(cells), len(table.columns))
			}
			table.rows = append(table.rows, cells)
		}
	}
	return nil
}

func (b *MemoryBackend) Select(stmt *sqlparser.Select) (*Results, error) {
	// Let's assume we only have one table at this point.
	tableName := sqlparser.String(stmt.From)
	table, ok := b.tables[tableName]
	if !ok {
		return &Results{}, fmt.Errorf("table %s does not exist", tableName)
	}

	r := Results{}

	for rowIndex := range table.rows {
		result := make([]Cell, 0)
		isFirstRow := len(r.Rows) == 0

		if stmt.Where != nil {
			v, _, _, err := table.evaluateCell(rowIndex, stmt.Where.Expr)
			if err != nil {
				return nil, err
			}
			if !v.AsBool() {
				continue
			}
		}

		for i := range stmt.SelectExprs {
			inExpr := stmt.SelectExprs[i].(*sqlparser.AliasedExpr).Expr
			v, colName, colType, err := table.evaluateCell(rowIndex, inExpr)
			if err != nil {
				return nil, err
			}

			if isFirstRow {
				r.Columns = append(r.Columns, struct {
					Type ColumnType
					Name string
				}{
					Type: colType,
					Name: colName,
				})
			}
			result = append(result, v)
		}
		r.Rows = append(r.Rows, result)
	}

	return &r, nil
}
