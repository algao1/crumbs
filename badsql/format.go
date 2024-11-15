package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type ColumnType int

const (
	TextType ColumnType = iota
	IntType
	BoolType
)

func (c ColumnType) String() string {
	switch c {
	case TextType:
		return "text"
	case IntType:
		return "int"
	case BoolType:
		return "bool"
	}
	return "unknown"
}

type Cell interface {
	AsText() string
	AsInt() int32
	AsBool() bool
}

type Results struct {
	Columns []struct {
		Type ColumnType
		Name string
	}
	Rows [][]Cell
}

func intToBytes(v int32) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, v)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func parseColumnType(t sqlparser.ColumnType) (ColumnType, error) {
	switch t.SQLType() {
	case sqltypes.Int32:
		return IntType, nil
	case sqltypes.Text:
		return TextType, nil
	case sqltypes.Bit:
		return BoolType, nil
	default:
		return TextType, fmt.Errorf("unsupported type: %s", t.SQLType())
	}
}
