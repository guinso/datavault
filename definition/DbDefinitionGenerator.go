package definition

import (
	"github.com/guinso/rdbmstool"
	"github.com/guinso/stringtool"
)

const (
	//LOAD_DATE is data vault standard table column name
	LOAD_DATE = "load_date"
	//END_DATE is data vault standard table column name
	END_DATE = "end_date"
	//RECORD_SOURCE is data vault standard table column name
	RECORD_SOURCE = "record_source"
)

func createHashKeyColumn(name string) rdbmstool.ColumnDefinition {
	return rdbmstool.ColumnDefinition{
		Name:     stringtool.ToSnakeCase(name) + "_hash_key",
		DataType: rdbmstool.CHAR, Length: 32, IsNullable: false}
}

func createEndDateColumn() rdbmstool.ColumnDefinition {
	return rdbmstool.ColumnDefinition{Name: END_DATE,
		DataType: rdbmstool.DATETIME, Length: 0, IsNullable: true}
}

func createLoadDateColumn() rdbmstool.ColumnDefinition {
	return rdbmstool.ColumnDefinition{Name: LOAD_DATE,
		DataType: rdbmstool.DATETIME, Length: 0, IsNullable: false}
}

func createRecordSourceColumn() rdbmstool.ColumnDefinition {
	return rdbmstool.ColumnDefinition{Name: RECORD_SOURCE,
		DataType: rdbmstool.CHAR, Length: 100, IsNullable: false}
}

func createIndexKey(colName string) rdbmstool.IndexKeyDefinition {
	return rdbmstool.IndexKeyDefinition{ColumnNames: []string{colName}}
}
