package definition

import (
	"github.com/guinso/datavault/sqlgenerator"
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

func createHashKeyColumn(name string) sqlgenerator.ColumnDefinition {
	return sqlgenerator.ColumnDefinition{
		Name:     stringtool.ToSnakeCase(name) + "_hash_key",
		DataType: sqlgenerator.CHAR, Length: 32, IsNullable: false}
}

func createEndDateColumn() sqlgenerator.ColumnDefinition {
	return sqlgenerator.ColumnDefinition{Name: END_DATE,
		DataType: sqlgenerator.DATETIME, Length: 0, IsNullable: true}
}

func createLoadDateColumn() sqlgenerator.ColumnDefinition {
	return sqlgenerator.ColumnDefinition{Name: LOAD_DATE,
		DataType: sqlgenerator.DATETIME, Length: 0, IsNullable: false}
}

func createRecordSourceColumn() sqlgenerator.ColumnDefinition {
	return sqlgenerator.ColumnDefinition{Name: RECORD_SOURCE,
		DataType: sqlgenerator.CHAR, Length: 100, IsNullable: false}
}

func createIndexKey(colName string) sqlgenerator.IndexKeyDefinition {
	return sqlgenerator.IndexKeyDefinition{ColumnNames: []string{colName}}
}
