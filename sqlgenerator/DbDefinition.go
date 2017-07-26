package sqlgenerator

// ColumnDataType is enum for data column's data type
type ColumnDataType uint8

// Data table column's data type definition
const (
	CHAR     ColumnDataType = iota + 1
	INTEGER  ColumnDataType = iota + 1
	DECIMAL  ColumnDataType = iota + 1
	FLOAT    ColumnDataType = iota + 1
	TEXT     ColumnDataType = iota + 1
	DATE     ColumnDataType = iota + 1
	DATETIME ColumnDataType = iota + 1
	BOOLEAN  ColumnDataType = iota + 1
)

// TableDefinition is information to create a data table
type TableDefinition struct {
	Name        string
	Columns     []ColumnDefinition
	PrimaryKey  []string //PK can form by more than one column
	ForiegnKeys []ForeignKeyDefinition
	UniqueKeys  []UniqueKeyDefinition
	Indices     []IndexKeyDefinition
	//do I need to include encoding as well?
}

// ColumnDefinition is information to defined a data table column
type ColumnDefinition struct {
	Name             string
	DataType         ColumnDataType
	Length           int
	IsNullable       bool
	DecimalPrecision int
}

// ForeignKeyDefinition is information to create a RDBMS FK
type ForeignKeyDefinition struct {
	ColumnName          string
	ReferenceTableName  string
	ReferenceColumnName string
}

// UniqueKeyDefinition is information to create an unique key
// a single unique key can define by more than one columns
type UniqueKeyDefinition struct {
	ColumnNames []string
}

type IndexKeyDefinition struct {
	ColumnNames []string
}
