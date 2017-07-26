package definition

import (
	"errors"
	"fmt"

	"github.com/guinso/datavault/sqlgenerator"
	"github.com/guinso/stringtool"
)

//SateliteDefinition is schema to describe satelite structure
type SateliteDefinition struct {
	Name         string
	HubReference *HubReference
	Attributes   []SateliteAttributeDefinition
	Revision     int
}

//SateliteAttributeDefinition is schema to descibe satelite attributes structure
type SateliteAttributeDefinition struct {
	Name             string
	DataType         sqlgenerator.ColumnDataType
	Length           int
	IsNullable       bool
	DecimalPrecision int
}

//GetDbTableName is function to generate equivalence datatable name
func (satDef *SateliteDefinition) GetDbTableName() string {
	return fmt.Sprintf("sat_%s_rev%d", stringtool.ToSnakeCase(satDef.Name), satDef.Revision)
}

// GenerateSQL is to generate SQL statement based on satelite definition
func (satDef *SateliteDefinition) GenerateSQL() (string, error) {
	if satDef == nil {
		return "", errors.New("Input parameter cannot be null")
	}

	if satDef.HubReference == nil {
		return "", errors.New("Satelite has no hub reference")
	}

	if satDef.Attributes == nil || len(satDef.Attributes) == 0 {
		return "", errors.New("Satelite must has atleast one attribute")
	}

	tableDef := sqlgenerator.TableDefinition{
		Name: fmt.Sprintf("sat_%s_rev%d", stringtool.ToSnakeCase(satDef.Name), satDef.Revision),
		Columns: []sqlgenerator.ColumnDefinition{
			createHashKeyColumn(satDef.HubReference.HubName),
			createLoadDateColumn(),
			createEndDateColumn(),
			createRecordSourceColumn()},
		PrimaryKey: []string{satDef.HubReference.GetHashKey(), LOAD_DATE},
		ForiegnKeys: []sqlgenerator.ForeignKeyDefinition{
			sqlgenerator.ForeignKeyDefinition{
				ColumnName:          satDef.HubReference.GetHashKey(),
				ReferenceColumnName: satDef.HubReference.GetHashKey(),
				ReferenceTableName:  satDef.HubReference.GetDbTableName()}},
		UniqueKeys: []sqlgenerator.UniqueKeyDefinition{},
		Indices: []sqlgenerator.IndexKeyDefinition{
			createIndexKey(satDef.HubReference.GetHashKey())}}

	for _, attribute := range satDef.Attributes {
		tableDef.Columns = append(tableDef.Columns, sqlgenerator.ColumnDefinition{
			Name:             stringtool.ToSnakeCase(attribute.Name),
			DataType:         attribute.DataType,
			Length:           attribute.Length,
			IsNullable:       attribute.IsNullable,
			DecimalPrecision: attribute.DecimalPrecision})
	}

	sql, err := sqlgenerator.GenerateTableSQL(&tableDef)
	if err != nil {
		return "", err
	}

	return sql, nil
}
