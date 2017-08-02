package definition

import (
	"errors"
	"fmt"

	"github.com/guinso/rdbmstool"
	"github.com/guinso/stringtool"
)

//LinkDefinition is schema to descibe link structure
type LinkDefinition struct {
	Name          string
	Revision      int
	HubReferences []HubReference
}

//GetHashKey is to generate data table equivalent hash key column name
func (linkDef *LinkDefinition) GetHashKey() string {
	return fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(linkDef.Name))
}

//GetDbTableName is to generate equivalent data table name
func (linkDef *LinkDefinition) GetDbTableName() string {
	return fmt.Sprintf("link_%s_rev%d", stringtool.ToSnakeCase(linkDef.Name), linkDef.Revision)
}

// GenerateSQL is to generate SQL statement based on link definition
func (linkDef *LinkDefinition) GenerateSQL() (string, error) {
	if linkDef == nil || linkDef.HubReferences == nil || len(linkDef.HubReferences) < 2 {
		//why atleast two hub reference?
		//1. point to main hub
		//2. point to reference hub (one or more)
		//example:
		//<link_invoice_address>
		//ref 1 is point to invoice (point to main hub)
		//ref 2 is point to address (point to reference hub)
		return "", errors.New("link definition must has atleast two hub reference")
	}

	tableDef := rdbmstool.TableDefinition{
		Name:        linkDef.GetDbTableName(),
		PrimaryKey:  []string{linkDef.GetHashKey()},
		UniqueKeys:  []rdbmstool.UniqueKeyDefinition{},
		ForiegnKeys: []rdbmstool.ForeignKeyDefinition{},
		Columns: []rdbmstool.ColumnDefinition{
			createHashKeyColumn(linkDef.Name),
			createLoadDateColumn(),
			createRecordSourceColumn()}}

	for _, hubRef := range linkDef.HubReferences {
		tableDef.Columns = append(tableDef.Columns, createHashKeyColumn(hubRef.HubName))

		tableDef.Indices = append(tableDef.Indices,
			rdbmstool.IndexKeyDefinition{ColumnNames: []string{hubRef.GetHashKey()}})
		tableDef.ForiegnKeys = append(tableDef.ForiegnKeys,
			rdbmstool.ForeignKeyDefinition{
				Columns: []rdbmstool.FKColumnDefinition{
					rdbmstool.FKColumnDefinition{
						ColumnName:    hubRef.GetHashKey(),
						RefColumnName: hubRef.GetHashKey()}},
				ReferenceTableName: hubRef.GetDbTableName()})
	}

	sql, err := rdbmstool.GenerateTableSQL(&tableDef)
	if err != nil {
		return "", err
	}

	return sql, nil
}
