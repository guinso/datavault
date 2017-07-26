package definition

import (
	"errors"
	"fmt"

	"github.com/guinso/datavault/sqlgenerator"
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
		return "", errors.New("link definition must has atleast two hub reference")
	}

	tableDef := sqlgenerator.TableDefinition{
		Name:        linkDef.GetDbTableName(),
		PrimaryKey:  []string{linkDef.GetHashKey()},
		UniqueKeys:  []sqlgenerator.UniqueKeyDefinition{},
		ForiegnKeys: []sqlgenerator.ForeignKeyDefinition{},
		Columns: []sqlgenerator.ColumnDefinition{
			createHashKeyColumn(linkDef.Name),
			createLoadDateColumn(),
			createRecordSourceColumn()}}

	for _, hubRef := range linkDef.HubReferences {
		tableDef.Columns = append(tableDef.Columns, createHashKeyColumn(hubRef.HubName))

		tableDef.Indices = append(tableDef.Indices,
			sqlgenerator.IndexKeyDefinition{ColumnNames: []string{hubRef.GetHashKey()}})
		tableDef.ForiegnKeys = append(tableDef.ForiegnKeys,
			sqlgenerator.ForeignKeyDefinition{
				ColumnName:          hubRef.GetHashKey(),
				ReferenceTableName:  hubRef.GetDbTableName(),
				ReferenceColumnName: hubRef.GetHashKey()})
	}

	sql, err := sqlgenerator.GenerateTableSQL(&tableDef)
	if err != nil {
		return "", err
	}

	return sql, nil
}
