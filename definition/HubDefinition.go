package definition

import (
	"fmt"

	"github.com/guinso/datavault/sqlgenerator"
	"github.com/guinso/stringtool"
)

//HubDefinition is schema to descibe hub structure
type HubDefinition struct {
	Name         string
	BusinessKeys []string
	Revision     int
}

//GetHashKey is to generate data table equivalent hash key column name
func (hubDef *HubDefinition) GetHashKey() string {
	return fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(hubDef.Name))
}

//GetDbTableName is to generate equivalent data table name
func (hubDef *HubDefinition) GetDbTableName() string {
	return fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubDef.Name), hubDef.Revision)
}

// GenerateSQL is to generate SQL statement based on hub definition
func (hubDef *HubDefinition) GenerateSQL() (string, error) {
	var sql string

	tableDef := sqlgenerator.TableDefinition{
		Name:        hubDef.GetDbTableName(),
		PrimaryKey:  []string{hubDef.GetHashKey()},
		UniqueKeys:  []sqlgenerator.UniqueKeyDefinition{},
		ForiegnKeys: []sqlgenerator.ForeignKeyDefinition{},
		Indices:     []sqlgenerator.IndexKeyDefinition{},
		Columns: []sqlgenerator.ColumnDefinition{
			createHashKeyColumn(hubDef.Name),
			createLoadDateColumn(),
			createRecordSourceColumn()}}

	if len(hubDef.BusinessKeys) > 0 {
		var uks []string

		for _, bk := range hubDef.BusinessKeys {
			tableDef.Columns = append(tableDef.Columns,
				sqlgenerator.ColumnDefinition{Name: stringtool.ToSnakeCase(bk),
					DataType: sqlgenerator.CHAR, Length: 100, IsNullable: false})

			uks = append(uks, stringtool.ToSnakeCase(bk))
		}

		tableDef.UniqueKeys = append(tableDef.UniqueKeys,
			sqlgenerator.UniqueKeyDefinition{ColumnNames: uks})
	}

	sql, err := sqlgenerator.GenerateTableSQL(&tableDef)

	if err != nil {
		return "", err
	}

	return sql, nil
}
