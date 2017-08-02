package definition

import (
	"fmt"

	"github.com/guinso/rdbmstool"
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

	tableDef := rdbmstool.TableDefinition{
		Name:        hubDef.GetDbTableName(),
		PrimaryKey:  []string{hubDef.GetHashKey()},
		UniqueKeys:  []rdbmstool.UniqueKeyDefinition{},
		ForiegnKeys: []rdbmstool.ForeignKeyDefinition{},
		Indices:     []rdbmstool.IndexKeyDefinition{},
		Columns: []rdbmstool.ColumnDefinition{
			createHashKeyColumn(hubDef.Name),
			createLoadDateColumn(),
			createRecordSourceColumn()}}

	if len(hubDef.BusinessKeys) > 0 {
		var uks []string

		for _, bk := range hubDef.BusinessKeys {
			tableDef.Columns = append(tableDef.Columns,
				rdbmstool.ColumnDefinition{Name: stringtool.ToSnakeCase(bk),
					DataType: rdbmstool.CHAR, Length: 100, IsNullable: false})

			uks = append(uks, stringtool.ToSnakeCase(bk))
		}

		tableDef.UniqueKeys = append(tableDef.UniqueKeys,
			rdbmstool.UniqueKeyDefinition{ColumnNames: uks})
	}

	sql, err := rdbmstool.GenerateTableSQL(&tableDef)

	if err != nil {
		return "", err
	}

	return sql, nil
}
