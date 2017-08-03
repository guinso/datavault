package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/datavault/dvmeta"
	"github.com/guinso/rdbmstool"
	mysqlMeta "github.com/guinso/rdbmstool/mysql"
	"github.com/guinso/stringtool"
)

//GetDbMetaTableName to get list of datatables' name which start with provided keyword
func getTableName(db rdbmstool.DbHandlerProxy, databaseName string, keyword string) ([]dvmeta.EntityInfo, error) {

	tables, tableErr := mysqlMeta.GetTableNames(db, databaseName, keyword)
	if tableErr != nil {
		return nil, fmt.Errorf("DV MySQL meta reader fail to query data table from database: " + tableErr.Error())
	}

	var result []dvmeta.EntityInfo
	for _, table := range tables {
		entity, name, revision, err := extractDbEntityName(table)

		if err == nil {
			result = append(result, dvmeta.EntityInfo{
				Type:     entity,
				Name:     name,
				Revision: revision})
		}
	}

	return result, nil
}

func extractDbEntityName(dbTableName string) (
	definition.EntityType, string, int, error) {

	//validate prefix
	var prefix string
	var entityType definition.EntityType
	if strings.HasPrefix(dbTableName, "hub_") {
		prefix = "hub_"
		entityType = definition.HUB

	} else if strings.HasPrefix(dbTableName, "link_") {
		prefix = "link_"
		entityType = definition.LINK

	} else if strings.HasPrefix(dbTableName, "sat_") {
		prefix = "sat_"
		entityType = definition.SATELITE

	} else {
		return 0, "", 0, fmt.Errorf("Unrecognized db table for data vault: %s", dbTableName)
	}

	//extract and validate entity name
	trimHeader := strings.TrimPrefix(dbTableName, prefix)
	raws := strings.Split(trimHeader, "_rev")
	if len(raws) != 2 {
		return 0, "", 0, fmt.Errorf("Invalid data vault db table name format: %s",
			dbTableName)
	}
	name := stringtool.SnakeToCamelCase(raws[0])

	//validate suffix (_rev)
	rev, revErr := strconv.Atoi(raws[1])
	if revErr != nil {
		return 0, "", 0, fmt.Errorf("Invalid revision value %s from table name %s",
			raws[1], dbTableName)
	}

	return entityType, name, rev, nil
}

func makeDVHashKey(entityName string) string {
	return fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(entityName))
}
