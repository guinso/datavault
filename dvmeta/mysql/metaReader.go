package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/datavault/dvmeta"
	"github.com/guinso/rdbmstool"
	mysqlMeta "github.com/guinso/rdbmstool/mysql"
	"github.com/guinso/stringtool"
)

//MetaReader implementation of both DataVaultMetaReader and DataVaultMetaReaderTx
type MetaReader struct {
	Db     *sql.DB
	DbName string
}

//GetHubDefinition to get hub metainfo based on hub name and its revision number in transaction mode
//*must provide database transaction handler
//example hub name: TaxInvoice, revision: 0
func (metaReader *MetaReader) GetHubDefinition(
	hubName string, revision int, dbHandler rdbmstool.DbHandlerProxy) (*definition.HubDefinition, error) {

	hubDbName := fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubName), revision)

	tableDef, defErr := mysqlMeta.GetTableDefinition(dbHandler, metaReader.DbName, hubDbName)
	if defErr != nil {
		return nil, defErr
	}

	hubDef := definition.HubDefinition{
		Name:     hubName,
		Revision: revision}
	hubHashKey := metaReader.makeDVHashKey(hubName)
	hasHashKeyCol := false
	hasLoadDateCol := false
	hasRecordSourceCol := false
	rowCount := 0
	for _, col := range tableDef.Columns {
		rowCount++

		switch col.DataType {
		case rdbmstool.CHAR:
			if strings.Compare(col.Name, hubHashKey) == 0 {
				hasHashKeyCol = true
			} else if strings.Compare(col.Name, "record_source") == 0 {
				hasRecordSourceCol = true
			} else {
				//append business key
				hubDef.BusinessKeys = append(hubDef.BusinessKeys,
					stringtool.SnakeToCamelCase(col.Name))
			}
			break
		case rdbmstool.DATETIME:
			if strings.Compare(col.Name, "load_date") == 0 {
				hasLoadDateCol = true
			} else {
				return nil, fmt.Errorf(
					"Unrecognized column found in hub: %s", col.Name)
			}
			break
		default:
			return nil, fmt.Errorf(
				"Unsupported datatype (%s) parse into HubDefinition", col.DataType)
		}
	}

	if rowCount == 0 {
		return nil, fmt.Errorf("Data table %s not found in database", hubDbName)
	}

	if !hasHashKeyCol {
		return nil, fmt.Errorf("Hash key column not found in hub %s", hubDbName)
	}

	if !hasLoadDateCol {
		return nil, fmt.Errorf("Load date column not found in hub %s", hubDbName)
	}

	if !hasRecordSourceCol {
		return nil, fmt.Errorf("Record source column not found in hub %s", hubDbName)
	}

	return &hubDef, nil
}

//GetLinkDefinition to get link metainfo based on link name and its revision number
func (metaReader *MetaReader) GetLinkDefinition(linkName string, revision int, dbHandler rdbmstool.DbHandlerProxy) (*definition.LinkDefinition, error) {
	linkDbName := fmt.Sprintf("link_%s_rev%d", stringtool.ToSnakeCase(linkName), revision)

	//read all FK records
	tableDef, tableErr := mysqlMeta.GetTableDefinition(dbHandler, metaReader.DbName, linkDbName)
	if tableErr != nil {
		return nil, tableErr
	}

	linkDefinition := definition.LinkDefinition{
		Name:          linkName,
		Revision:      revision,
		HubReferences: []definition.HubReference{}}

	hasHashKey := false
	hasLoadDate := false
	hasRecordSource := false
	for _, col := range tableDef.Columns {
		switch col.DataType {
		case rdbmstool.CHAR:
			if strings.Compare(col.Name, metaReader.makeDVHashKey(linkName)) == 0 {
				hasHashKey = true
			} else if strings.Compare(col.Name, "record_source") == 0 {
				hasRecordSource = true
			}
			break
		case rdbmstool.DATETIME:
			if strings.Compare(col.Name, "load_date") == 0 {
				hasLoadDate = true
			}
			break
		default:
			return nil, fmt.Errorf(
				"Unsupported datatype (%s) parse into LinkDefinition", col.DataType.String())
		}
	}

	if !hasHashKey {
		return nil, fmt.Errorf("Hash key column not found in link %s", linkDbName)
	}

	if !hasLoadDate {
		return nil, fmt.Errorf("Load date column not found in link %s", linkDbName)
	}

	if !hasRecordSource {
		return nil, fmt.Errorf("Record source column not found in link %s", linkDbName)
	}

	for _, fk := range tableDef.ForiegnKeys {
		if len(fk.Columns) != 1 {
			return nil, fmt.Errorf("Link entity only support one pair of FK reference"+
				" but found %s has %d pair instead", fk.Name, len(fk.Columns))
		}

		_, name, revision, extractErr := extractDbEntityName(linkDbName)
		if extractErr != nil {
			return nil, extractErr
		}
		linkDefinition.HubReferences = append(linkDefinition.HubReferences,
			definition.HubReference{
				HubName:  name,
				Revision: revision})
	}

	if len(linkDefinition.HubReferences) < 2 {
		return nil, fmt.Errorf("invalid link entity: atleast two hub references must be presense but found %d reference only",
			len(linkDefinition.HubReferences))
	}

	return &linkDefinition, nil
}

//GetSateliteDefinition to get satelite metainfo based on satelite name and its revision number
func (metaReader *MetaReader) GetSateliteDefinition(satName string, revision int, dbHandler rdbmstool.DbHandlerProxy) (*definition.SateliteDefinition, error) {
	return nil, errors.New("Not implemented yet")
}

//GetAllHubs list all available hub(s) entity in given database schema
func (metaReader *MetaReader) GetAllHubs() []dvmeta.EntityInfo {
	x, err := getTableName(metaReader.Db, metaReader.DbName, "hub_")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetAllLinks list all available link(s) entity in given database schema
func (metaReader *MetaReader) GetAllLinks() []dvmeta.EntityInfo {
	x, err := getTableName(metaReader.Db, metaReader.DbName, "link_")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetAllSatelites list all available satelite(s) entity in given database schema
func (metaReader *MetaReader) GetAllSatelites() []dvmeta.EntityInfo {
	x, err := getTableName(metaReader.Db, metaReader.DbName, "satelite_")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

func (metaReader *MetaReader) makeDVHashKey(entityName string) string {
	return fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(entityName))
}

//GetDbMetaTableName to get list of datatables' name which start with provided keyword
func getTableName(db rdbmstool.DbHandlerProxy, databaseName string, tableNamePrefix string) ([]dvmeta.EntityInfo, error) {

	tables, tableErr := mysqlMeta.GetTableNames(db, databaseName, tableNamePrefix+"%")
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
	if strings.Compare(dbTableName, "hub_") == 0 {
		prefix = "hub_"
		entityType = definition.HUB

	} else if strings.Compare(dbTableName, "link_") == 0 {
		prefix = "link_"
		entityType = definition.LINK

	} else if strings.Compare(dbTableName, "sat_") == 0 {
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
