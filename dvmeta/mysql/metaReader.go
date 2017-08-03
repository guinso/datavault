package mysql

import (
	"fmt"
	"strings"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/datavault/dvmeta"
	"github.com/guinso/rdbmstool"
	mysqlMeta "github.com/guinso/rdbmstool/mysql"
	"github.com/guinso/stringtool"
)

//MetaReader implementation of both DataVaultMetaReader and DataVaultMetaReaderTx
type MetaReader struct {
	DbName string
}

//GetHubDefinition to get hub metainfo based on hub name and its revision number in transaction mode
//*must provide database transaction handler
//example hub name: TaxInvoice, revision: 0
func (metaReader *MetaReader) GetHubDefinition(
	hubName string, revision int, dbHandler rdbmstool.DbHandlerProxy) (
	*definition.HubDefinition, error) {

	hubDbName := fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubName), revision)

	tableDef, defErr := mysqlMeta.GetTableDefinition(dbHandler, metaReader.DbName, hubDbName)
	if defErr != nil {
		return nil, defErr
	}

	hubDef := definition.HubDefinition{
		Name:     hubName,
		Revision: revision}
	hubHashKey := makeDVHashKey(hubName)
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
func (metaReader *MetaReader) GetLinkDefinition(
	linkName string, revision int, dbHandler rdbmstool.DbHandlerProxy) (
	*definition.LinkDefinition, error) {

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

	expectedhasKey := makeDVHashKey(linkName)
	for _, col := range tableDef.Columns {
		switch col.DataType {
		case rdbmstool.CHAR:
			if strings.Compare(col.Name, expectedhasKey) == 0 {
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

		entityType, name, revision, extractErr := extractDbEntityName(fk.ReferenceTableName)
		if extractErr != nil {
			return nil, extractErr
		}

		if entityType == definition.HUB {
			linkDefinition.HubReferences = append(linkDefinition.HubReferences,
				definition.HubReference{
					HubName:  name,
					Revision: revision})
		}
	}

	if len(linkDefinition.HubReferences) < 2 {
		return nil, fmt.Errorf("invalid link entity: atleast two hub references must be presense but found %d reference only",
			len(linkDefinition.HubReferences))
	}

	return &linkDefinition, nil
}

//GetSateliteDefinition get satelite metainfo based on satelite name and its revision number
func (metaReader *MetaReader) GetSateliteDefinition(
	satName string, revision int, dbHandler rdbmstool.DbHandlerProxy) (
	*definition.SateliteDefinition, error) {

	satDbName := fmt.Sprintf("sat_%s_rev%d", stringtool.ToSnakeCase(satName), revision)

	//read all FK records
	tableDef, tableErr := mysqlMeta.GetTableDefinition(dbHandler, metaReader.DbName, satDbName)
	if tableErr != nil {
		return nil, tableErr
	}

	satDefinition := definition.SateliteDefinition{
		Name:       satName,
		Revision:   revision,
		Attributes: []definition.SateliteAttributeDefinition{},
	}

	hasHashKey := false
	hasLoadDate := false
	hasEndDate := false
	hasRecordSource := false
	//validate one and only foreign key
	if len(tableDef.ForiegnKeys) != 1 {
		return nil, fmt.Errorf("Satelite %s only allow one FK,"+
			" but found %d instead", satName, len(tableDef.ForiegnKeys))
	}
	fk := tableDef.ForiegnKeys[0]
	if len(fk.Columns) != 1 {
		return nil, fmt.Errorf("Satelite %s FK only allow one pair "+
			"binding but found %d instead", satName, len(fk.Columns))
	}
	entity, refName, refrev, refErr := extractDbEntityName(fk.ReferenceTableName)
	if refErr != nil {
		return nil, fmt.Errorf("Satelite %s FK has invalid reference table, %s: %s",
			satName, fk.ReferenceTableName, refErr.Error())
	}
	if entity != definition.HUB {
		return nil, fmt.Errorf("Satelite %s FK only allow to refer hub entity but found %s",
			satName, entity.String())
	}
	satDefinition.HubReference = &definition.HubReference{
		HubName:  refName,
		Revision: refrev}

	for _, col := range tableDef.Columns {
		switch col.DataType {
		case rdbmstool.BOOLEAN:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:       stringtool.SnakeToCamelCase(col.Name),
					DataType:   col.DataType,
					IsNullable: col.IsNullable})
			break
		case rdbmstool.CHAR:
			colHashKey := fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(refName))

			if strings.Compare(col.Name, "record_source") == 0 {
				hasRecordSource = true
			} else if strings.Compare(col.Name, colHashKey) == 0 {
				hasHashKey = true
			} else {
				satDefinition.Attributes = append(satDefinition.Attributes,
					definition.SateliteAttributeDefinition{
						Name:       stringtool.SnakeToCamelCase(col.Name),
						DataType:   col.DataType,
						Length:     col.Length,
						IsNullable: col.IsNullable})
			}
			break
		case rdbmstool.DATE:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:       stringtool.SnakeToCamelCase(col.Name),
					DataType:   col.DataType,
					IsNullable: col.IsNullable})
			break
		case rdbmstool.DATETIME:
			if strings.Compare(col.Name, "load_date") == 0 {
				hasLoadDate = true
			} else if strings.Compare(col.Name, "end_date") == 0 {
				hasEndDate = true
			} else {
				satDefinition.Attributes = append(satDefinition.Attributes,
					definition.SateliteAttributeDefinition{
						Name:       stringtool.SnakeToCamelCase(col.Name),
						DataType:   col.DataType,
						IsNullable: col.IsNullable})
			}
			break
		case rdbmstool.DECIMAL:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:             stringtool.SnakeToCamelCase(col.Name),
					DataType:         col.DataType,
					IsNullable:       col.IsNullable,
					Length:           col.Length,
					DecimalPrecision: col.DecimalPrecision})
			break
		case rdbmstool.FLOAT:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:       stringtool.SnakeToCamelCase(col.Name),
					DataType:   col.DataType,
					IsNullable: col.IsNullable})
			break
		case rdbmstool.INTEGER:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:       stringtool.SnakeToCamelCase(col.Name),
					DataType:   col.DataType,
					IsNullable: col.IsNullable,
					Length:     col.Length})
			break
		case rdbmstool.TEXT:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:       stringtool.SnakeToCamelCase(col.Name),
					DataType:   col.DataType,
					IsNullable: col.IsNullable})
			break
		case rdbmstool.VARCHAR:
			satDefinition.Attributes = append(satDefinition.Attributes,
				definition.SateliteAttributeDefinition{
					Name:       stringtool.SnakeToCamelCase(col.Name),
					DataType:   col.DataType,
					IsNullable: col.IsNullable,
					Length:     col.Length})
			break
		default:
			return nil, fmt.Errorf("Unsupported datatype for Satelite definition: %s",
				col.DataType.String())
		}
	}

	if !hasHashKey {
		return nil, fmt.Errorf("Hash key column not found in satelite %s", satDbName)
	}

	if !hasLoadDate {
		return nil, fmt.Errorf("Load date column not found in satelite %s", satDbName)
	}

	if !hasEndDate {
		return nil, fmt.Errorf("End date column not found in satelite %s", satDbName)
	}

	if !hasRecordSource {
		return nil, fmt.Errorf("Record source column not found in satelite %s", satDbName)
	}

	return &satDefinition, nil
}

//GetAllHubs list all available hub(s) entity in given database schema
func (metaReader *MetaReader) GetAllHubs(dbHandler rdbmstool.DbHandlerProxy) []dvmeta.EntityInfo {

	x, err := getTableName(dbHandler, metaReader.DbName, "hub_%")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetAllLinks list all available link(s) entity in given database schema
func (metaReader *MetaReader) GetAllLinks(dbHandler rdbmstool.DbHandlerProxy) []dvmeta.EntityInfo {
	x, err := getTableName(dbHandler, metaReader.DbName, "link_%")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetAllSatelites list all available satelite(s) entity in given database schema
func (metaReader *MetaReader) GetAllSatelites(dbHandler rdbmstool.DbHandlerProxy) []dvmeta.EntityInfo {
	x, err := getTableName(dbHandler, metaReader.DbName, "sat_%")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//SearchEntities list all available data vault entities based on given keyword
func (metaReader *MetaReader) SearchEntities(dbHandler rdbmstool.DbHandlerProxy, searchKeyword string) []dvmeta.EntityInfo {
	x, err := getTableName(dbHandler, metaReader.DbName, "%"+searchKeyword+"%")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetRelationship search all direct related links and satelites for provided hub
func (metaReader *MetaReader) GetRelationship(dbHandler rdbmstool.DbHandlerProxy, hubName string, hubRevision int) (*dvmeta.HubRelationship, error) {
	//get related satalites which refer to specified hub
	hubTableName := fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubName), hubRevision)
	tables, linkErr := mysqlMeta.GetLinkedFK(dbHandler, metaReader.DbName, hubTableName)
	if linkErr != nil {
		return nil, linkErr
	}

	result := dvmeta.HubRelationship{
		HubName:     hubName,
		HubRevision: hubRevision,
		Satelites:   []definition.SateliteDefinition{},
		Links:       []dvmeta.HubLinkRelationship{},
	}
	for _, tableName := range tables {
		entityType, name, rev, err := extractDbEntityName(tableName)
		if err != nil {
			return nil, err
		}

		switch entityType {
		case definition.SATELITE:
			satDef, satErr := metaReader.GetSateliteDefinition(name, rev, dbHandler)
			if satErr != nil {
				return nil, satErr
			}
			result.Satelites = append(result.Satelites, *satDef)
			break
		case definition.LINK:
			linkDef, linkErr := metaReader.GetLinkDefinition(name, rev, dbHandler)
			if linkErr != nil {
				return nil, linkErr
			}

			//append hub link relationship
			hubLink, hubLinkErr := metaReader.getHubLinkRelationship(
				dbHandler, linkDef, hubName, hubRevision)
			if hubLinkErr != nil {
				return nil, hubLinkErr
			}

			result.Links = append(result.Links, *hubLink)
			break
		}
	}

	return &result, nil
}

func (metaReader *MetaReader) getHubLinkRelationship(dbHandler rdbmstool.DbHandlerProxy, linkDef *definition.LinkDefinition,
	hubName string, hubRevision int) (*dvmeta.HubLinkRelationship, error) {

	hubLink := dvmeta.HubLinkRelationship{
		Definition: linkDef,
		Hubs:       []definition.HubDefinition{},
		Satelites:  []definition.SateliteDefinition{},
	}

	expectedTableName := fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubName), hubRevision)

	for _, hubRef := range linkDef.HubReferences {

		tmpTableName := fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubRef.HubName), hubRef.Revision)
		//append if it is not reference to entry point's hub name
		if strings.Compare(tmpTableName, expectedTableName) != 0 {
			//made a hub definition
			hubDef, hubErr := metaReader.GetHubDefinition(hubRef.HubName, hubRef.Revision, dbHandler)
			if hubErr != nil {
				return nil, hubErr
			}
			hubLink.Hubs = append(hubLink.Hubs, *hubDef)

			//search all related satelite(s) for linked hub
			tables, tableErr := mysqlMeta.GetLinkedFK(dbHandler, metaReader.DbName, hubRef.GetDbTableName())
			if tableErr != nil {
				return nil, tableErr
			}

			for _, table := range tables {
				entityType, name, rev, err := extractDbEntityName(table)
				if err != nil || entityType != definition.SATELITE {
					continue //skip if it is not a valid format satelite db table
				}

				satDef, satErr := metaReader.GetSateliteDefinition(name, rev, dbHandler)
				if satErr != nil {
					return nil, satErr
				}

				hubLink.Satelites = append(hubLink.Satelites, *satDef)
			}
		}
	}

	return &hubLink, nil
}
