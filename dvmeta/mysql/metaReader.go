package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/guinso/stringtool"

	"errors"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/datavault/dvmeta"
)

//MetaReader implementation of DataVaultMetaReader
type MetaReader struct {
	Db     *sql.DB
	DbName string
}

type dataTableColumn struct {
	ColumnName       string
	OrdinalPosition  int
	DefaultValue     sql.NullString
	IsNullable       bool //varchar(3)
	DataType         string
	CharMaxLength    sql.NullInt64
	NumericLength    sql.NullInt64
	NumericPrecision sql.NullInt64
	//NumericScale int
	DatetimePrecision sql.NullInt64
	CharSetName       sql.NullString
	CollationName     sql.NullString
	ColumnKey         string
}

//GetHubDefinition to get hub metainfo based on hub name and its revision number
//example hub name: TaxInvoice, revision: 0
func (metaReader *MetaReader) GetHubDefinition(hubName string, revision int) (*definition.HubDefinition, error) {
	hubDbName := fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hubName), revision)

	cols, colErr := getDataTableColumns(metaReader.Db, metaReader.DbName, hubDbName)

	if colErr != nil {
		return nil, errors.New("Failed to read column metadata for data table '" +
			hubDbName + "': " + colErr.Error())
	}

	hubDef := definition.HubDefinition{
		Name:     hubName,
		Revision: revision}
	hubHashKey := fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(hubName))
	hasHashKeyCol := false
	hasLoadDateCol := false
	hasRecordSourceCol := false
	rowCount := 0
	for _, col := range cols {
		rowCount++

		switch col.DataType {
		case "char":
			if strings.Compare(col.ColumnName, hubHashKey) == 0 {
				hasHashKeyCol = true
			} else if strings.Compare(col.ColumnName, "record_source") == 0 {
				hasRecordSourceCol = true
			} else {
				//append business key
				hubDef.BusinessKeys = append(hubDef.BusinessKeys,
					stringtool.SnakeToCamelCase(col.ColumnName))
			}
			break
		case "datetime":
			if strings.Compare(col.ColumnName, "load_date") == 0 {
				hasLoadDateCol = true
			} else {
				return nil, fmt.Errorf(
					"Unrecognized column found in hub: %s", col.ColumnName)
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
func (metaReader *MetaReader) GetLinkDefinition(linkName string, revision int) (*definition.LinkDefinition, error) {
	return nil, errors.New("Not implemented yet")
}

func (metaReader *MetaReader) GetSateliteDefinition(satName string, revision int) (*definition.SateliteDefinition, error) {
	return nil, errors.New("Not implemented yet")
}

//getDataTableColumns get all datatable's column(s) definition
func getDataTableColumns(db *sql.DB, dbName string, tableName string) ([]dataTableColumn, error) {
	rows, err := db.Query("SELECT column_name, ordinal_position, column_default, "+
		"is_nullable, data_type, character_maximum_length,  "+
		"character_octet_length, numeric_precision, datetime_precision, "+
		"character_set_name, collation_name, column_key "+
		"FROM information_schema.columns "+
		"WHERE table_schema=? AND table_name=?", dbName, tableName)

	if err != nil {
		return nil, err
	}

	columns := []dataTableColumn{}
	var columnName string
	var ordinalPosition int
	var defaultValue sql.NullString
	var isNull string
	var dataType string
	var charMaxLength sql.NullInt64
	var numericLength sql.NullInt64
	var numericPrecision sql.NullInt64
	var datetimePrecision sql.NullInt64
	var charset sql.NullString
	var collation sql.NullString
	var colKey string
	for rows.Next() {

		err := rows.Scan(&columnName, &ordinalPosition, &defaultValue,
			&isNull, &dataType, &charMaxLength, &numericLength,
			&numericPrecision, &datetimePrecision, &charset, &collation, &colKey)

		if err != nil {
			return nil, err
		}

		isNullable := strings.Compare(isNull, "YES") == 0
		columnValue := dataTableColumn{
			ColumnName:        columnName,
			OrdinalPosition:   ordinalPosition,
			DefaultValue:      defaultValue,
			IsNullable:        isNullable,
			DataType:          dataType,
			CharMaxLength:     charMaxLength,
			NumericLength:     numericLength,
			NumericPrecision:  numericPrecision,
			DatetimePrecision: datetimePrecision,
			CharSetName:       charset,
			CollationName:     collation,
			ColumnKey:         colKey}

		columns = append(columns, columnValue)
	}

	return columns, nil
}

//GetDbMetaTableName to get list of datatables' name which start with provided keyword
func getDbMetaTableName(db *sql.DB, databaseName string, tableNamePrefix string) ([]dvmeta.EntityInfo, error) {
	rows, err := db.Query("SELECT table_name FROM information_schema.tables"+
		" where table_schema=? AND table_name LIKE '"+tableNamePrefix+"%'", databaseName)

	if err != nil {
		return nil, err
	}

	var result []dvmeta.EntityInfo
	for rows.Next() {
		var tmp string
		err := rows.Scan(&tmp)

		if err != nil {
			continue
		}

		trimStr := tmp[4:]
		x := strings.Split(trimStr, "_rev")

		if len(x) != 2 {
			continue
		}

		revision, convErr := strconv.Atoi(x[1])

		if convErr != nil {
			continue
		}

		result = append(result, dvmeta.EntityInfo{
			Name:     stringtool.SnakeToCamelCase(x[0]),
			Revision: revision})
	}

	return result, nil
}

func (metaReader *MetaReader) execSQL(sql string, transaction *sql.Tx) error {
	_, execErr := transaction.Exec(sql)
	if execErr != nil {
		return execErr
	}

	return nil
}

//GetAllHubs list all available hub(s) entity in given database schema
func (metaReader *MetaReader) GetAllHubs() []dvmeta.EntityInfo {
	x, err := getDbMetaTableName(metaReader.Db, metaReader.DbName, "hub_")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetAllLinks list all available link(s) entity in given database schema
func (metaReader *MetaReader) GetAllLinks() []dvmeta.EntityInfo {
	x, err := getDbMetaTableName(metaReader.Db, metaReader.DbName, "link_")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}

//GetAllSatelites list all available satelite(s) entity in given database schema
func (metaReader *MetaReader) GetAllSatelites() []dvmeta.EntityInfo {
	x, err := getDbMetaTableName(metaReader.Db, metaReader.DbName, "satelite_")

	if err != nil {
		return []dvmeta.EntityInfo{}
	}

	return x
}
