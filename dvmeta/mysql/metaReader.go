package mysql

import (
	"database/sql"

	"github.com/guinso/datavault/definition"
)

//MetaReader implementation of DataVaultMetaReader
type MetaReader struct {
}

//GetHubDefinition to get latest hub metainfo based on hub name
func GetHubDefinition(db *sql.DB, hubName string) (*definition.HubDefinition, error) {
	hubDef := definition.HubDefinition{}

	return &hubDef, nil
}

//GetDbMetaTableName to get list of datatables' name which start with provided keyword
func GetDbMetaTableName(db *sql.DB, databaseName string, tableNamePrefix string) []string {
	rows, err := db.Query("SELECT table_name FROM information_schema.tables"+
		" where table_schema=? AND table_name LIKE '"+tableNamePrefix+"%'", databaseName)

	if err != nil {
		return nil
	}

	var result []string
	for rows.Next() {
		var tmp string
		rows.Scan(&tmp)

		result = append(result, tmp)
	}

	return result
}

func execSQL(sql string, transaction *sql.Tx) error {
	_, execErr := transaction.Exec(sql)
	if execErr != nil {
		return execErr
	}

	return nil
}
