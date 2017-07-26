package record

import (
	"errors"
	"fmt"
	"time"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/stringtool"
)

//HubInsertRecord is Hub insert record schema
type HubInsertRecord struct {
	HubName         string
	HubRevision     int
	RecordSource    string
	LoadDate        time.Time
	HashKey         string
	BusinessKeyVues []HubBusinessKeyInsertRecord
}

//HubBusinessKeyInsertRecord is Hub business ID insert record schema
type HubBusinessKeyInsertRecord struct {
	BusinessKey   string
	BusinessValue string
}

func (hub *HubInsertRecord) getDbTableName() string {
	return fmt.Sprintf("hub_%s_rev%d", stringtool.ToSnakeCase(hub.HubName), hub.HubRevision)
}

func (hub *HubInsertRecord) getHashKeyDbColumnName() string {
	return fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(hub.HubName))
}

//GenerateSQL to generate SQL insert statement for hub record
func (hub *HubInsertRecord) GenerateSQL() (string, error) {
	if hub.BusinessKeyVues == nil || len(hub.BusinessKeyVues) == 0 {
		return "", errors.New("hub must has atlest one business key value")
	}

	colSQL := fmt.Sprintf("`%s`, `%s`, `%s`",
		hub.getHashKeyDbColumnName(),
		definition.LOAD_DATE,
		definition.RECORD_SOURCE)

	valueSQL := fmt.Sprintf("'%s', '%s', '%s'",
		hub.HashKey,
		hub.LoadDate.Format("2006-01-02"),
		hub.RecordSource)

	for _, business := range hub.BusinessKeyVues {
		colSQL = colSQL + ", `" + stringtool.ToSnakeCase(business.BusinessKey) + "`"

		valueSQL = valueSQL + ", '" + business.BusinessValue + "'"
	}

	return fmt.Sprintf("INSERT INTO `%s` \n(%s) \nVALUES (%s)",
		hub.getDbTableName(), colSQL, valueSQL), nil
}
