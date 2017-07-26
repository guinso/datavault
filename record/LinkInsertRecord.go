package record

import (
	"errors"
	"fmt"
	"time"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/stringtool"
)

//LinkInsertRecord is link insert record schema
type LinkInsertRecord struct {
	LinkName         string
	LinkRevision     int
	HashKey          string
	LoadDate         time.Time
	ReferenceHashKey []LinkReferenceInsertRecord
	RecordSource     string
}

//LinkReferenceInsertRecord is link's hub reference insert record schema
type LinkReferenceInsertRecord struct {
	HubName      string
	HashKeyValue string
}

func (link *LinkInsertRecord) getDbTableName() string {
	return fmt.Sprintf("link_%s_rev%d", stringtool.ToSnakeCase(link.LinkName), link.LinkRevision)
}

func (link *LinkInsertRecord) getHashKeyDbColumnName() string {
	return fmt.Sprintf("%s_hash_key", stringtool.ToSnakeCase(link.LinkName))
}

//GenerateSQL is to generate SQL insert statement for link schema
func (link *LinkInsertRecord) GenerateSQL() (string, error) {
	if link.ReferenceHashKey == nil || len(link.ReferenceHashKey) < 2 {
		return "", errors.New("Link must has atleast two reference hub")
	}

	colSQL := fmt.Sprintf("`%s`, `%s`, `%s`",
		link.getHashKeyDbColumnName(),
		definition.RECORD_SOURCE,
		definition.LOAD_DATE)

	valueSQL := fmt.Sprintf("'%s', '%s', '%s'", link.HashKey, link.RecordSource, link.LoadDate)

	for _, ref := range link.ReferenceHashKey {
		colSQL = colSQL + ", `" + stringtool.ToSnakeCase(ref.HubName) + "_hash_key`"
		valueSQL = valueSQL + ", '" + ref.HashKeyValue + "'"
	}

	return fmt.Sprintf("INSERT INTO `%s` \n(%s) \nVALUES (%s)",
		link.getDbTableName(), colSQL, valueSQL), nil
}
