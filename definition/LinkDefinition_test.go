package definition

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestCreateLink(t *testing.T) {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8", "root", "", "localhost", 3306, "test"))

	if err != nil {
		t.Error(err.Error())
		return
	}

	linkDef := LinkDefinition{
		Name:     "InvoiceOrderItem",
		Revision: 0,
		HubReferences: []HubReference{
			HubReference{
				HubName:  "Invoice",
				Revision: 0},
			HubReference{
				HubName:  "InvoiceOrder",
				Revision: 0}}}

	sql, sqlErr := linkDef.GenerateSQL()
	if sqlErr != nil {
		t.Error(sqlErr.Error())
	}

	//log.Println(sql)

	//drop link
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + linkDef.GetDbTableName() + "`"); err != nil {
		t.Errorf("Fail to drop link %s, revision %d: %s", linkDef.Name, linkDef.Revision, err.Error())
		return
	}
	//create hub
	if _, err := db.Exec(sql); err != nil {
		t.Errorf("Fail to create link %s revision %d: %s", linkDef.Name, linkDef.Revision, err.Error())
		return
	}

	//create again, should be throw error
	if _, err := db.Exec(sql); err == nil {
		t.Errorf("Create link %s revision %d should be fail", linkDef.Name, linkDef.Revision)
		return
	}
}
