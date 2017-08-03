package definition

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/guinso/rdbmstool"

	//explicitly include GO mysql library
	_ "github.com/go-sql-driver/mysql"
)

func TestCreateHub(t *testing.T) {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8", "root", "", "localhost", 3306, "test"))

	if err != nil {
		t.Error(err.Error())
		return
	}

	tx, txErr := db.Begin()
	if txErr != nil {
		t.Error(txErr.Error())
		return
	}

	//drop link
	if _, err := tx.Exec("DROP TABLE IF EXISTS `link_invoice_order_item_rev0`"); err != nil {
		t.Errorf("Fail to drop link %s, revision %d: %s", "InvoiceOrderItem", 0, err.Error())
		tx.Rollback()
		return
	}

	//drop satelite
	if _, err := tx.Exec("DROP TABLE IF EXISTS `sat_invoice_rev0`"); err != nil {
		t.Errorf("Fail to drop satelite %s, revision %d: %s", "Invoice", 0, err.Error())
		tx.Rollback()
		return
	}
	if _, err := tx.Exec("DROP TABLE IF EXISTS `sat_invoice_order_rev0`"); err != nil {
		t.Errorf("Fail to drop satelite %s, revision %d: %s", "InvoiceOrder", 0, err.Error())
		tx.Rollback()
		return
	}

	_testCreateHub(tx, t, &HubDefinition{
		Name:         "Invoice",
		Revision:     0,
		BusinessKeys: []string{"InvoiceNo"}})

	_testCreateHub(tx, t, &HubDefinition{
		Name:         "InvoiceOrder",
		Revision:     0,
		BusinessKeys: []string{}})

	tx.Rollback()
}

func _testCreateHub(db rdbmstool.DbHandlerProxy, t *testing.T, hubDef *HubDefinition) {
	sql, sqlErr := hubDef.GenerateSQL()
	if sqlErr != nil {
		t.Error(sqlErr.Error())
	}

	//drop hub
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + hubDef.GetDbTableName() + "`"); err != nil {
		t.Errorf("Fail to drop hub %s, revision %d: %s", hubDef.Name, hubDef.Revision, err.Error())
		return
	}
	//create hub
	if _, err := db.Exec(sql); err != nil {
		t.Errorf("Fail to create hub %s revision %d: %s", hubDef.Name, hubDef.Revision, err.Error())
		return
	}

	//create again, should be throw error
	if _, err := db.Exec(sql); err == nil {
		t.Errorf("Create hub %s revision %d should be fail", hubDef.Name, hubDef.Revision)
		return
	}
}
