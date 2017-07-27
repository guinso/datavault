package definition

import (
	"database/sql"
	"fmt"
	"testing"

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

	hubDef := HubDefinition{
		Name:         "TaxInvoice",
		Revision:     0,
		BusinessKeys: []string{"InvoiceNo"}}

	sql, sqlErr := hubDef.GenerateSQL()
	if sqlErr != nil {
		t.Error(sqlErr.Error())
	}

	//drop hub_tax_invoice_rev0
	if _, err := db.Exec("DROP TABLE IF EXISTS `hub_tax_invoice_rev0`"); err != nil {
		t.Error("Fail to drop hub tax invoice revision 0: " + err.Error())
		return
	}
	//create hub tax invoice
	if _, err := db.Exec(sql); err != nil {
		t.Error("Fail to create hub TaxInvoice: " + err.Error())
		return
	}

	//create again, should be throw error
	if _, err := db.Exec(sql); err == nil {
		t.Error("Create hub tax invoice should be fail")
		return
	}
}
