package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	//explicitly include GO mysql library
	_ "github.com/go-sql-driver/mysql"
)

func TestGetHubDefinition(t *testing.T) {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8", "root", "", "localhost", 3306, "test"))

	if err != nil {
		t.Error(err.Error())
		return
	}

	transaction, txErr := db.Begin()

	if txErr != nil {
		t.Error(txErr.Error())
		transaction.Rollback()
		return
	}

	metaReader := MetaReader{
		Db:     db,
		DbName: "test"}

	hubDef, err := metaReader.GetHubDefinition("TaxInvoice", 0, transaction)

	if err != nil {
		t.Error(err.Error())
		transaction.Rollback()
		return
	}

	if strings.Compare(hubDef.Name, "TaxInvoice") != 0 {
		t.Errorf("Expect hub name is %s, given %s instead", "TaxInvoice", hubDef.Name)
	}

	if hubDef.Revision != 0 {
		t.Errorf("Expect hub revision is %d, given %d instead", 0, hubDef.Revision)
	}

	transaction.Rollback()
}
