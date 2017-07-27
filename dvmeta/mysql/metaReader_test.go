package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	//explicitly include GO mysql library
	_ "github.com/go-sql-driver/mysql"
)

func TestGetDataTableColumns(t *testing.T) {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8", "root", "", "localhost", 3306, "test"))

	if err != nil {
		t.Error(err.Error())
		return
	}

	cols, err := getDataTableColumns(db, "test", "account")
	if err != nil {
		t.Error(err.Error())
		return
	}

	if len(cols) == 0 {
		t.Error("No column found for table 'account'")
		return
	}
}

func TestGetHubDefinition(t *testing.T) {
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8", "root", "", "localhost", 3306, "test"))

	if err != nil {
		t.Error(err.Error())
		return
	}

	metaReader := MetaReader{
		Db:     db,
		DbName: "test"}

	hubDef, err := metaReader.GetHubDefinition("TaxInvoice", 0)

	if err != nil {
		t.Error(err.Error())
		return
	}

	if strings.Compare(hubDef.Name, "TaxInvoice") != 0 {
		t.Errorf("Expect hub name is %s, given %s instead", "TaxInvoice", hubDef.Name)
	}

	if hubDef.Revision != 0 {
		t.Errorf("Expect hub revision is %d, given %d instead", 0, hubDef.Revision)
	}
}
