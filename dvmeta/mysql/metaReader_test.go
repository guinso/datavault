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
		DbName: "test"}

	hubDef, err := metaReader.GetHubDefinition("Invoice", 0, transaction)

	if err != nil {
		t.Error(err.Error())
		transaction.Rollback()
		return
	}

	if strings.Compare(hubDef.Name, "Invoice") != 0 {
		t.Errorf("Expect hub name is %s, given %s instead", "Invoice", hubDef.Name)
	}

	if hubDef.Revision != 0 {
		t.Errorf("Expect hub revision is %d, given %d instead", 0, hubDef.Revision)
	}

	transaction.Rollback()
}

func TestGetLinkDefinition(t *testing.T) {
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
		DbName: "test"}

	linkDef, err := metaReader.GetLinkDefinition("InvoiceOrderItem", 0, transaction)

	if err != nil {
		t.Error(err.Error())
		transaction.Rollback()
		return
	}

	if linkDef == nil {
		t.Error("NULL link definition instance detected")
	}

	transaction.Rollback()
}

func TestGetSateliteDefinition(t *testing.T) {
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
		DbName: "test"}

	linkDef, err := metaReader.GetSateliteDefinition("Invoice", 0, transaction)

	if err != nil {
		t.Error(err.Error())
		transaction.Rollback()
		return
	}

	if linkDef == nil {
		t.Error("NULL satelite definition instance detected")
	}

	transaction.Rollback()
}

func TestGetAllEntities(t *testing.T) {
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
		DbName: "test"}

	hubs := metaReader.GetAllHubs(transaction)
	if len(hubs) == 0 {
		t.Error("Expect hubs count more than 0")
	}

	links := metaReader.GetAllLinks(transaction)
	if len(links) == 0 {
		t.Error("Expect links count more than 0")
	}

	sats := metaReader.GetAllSatelites(transaction)
	if len(sats) == 0 {
		t.Error("Expect satelites count more than 0")
	}

	entities := metaReader.SearchEntities(transaction, "invoice")
	if len(entities) == 0 {
		t.Error("Expect entities with invoice term is more than 0")
	}

	transaction.Rollback()
}

func TestGetRelationship(t *testing.T) {
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
		DbName: "test"}

	hubDescriptor, err := metaReader.GetRelationship(transaction, "Invoice", 0)
	if err != nil {
		t.Error(err.Error())
		transaction.Rollback()
		return
	}

	if hubDescriptor == nil {
		t.Error("return value should not be NULL")
	}

	//dump hubDescriptor
	/*
		t.Log(fmt.Sprintf("==== %s(%d) Descriptor ====",
			hubDescriptor.HubName, hubDescriptor.HubRevision), "")
		for satIndex, satDef := range hubDescriptor.Satelites {
			t.Log(fmt.Sprintf("#Satelite[%d] %s(%d)", satIndex, satDef.Name, satDef.Revision), "")
			for _, attr := range satDef.Attributes {
				t.Log(fmt.Sprintf("--%s [%s]", attr.Name, attr.DataType.String()))
			}
		}
		t.Log(fmt.Sprintf("-----------------"), "")
		for linkIndex, linkHub := range hubDescriptor.Links {
			for _, satDef := range linkHub.Satelites {
				t.Log(fmt.Sprintf("#Link-Sat[%d] %s(%d)",
					linkIndex, satDef.Name, satDef.Revision), "")
				for _, attr := range satDef.Attributes {
					t.Log(fmt.Sprintf("--%s [%s]", attr.Name, attr.DataType.String()))
				}
			}
		}
		t.Error("***Intentionally make it failed to enable printing log message***")*/

	transaction.Rollback()

}
