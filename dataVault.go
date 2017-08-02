package datavault

import (
	"database/sql"
	"fmt"

	"github.com/guinso/datavault/dvmeta"
	mysqlMeta "github.com/guinso/datavault/dvmeta/mysql"
	"github.com/guinso/datavault/record"

	//explicitly include GO mysql library
	_ "github.com/go-sql-driver/mysql"
)

//DataVault handler of data vault
type DataVault struct {
	DbName     string
	DbAddress  string
	Db         *sql.DB
	MetaReader dvmeta.DataVaultMetaReader
}

//CreateDV create data vault handler instance
func CreateDV(address string, username string, password string,
	dbName string, port int) (*DataVault, error) {

	//TODO:  handle various database vendor
	db, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8", username, password, address, port, dbName))

	if err != nil {
		return nil, err
	}

	//check connection is valid or not
	if pingErr := db.Ping(); pingErr != nil {
		return nil, pingErr
	}

	meta := mysqlMeta.MetaReader{
		DbName: dbName}

	dv := DataVault{
		DbName:     dbName,
		DbAddress:  address,
		Db:         db,
		MetaReader: &meta}

	return &dv, nil
}

//InsertRecord to insert new record into database
func (dv *DataVault) InsertRecord(dvInsertRecord *record.DvInsertRecord) error {
	sqls, sqlErr := dvInsertRecord.GenerateMultiSQL()

	if sqlErr != nil {
		return sqlErr
	}

	transaction, beginErr := dv.Db.Begin()
	if beginErr != nil {
		return beginErr
	}

	//TODO: test with various database vendor
	for _, sql := range sqls {
		execErr := dv.execSQL(sql, transaction)
		if execErr != nil {
			transaction.Rollback()
			return execErr
		}
	}

	commitErr := transaction.Commit()
	if commitErr != nil {
		return commitErr
	}

	return nil
}

func (dv *DataVault) execSQL(sql string, transaction *sql.Tx) error {
	_, execErr := transaction.Exec(sql)
	if execErr != nil {
		return execErr
	}

	return nil
}
