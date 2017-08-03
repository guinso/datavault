package definition

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/guinso/rdbmstool"
)

func TestCreateSatelite(t *testing.T) {
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

	_testCreateSatelite(tx, t, &SateliteDefinition{
		Name:     "Invoice",
		Revision: 0,
		HubReference: &HubReference{
			HubName:  "Invoice",
			Revision: 0,
		},
		Attributes: []SateliteAttributeDefinition{
			SateliteAttributeDefinition{
				Name:       "DateOfIssue",
				DataType:   rdbmstool.DATE,
				IsNullable: false,
			},
			SateliteAttributeDefinition{
				Name:       "Remark",
				DataType:   rdbmstool.TEXT,
				IsNullable: true,
			},
			SateliteAttributeDefinition{
				Name:             "Tax",
				DataType:         rdbmstool.DECIMAL,
				Length:           10,
				DecimalPrecision: 2,
			},
		},
	})

	_testCreateSatelite(tx, t, &SateliteDefinition{
		Name:     "InvoiceOrder",
		Revision: 0,
		HubReference: &HubReference{
			HubName:  "InvoiceOrder",
			Revision: 0,
		},
		Attributes: []SateliteAttributeDefinition{
			SateliteAttributeDefinition{
				Name:       "No",
				DataType:   rdbmstool.INTEGER,
				Length:     10,
				IsNullable: false,
			},
			SateliteAttributeDefinition{
				Name:       "Description",
				DataType:   rdbmstool.CHAR,
				Length:     100,
				IsNullable: false,
			},
			SateliteAttributeDefinition{
				Name:             "UnitPrice",
				DataType:         rdbmstool.DECIMAL,
				Length:           10,
				DecimalPrecision: 2,
				IsNullable:       false,
			},
			SateliteAttributeDefinition{
				Name:       "Qty",
				DataType:   rdbmstool.INTEGER,
				Length:     10,
				IsNullable: false,
			},
		},
	})

	tx.Rollback()
}

func _testCreateSatelite(db rdbmstool.DbHandlerProxy, t *testing.T, satDef *SateliteDefinition) {
	sql, sqlErr := satDef.GenerateSQL()
	if sqlErr != nil {
		t.Error(sqlErr.Error())
	}

	//x log.Println(sql)

	//drop satelite
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + satDef.GetDbTableName() + "`"); err != nil {
		t.Errorf("Fail to drop satelite %s, revision %d: %s", satDef.Name, satDef.Revision, err.Error())
		return
	}
	//create satelite
	if _, err := db.Exec(sql); err != nil {
		t.Errorf("Fail to create satelite %s revision %d: %s", satDef.Name, satDef.Revision, err.Error())
		return
	}

	//create again, should be throw error
	if _, err := db.Exec(sql); err == nil {
		t.Errorf("Create satelite %s revision %d should be fail", satDef.Name, satDef.Revision)
		return
	}
}
