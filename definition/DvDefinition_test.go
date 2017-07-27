package definition

import (
	"testing"

	"github.com/guinso/datavault/sqlgenerator"
)

func TestDataVaultDefinitionGenerateSQL(t *testing.T) {
	dvDef := DataVaultDefinition{
		Hubs: []HubDefinition{
			HubDefinition{
				Name: "invoice",
				BusinessKeys: []string{
					"docNo"},
				Revision: 0}},
		satelites: []SateliteDefinition{
			SateliteDefinition{
				Name: "invoice",
				HubReference: &HubReference{
					HubName:  "invoice",
					Revision: 0},
				Revision: 0,
				Attributes: []SateliteAttributeDefinition{
					SateliteAttributeDefinition{
						Name:             "date",
						DataType:         sqlgenerator.DATE,
						Length:           0,
						IsNullable:       false,
						DecimalPrecision: 0},
					SateliteAttributeDefinition{
						Name:       "remark",
						DataType:   sqlgenerator.TEXT,
						IsNullable: true},
					SateliteAttributeDefinition{
						Name:       "status",
						DataType:   sqlgenerator.INTEGER,
						IsNullable: false,
						Length:     1}}}},
		Links: []LinkDefinition{
			LinkDefinition{
				Name:     "invPreparedBy",
				Revision: 0,
				HubReferences: []HubReference{
					HubReference{
						HubName:  "invoice",
						Revision: 0},
					HubReference{
						HubName:  "employee",
						Revision: 0}}}}}

	_, err := dvDef.GenerateSQL()
	if err != nil {
		t.Error(err.Error())
		return
	}
}
