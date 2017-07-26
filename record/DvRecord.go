package record

import (
	"fmt"
	"time"
)

// DvInsertRecord is datavault insert record schema
type DvInsertRecord struct {
	LoadDate time.Time

	Hubs      []HubInsertRecord
	Links     []LinkInsertRecord
	Satelites []SateliteInsertRecord
}

//GenerateSQL is to generate SQL statement to represent a set of entities record
func (dv *DvInsertRecord) GenerateSQL() (string, error) {

	integrateErr := dv.checkIntegrity()
	if integrateErr != nil {
		return "", fmt.Errorf(
			"Unable to generate datavault insert record, "+
				"integrity fail:\n%s",
			integrateErr.Error())
	}

	var SQLstatement string

	//generate HUB SQL
	for _, hub := range dv.Hubs {
		hubSQL, hubErr := hub.GenerateSQL()

		if hubErr != nil {
			return "", fmt.Errorf(
				"Unable to generate insert SQL statement for entity HUB %s:\n%s",
				hub.HubName, hubErr.Error())
		}

		SQLstatement = SQLstatement + hubSQL + ";\n"
	}

	//generate LINK SQL
	for _, link := range dv.Links {
		linkSQL, linkErr := link.GenerateSQL()

		if linkErr != nil {
			return "", fmt.Errorf("Unable to generate insert SQL statement for entity Link %s:\n%s",
				link.LinkName,
				linkErr.Error())
		}

		SQLstatement = SQLstatement + linkSQL + ";\n"
	}

	//generate Satelite SQL
	for _, sat := range dv.Satelites {
		satSQL, satErr := sat.GenerateSQL()

		if satErr != nil {
			return "", fmt.Errorf("Unable to generate insert SQL statement for entity Satelite %s:\n%s",
				sat.SateliteName,
				satErr.Error())
		}

		SQLstatement = SQLstatement + satSQL + ";\n"
	}

	return SQLstatement, nil
}

//GenerateSQL is to generate SQL statement to represent a set of entities record
func (dv *DvInsertRecord) GenerateMultiSQL() ([]string, error) {

	integrateErr := dv.checkIntegrity()
	if integrateErr != nil {
		return nil, fmt.Errorf(
			"Unable to generate datavault insert record, "+
				"integrity fail:\n%s",
			integrateErr.Error())
	}

	var SQLstatement []string

	//generate HUB SQL
	for _, hub := range dv.Hubs {
		hubSQL, hubErr := hub.GenerateSQL()

		if hubErr != nil {
			return nil, fmt.Errorf(
				"Unable to generate insert SQL statement for entity HUB %s:\n%s",
				hub.HubName, hubErr.Error())
		}

		SQLstatement = append(SQLstatement, hubSQL)
	}

	//generate LINK SQL
	for _, link := range dv.Links {
		linkSQL, linkErr := link.GenerateSQL()

		if linkErr != nil {
			return nil, fmt.Errorf("Unable to generate insert SQL statement for entity Link %s:\n%s",
				link.LinkName,
				linkErr.Error())
		}

		SQLstatement = append(SQLstatement, linkSQL)
	}

	//generate Satelite SQL
	for _, sat := range dv.Satelites {
		satSQL, satErr := sat.GenerateSQL()

		if satErr != nil {
			return nil, fmt.Errorf("Unable to generate insert SQL statement for entity Satelite %s:\n%s",
				sat.SateliteName,
				satErr.Error())
		}

		SQLstatement = append(SQLstatement, satSQL)
	}

	return SQLstatement, nil
}

func (dv *DvInsertRecord) checkIntegrity() error {
	//TODO check integrity
	//
	//  Hub, Link, and Satelite must has hash key value
	//  Satelite has valid hash key reference
	//  Link has valid hash key reference
	//  No duplicate hub hash key been used
	//	All Hub, Link, and Satelite name is valid (exists in database)
	return nil
}
