package record

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/guinso/datavault/definition"
	"github.com/guinso/rdbmstool"
	"github.com/guinso/stringtool"
)

//SateliteInsertRecord is satelite insert record schema
type SateliteInsertRecord struct {
	SateliteName    string
	Revision        int
	RecordSource    string
	HubName         string
	HubHashKeyValue string
	LoadDate        time.Time
	Attributes      []SateliteAttrInsertRecord
}

//SateliteAttrInsertRecord is satelite attribute insert record schema
type SateliteAttrInsertRecord struct {
	AttributeName string
	Value         interface{} //any basic type of value: int, string, double, float
	Meta          *definition.SateliteAttributeDefinition
}

func (satInsert *SateliteInsertRecord) getDbTableName() string {
	return fmt.Sprintf("sat_%s_rev%d",
		stringtool.ToSnakeCase(satInsert.SateliteName), satInsert.Revision)
}

func (satInsert *SateliteInsertRecord) getHubColumnName() string {
	return fmt.Sprintf("%s_hash_key",
		stringtool.ToSnakeCase(satInsert.HubName))
}

//GenerateSQL to generate executable SQL statement to insert new satelite record row
func (satInsert *SateliteInsertRecord) GenerateSQL() (string, error) {
	var columns string
	var values string

	if satInsert.Attributes == nil || len(satInsert.Attributes) == 0 {
		return "", errors.New(
			"unable to generate SQL to insert new satelite record as there is no attribute found")
	}

	for index, attrValue := range satInsert.Attributes {
		tmpStr, tmpErr := attrValue.convertValueToString()

		if tmpErr != nil {
			return "", fmt.Errorf(
				"SateliteInsertRecord Fail to generate SQL: \n%s", tmpErr.Error())
		}

		if index == 0 {
			columns = "`" + stringtool.ToSnakeCase(attrValue.AttributeName) + "`"
			values = tmpStr
		} else {
			columns = columns + ",`" + stringtool.ToSnakeCase(attrValue.AttributeName) + "`"
			values = values + "," + tmpStr
		}
	}

	sql := fmt.Sprintf("INSERT INTO `%s` \n(`%s`, `%s`, `%s`, %s) \nVALUES \n(%s, %s, %s, %s)",
		satInsert.getDbTableName(),
		satInsert.getHubColumnName(),
		definition.LOAD_DATE,
		definition.RECORD_SOURCE,
		columns,
		"'"+satInsert.HubHashKeyValue+"'",
		"'"+satInsert.LoadDate.Format("2006-01-02")+"'",
		"'"+satInsert.RecordSource+"'",
		values)

	return sql, nil
}

func (attrValue *SateliteAttrInsertRecord) convertValueToString() (string, error) {
	if attrValue.Value == nil {
		return "", errors.New("value cannot be null")
	}

	metaType := reflect.TypeOf(attrValue.Value)
	dataType := attrValue.Meta.DataType

	if dataType == rdbmstool.BOOLEAN && metaType.Kind() == reflect.Bool {
		tmpBool, _ := attrValue.Value.(bool)
		if tmpBool {
			return "true", nil
		}
		return "false", nil

	} else if dataType == rdbmstool.DATE && metaType == reflect.TypeOf(time.Time{}) {
		tmpTime, _ := attrValue.Value.(time.Time)
		return fmt.Sprintf("'%s'", tmpTime.Format("2006-01-02")), nil

	} else if dataType == rdbmstool.DATETIME && metaType == reflect.TypeOf(time.Time{}) {
		tmpTime, _ := attrValue.Value.(time.Time)
		return fmt.Sprintf("'%s'", tmpTime.Format("2006-01-02 15:04:05")), nil

	} else if dataType == rdbmstool.DECIMAL &&
		(metaType.Kind() == reflect.Float32 || metaType.Kind() == reflect.Float64) {
		tmpFloat, _ := attrValue.Value.(float64)
		return fmt.Sprintf("%."+strconv.Itoa(attrValue.Meta.DecimalPrecision)+"f", tmpFloat), nil

	} else if dataType == rdbmstool.FLOAT && metaType.Kind() == reflect.Float32 {
		tmpFloat, _ := attrValue.Value.(float32)
		return fmt.Sprintf("%f", tmpFloat), nil

	} else if dataType == rdbmstool.INTEGER && metaType.Kind() == reflect.Int {
		tmpInt, _ := attrValue.Value.(int)
		return fmt.Sprintf("%d", tmpInt), nil

	} else if dataType == rdbmstool.TEXT && metaType.Kind() == reflect.String {
		tmpStr, _ := attrValue.Value.(string)
		return fmt.Sprintf("'%s'", tmpStr), nil

	} else if dataType == rdbmstool.CHAR && metaType.Kind() == reflect.String {
		tmpStr, _ := attrValue.Value.(string)
		return fmt.Sprintf("'%s'", tmpStr), nil

	} else {
		return "", errors.New("value type not match: value type is: " + metaType.Name())
	}
}
