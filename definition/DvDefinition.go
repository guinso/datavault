package definition

//DataVaultDefinition is a set of DataVault definition (blue print) to build data vault's database
type DataVaultDefinition struct {
	Hubs      []HubDefinition
	satelites []SateliteDefinition
	Links     []LinkDefinition
}

//EntityType data vault entity category; e.g. hub, link, and satelite
type EntityType uint8

//List of data vault entity category
const (
	HUB      EntityType = iota + 1
	LINK     EntityType = iota + 1
	SATELITE EntityType = iota + 1
)

func (entity EntityType) String() string {
	if entity == HUB {
		return "hub"
	} else if entity == LINK {
		return "link"
	} else if entity == SATELITE {
		return "satelite"
	}

	return "unknown"
}

//GenerateSQL is to generate multiple SQL statements to create respective DV data tables
func (dvDef *DataVaultDefinition) GenerateSQL() ([]string, error) {
	result := []string{}

	//generate Hubs' SQL
	if len(dvDef.Hubs) > 0 {
		for _, hubDef := range dvDef.Hubs {
			hubSQL, hubErr := hubDef.GenerateSQL()

			if hubErr != nil {
				return nil, hubErr
			}

			result = append(result, hubSQL)
		}
	}

	//generate Satelites' SQL
	if len(dvDef.satelites) > 0 {
		for _, satDef := range dvDef.satelites {
			satSQL, satErr := satDef.GenerateSQL()

			if satErr != nil {
				return nil, satErr
			}

			result = append(result, satSQL)
		}
	}

	//generate Links' SQL
	if len(dvDef.Links) > 0 {
		for _, linkDef := range dvDef.Links {
			linkSQL, linkErr := linkDef.GenerateSQL()

			if linkErr != nil {
				return nil, linkErr
			}

			result = append(result, linkSQL)
		}
	}

	return result, nil
}
