package dvmeta

import (
	"github.com/guinso/datavault/definition"
	"github.com/guinso/rdbmstool"
)

//DataVaultMetaReader interface to read metadata of datavault from database
type DataVaultMetaReader interface {
	GetHubDefinition(hubName string, revision int,
		dbHandler rdbmstool.DbHandlerProxy) (*definition.HubDefinition, error)
	GetLinkDefinition(linkName string, revision int,
		dbHandler rdbmstool.DbHandlerProxy) (*definition.LinkDefinition, error)
	GetSateliteDefinition(satName string, revision int,
		dbHandler rdbmstool.DbHandlerProxy) (*definition.SateliteDefinition, error)

	GetAllHubs(dbHandler rdbmstool.DbHandlerProxy) []EntityInfo
	GetAllLinks(dbHandler rdbmstool.DbHandlerProxy) []EntityInfo
	GetAllSatelites(dbHandler rdbmstool.DbHandlerProxy) []EntityInfo

	SearchEntities(dbHandler rdbmstool.DbHandlerProxy, searchKeyword string) []EntityInfo

	GetRelationship(dbHandler rdbmstool.DbHandlerProxy, hubName string, hubRevision int) (*HubRelationship, error)
}

//EntityInfo basic information of an data vault entity
//name: entity name, example hub name, link name and satelite name
//revision: revision for each discovered entity such as invoice satelite revision
type EntityInfo struct {
	Type     definition.EntityType
	Name     string
	Revision int
}

type HubRelationship struct {
	HubName     string
	HubRevision int
	Satelites   []definition.SateliteDefinition
	Links       []HubLinkRelationship
}

type HubLinkRelationship struct {
	Definition *definition.LinkDefinition
	Hubs       []definition.HubDefinition
	Satelites  []definition.SateliteDefinition
}
