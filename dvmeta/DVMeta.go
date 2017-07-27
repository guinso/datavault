package dvmeta

import "github.com/guinso/datavault/definition"

//DataVaultMetaReader interface to read metadata of datavault from database
type DataVaultMetaReader interface {
	GetHubDefinition(hubName string, revision int) (*definition.HubDefinition, error)
	GetLinkDefinition(linkName string, revision int) (*definition.LinkDefinition, error)
	GetSateliteDefinition(satName string, revision int) (*definition.SateliteDefinition, error)

	GetAllHubs() []EntityInfo
	GetAllLinks() []EntityInfo
	GetAllSatelites() []EntityInfo
}

type EntityInfo struct {
	Name     string
	Revision int
}
