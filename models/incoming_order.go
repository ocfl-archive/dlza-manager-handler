package models

type ObjectPath struct {
	FilePath     string `json:"filePath"`
	InfoFilePath string `json:"infoFilePath"`
}

type IncomingOrder struct {
	CollectionAlias string       `json:"collectionAlias"`
	ObjectPaths     []ObjectPath `json:"objectPaths"`
}
