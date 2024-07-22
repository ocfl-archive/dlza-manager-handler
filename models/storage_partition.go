package models

type StoragePartition struct {
	Alias             string
	Name              string
	MaxSize           int
	MaxObjects        int
	CurrentSize       int
	CurrentObjects    int
	Id                string
	StorageLocationId string
}
