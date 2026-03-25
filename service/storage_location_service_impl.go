package service

import (
	"emperror.dev/errors"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	dlzaMapper "github.com/ocfl-archive/dlza-manager/mapper"
	dlzaService "github.com/ocfl-archive/dlza-manager/service"
)

func NewStorageLocationService(collectionRepository repository.CollectionRepository, storageLocationRepository repository.StorageLocationRepository,
	StoragePartitionService StoragePartitionService) StorageLocationService {
	return &StorageLocationServiceImpl{CollectionRepository: collectionRepository,
		StorageLocationRepository: storageLocationRepository,
		StoragePartitionService:   StoragePartitionService}
}

type StorageLocationServiceImpl struct {
	CollectionRepository      repository.CollectionRepository
	StorageLocationRepository repository.StorageLocationRepository
	StoragePartitionService   StoragePartitionService
}

func (s StorageLocationServiceImpl) GetStorageLocationsStatusForCollectionAlias(alias string, size int64, signature string, head string) (string, error) {

	collection, err := s.CollectionRepository.GetCollectionByAlias(alias)
	if err != nil {
		return "", errors.Wrapf(err, "Could not get collectionId for collection with alias '%s'", alias)
	}
	storageLocations, err := s.StorageLocationRepository.GetStorageLocationsByTenantId(collection.TenantId)
	if err != nil {
		return "", errors.Wrapf(err, "Could not get storageLocations for collection with alias '%s'", alias)
	}
	storageLocationsPb := &pb.StorageLocations{}
	for _, storageLocation := range storageLocations {
		storageLocationsPb.StorageLocations = append(storageLocationsPb.StorageLocations, dlzaMapper.ConvertToStorageLocationPb(storageLocation))
	}
	storageLocationsPb.StorageLocations = dlzaService.GetCheapestStorageLocationsForQuality(storageLocationsPb, collection.Quality)
	objectPb := &pb.Object{Signature: signature, Head: head}
	for _, storageLocation := range storageLocationsPb.StorageLocations {
		_, err := s.StoragePartitionService.GetStoragePartitionForLocation(&pb.SizeObjectLocation{Size: size, Location: storageLocation, Object: objectPb})
		if err != nil {
			return "Could not get storagePartition for storageLocation " + storageLocation.Alias, errors.Wrapf(err, "Could not get storagePartition for storageLocation '%s'", storageLocation.Alias)
		}
	}
	return "", nil
}
