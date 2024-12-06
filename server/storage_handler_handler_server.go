package server

import (
	"context"
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager-handler/service"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/ocfl-archive/dlza-manager/mapper"
	"github.com/ocfl-archive/dlza-manager/models"
	"io"
	"log"
)

type StorageHandlerHandlerServer struct {
	pbHandler.UnimplementedStorageHandlerHandlerServiceServer
	UploaderService                    service.UploaderService
	CollectionRepository               repository.CollectionRepository
	ObjectRepository                   repository.ObjectRepository
	ObjectInstanceRepository           repository.ObjectInstanceRepository
	StorageLocationRepository          repository.StorageLocationRepository
	StoragePartitionService            service.StoragePartitionService
	FileRepository                     repository.FileRepository
	StatusRepository                   repository.StatusRepository
	TransactionRepository              repository.TransactionRepository
	RefreshMaterializedViewsRepository repository.RefreshMaterializedViewsRepository
	Logger                             zLogger.ZLogger
}

func (c *StorageHandlerHandlerServer) TenantHasAccess(ctx context.Context, object *pb.UploaderAccessObject) (*pb.Status, error) {
	status, err := c.UploaderService.TenantHasAccess(object)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not execute TenantHasAccess, err: %v", err)
	}
	return &status, nil
}

func (c *StorageHandlerHandlerServer) SaveAllTableObjectsAfterCopyingStream(stream pbHandler.StorageHandlerHandlerService_SaveAllTableObjectsAfterCopyingStreamServer) error {
	var instanceWithPartitionAndObjectWithFiles []*pb.InstanceWithPartitionAndObjectWithFile
	for {
		instanceWithPartitionAndObjectWithFile, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		instanceWithPartitionAndObjectWithFiles = append(instanceWithPartitionAndObjectWithFiles, instanceWithPartitionAndObjectWithFile)
	}
	collectionId, err := c.CollectionRepository.GetCollectionIdByAlias(instanceWithPartitionAndObjectWithFiles[0].CollectionAlias)
	if err != nil {
		c.Logger.Error().Msgf("Could not get collectionId for collection with alias: '%s'", instanceWithPartitionAndObjectWithFiles[0].CollectionAlias, err)
		return errors.Wrapf(err, "Could not get collectionId for collection with alias: '%s'", instanceWithPartitionAndObjectWithFiles[0].CollectionAlias)
	}
	instanceWithPartitionAndObjectWithFiles[0].Object.CollectionId = collectionId
	err = c.TransactionRepository.SaveAllTableObjectsAfterCopying(instanceWithPartitionAndObjectWithFiles)
	if err != nil {
		c.Logger.Error().Msgf("Could not SaveAllTableObjectsAfterCopying for collection with alias: %s and path: %s", instanceWithPartitionAndObjectWithFiles[0].CollectionAlias, err)
		return errors.Wrapf(err, "Could not SaveAllTableObjectsAfterCopying for collection with alias: %s and path: %s", instanceWithPartitionAndObjectWithFiles[0].CollectionAlias,
			instanceWithPartitionAndObjectWithFiles[0].ObjectInstance.Path)
	}
	err = c.RefreshMaterializedViewsRepository.RefreshMaterializedViews()
	if err != nil {
		c.Logger.Error().Msgf("couldn't create refresh mat. views, err: %v", err)
		log.Printf("couldn't create refresh mat. views, err: %v", err)
	}
	return nil
}

func (c *StorageHandlerHandlerServer) GetStorageLocationsByCollectionAlias(ctx context.Context, collectionAlias *pb.CollectionAlias) (*pb.StorageLocations, error) {

	collection, err := c.CollectionRepository.GetCollectionByAlias(collectionAlias.CollectionAlias)
	if err != nil {
		c.Logger.Error().Msgf("Could not get collectionId for collection with alias '%s'", collectionAlias.CollectionAlias, err)
		return nil, errors.Wrapf(err, "Could not get collectionId for collection with alias '%s'", collectionAlias.CollectionAlias)
	}
	storageLocations, err := c.StorageLocationRepository.GetStorageLocationsByTenantId(collection.TenantId)
	if err != nil {
		c.Logger.Error().Msgf("Could not get storageLocations for collection with alias '%s'", collectionAlias.CollectionAlias, err)
		return nil, errors.Wrapf(err, "Could not get storageLocations for collection with alias '%s'", collectionAlias.CollectionAlias)
	}
	storageLocations = service.GetCheapestStorageLocationsForQuality(storageLocations, collection.Quality)
	storageLocationsPb := make([]*pb.StorageLocation, 0)
	for _, storageLocation := range storageLocations {
		storageLocationPb := mapper.ConvertToStorageLocationPb(storageLocation)
		storageLocationsPb = append(storageLocationsPb, storageLocationPb)
	}

	return &pb.StorageLocations{StorageLocations: storageLocationsPb}, nil
}

func (c *StorageHandlerHandlerServer) GetStorageLocationsByObjectId(ctx context.Context, id *pb.Id) (*pb.StorageLocations, error) {
	storageLocations, err := c.StorageLocationRepository.GetStorageLocationsByObjectId(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get current storage locations", err)
		return nil, errors.Wrapf(err, "Could not get current storage locations")
	}
	storageLocationsPb := make([]*pb.StorageLocation, 0)
	for _, storageLocation := range storageLocations {
		storageLocationPb := mapper.ConvertToStorageLocationPb(storageLocation)
		storageLocationsPb = append(storageLocationsPb, storageLocationPb)
	}
	return &pb.StorageLocations{StorageLocations: storageLocationsPb}, nil
}

func (c *StorageHandlerHandlerServer) GetStoragePartitionForLocation(ctx context.Context, sizeAndLocationId *pb.SizeAndId) (*pb.StoragePartition, error) {
	partition, err := c.StoragePartitionService.GetStoragePartitionForLocation(sizeAndLocationId)
	if err != nil {
		c.Logger.Error().Msgf("Could not get storagePartition for storageLocation", err)
		return nil, errors.Wrapf(err, "Could not get storagePartition for storageLocation")
	}
	return partition, nil
}

func (c *StorageHandlerHandlerServer) UpdateStoragePartition(ctx context.Context, storagePartition *pb.StoragePartition) (*pb.Status, error) {
	status, err := c.StoragePartitionService.UpdateStoragePartition(storagePartition)
	if err != nil {
		c.Logger.Error().Msgf("Could not update storagePartition with ID: %v", storagePartition.Id, err)
		return nil, errors.Wrapf(err, "Could not update storagePartition with ID: %v", storagePartition.Id)
	}
	return status, nil
}

func (c *StorageHandlerHandlerServer) GetStorageLocationById(ctx context.Context, id *pb.Id) (*pb.StorageLocation, error) {
	storageLocation, err := c.StorageLocationRepository.GetStorageLocationById(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get storageLocation for location ID", err)
		return nil, errors.Wrapf(err, "Could not get storageLocation for location ID")
	}
	return mapper.ConvertToStorageLocationPb(storageLocation), nil
}

func (c *StorageHandlerHandlerServer) GetAndSaveStoragePartitionWithRelevantAlias(ctx context.Context, storagePartition *pb.StoragePartition) (*pb.StoragePartition, error) {
	storagePartitionWithAlias, err := c.StoragePartitionService.GetAndSaveStoragePartitionWithRelevantAlias(storagePartition)
	if err != nil {
		c.Logger.Error().Msgf("Could not fill storagePartition with alias", err)
		return nil, errors.Wrapf(err, "Could not fill storagePartition with alias")
	}
	return storagePartitionWithAlias, nil
}
func (c *StorageHandlerHandlerServer) GetObjectsByCollectionAlias(ctx context.Context, collectionAlias *pb.CollectionAlias) (*pb.Objects, error) {

	id, err := c.CollectionRepository.GetCollectionIdByAlias(collectionAlias.CollectionAlias)
	if err != nil {
		c.Logger.Error().Msgf("Could not get collectionId for collection with alias '%s'", err)
		return nil, errors.Wrapf(err, "Could not get collectionId for collection with alias '%s'", collectionAlias.CollectionAlias)
	}

	objects, err := c.ObjectRepository.GetObjectsByCollectionId(id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get objects for collection with alias '%s'", err)
		return nil, errors.Wrapf(err, "Could not get objects for collection with alias '%s'", collectionAlias.CollectionAlias)
	}
	objectsPb := make([]*pb.Object, 0)
	for _, object := range objects {
		objectPb := mapper.ConvertToObjectPb(object)
		objectsPb = append(objectsPb, objectPb)
	}
	return &pb.Objects{Objects: objectsPb}, nil
}

func (c *StorageHandlerHandlerServer) GetObjectsInstancesByObjectId(ctx context.Context, id *pb.Id) (*pb.ObjectInstances, error) {
	objectInstances, err := c.ObjectInstanceRepository.GetObjectInstancesByObjectId(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get objectInstances for object ID", err)
		return nil, errors.Wrapf(err, "Could not get objectInstances for object ID")
	}
	objectInstancesPb := make([]*pb.ObjectInstance, 0)
	for _, objectInstance := range objectInstances {
		objectInstancePb := mapper.ConvertToObjectInstancePb(objectInstance)
		objectInstancesPb = append(objectInstancesPb, objectInstancePb)
	}
	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb}, nil
}

func (c *StorageHandlerHandlerServer) CreateObjectInstance(ctx context.Context, objectInstance *pb.ObjectInstance) (*pb.Id, error) {
	id, err := c.ObjectInstanceRepository.CreateObjectInstance(mapper.ConvertToObjectInstance(objectInstance))
	if err != nil {
		c.Logger.Error().Msgf("Could not create objectInstance for object ID: '%s'", err)
		return nil, errors.Wrapf(err, "Could not create objectInstance for object ID: '%s'", objectInstance.ObjectId)
	}
	return &pb.Id{Id: id}, nil
}

func (c *StorageHandlerHandlerServer) GetStoragePartitionsByStorageLocationId(ctx context.Context, locationId *pb.Id) (*pb.StoragePartitions, error) {
	partitions, err := c.StoragePartitionService.GetStoragePartitionsForLocationId(locationId)
	if err != nil {
		c.Logger.Error().Msgf("Could not get storagePartition for storageLocation", err)
		return nil, errors.Wrapf(err, "Could not get storagePartition for storageLocation")
	}
	return partitions, nil
}

func (c *StorageHandlerHandlerServer) DeleteObjectInstance(ctx context.Context, id *pb.Id) (*pb.Status, error) {
	err := c.ObjectInstanceRepository.DeleteObjectInstance(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not delete objectInstance with ID: '%s'", id.Id, err)
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not delete objectInstance with ID: '%s'", id.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *StorageHandlerHandlerServer) AlterStatus(ctx context.Context, statusPb *pb.StatusObject) (*pb.Status, error) {
	status := models.ArchivingStatus{Status: statusPb.Status, LastChanged: statusPb.LastChanged, Id: statusPb.Id}
	err := c.StatusRepository.AlterStatus(status)
	if err != nil {
		c.Logger.Error().Msgf("Could not AlterStatus with id: '%s'", statusPb.Id, err)
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not AlterStatus with id: '%s'", statusPb.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *StorageHandlerHandlerServer) GetObjectById(ctx context.Context, id *pb.Id) (*pb.Object, error) {
	object, err := c.ObjectRepository.GetObjectById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetObjectById with id: '%s'", id.Id)
	}
	objectPb := mapper.ConvertToObjectPb(object)
	return objectPb, nil
}
