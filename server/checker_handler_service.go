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
)

type CheckerHandlerServer struct {
	pbHandler.UnimplementedCheckerHandlerServiceServer
	ObjectInstanceRepository      repository.ObjectInstanceRepository
	ObjectRepository              repository.ObjectRepository
	CollectionRepository          repository.CollectionRepository
	ObjectInstanceCheckRepository repository.ObjectInstanceCheckRepository
	StorageLocationRepository     repository.StorageLocationRepository
	Logger                        zLogger.ZLogger
}

func (c *CheckerHandlerServer) GetAllObjectInstances(ctx context.Context, noParam *pb.NoParam) (*pb.ObjectInstances, error) {
	objectInstances, err := c.ObjectInstanceRepository.GetAllObjectInstances()
	if err != nil {
		c.Logger.Error().Msgf("Could not get all object instances", err)
		return nil, errors.Wrapf(err, "Could not get all object instances")
	}
	var objectInstancesPb []*pb.ObjectInstance

	for _, objectInstance := range objectInstances {
		objectInstancesPb = append(objectInstancesPb, mapper.ConvertToObjectInstancePb(objectInstance))
	}

	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb}, nil
}

func (c *CheckerHandlerServer) GetObjectInstanceChecksByObjectInstanceId(ctx context.Context, id *pb.Id) (*pb.ObjectInstanceChecks, error) {
	objectInstanceChecks, err := c.ObjectInstanceCheckRepository.GetObjectInstanceChecksByObjectInstanceId(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get objectInstanceChecks for object instance ID", err)
		return nil, errors.Wrapf(err, "Could not get objectInstances for object instance ID")
	}
	objectInstanceChecksPb := make([]*pb.ObjectInstanceCheck, 0)
	for _, objectInstanceCheck := range objectInstanceChecks {
		objectInstanceCheckPb := mapper.ConvertToObjectInstanceCheckPb(objectInstanceCheck)
		objectInstanceChecksPb = append(objectInstanceChecksPb, objectInstanceCheckPb)
	}
	return &pb.ObjectInstanceChecks{ObjectInstanceChecks: objectInstanceChecksPb}, nil
}

func (c *CheckerHandlerServer) UpdateObjectInstance(ctx context.Context, objectInstancePb *pb.ObjectInstance) (*pb.NoParam, error) {
	err := c.ObjectInstanceRepository.UpdateObjectInstance(mapper.ConvertToObjectInstance(objectInstancePb))
	if err != nil {
		c.Logger.Error().Msgf("Could not get all object instances", err)
		return nil, errors.Wrapf(err, "Could not get all object instances")
	}
	return nil, nil
}

func (c *CheckerHandlerServer) CreateObjectInstanceCheck(ctx context.Context, objectInstanceCheckPb *pb.ObjectInstanceCheck) (*pb.NoParam, error) {
	_, err := c.ObjectInstanceCheckRepository.CreateObjectInstanceCheck(mapper.ConvertToObjectInstanceCheck(objectInstanceCheckPb))
	if err != nil {
		c.Logger.Error().Msgf("Could not create object instance check", err)
		return &pb.NoParam{}, errors.Wrapf(err, "Could not create object instance check")
	}
	return &pb.NoParam{}, nil
}

func (c *CheckerHandlerServer) GetObjectById(ctx context.Context, id *pb.Id) (*pb.Object, error) {
	object, err := c.ObjectRepository.GetObjectById(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get object by id: %v", id.Id, err)
		return nil, errors.Wrapf(err, "Could not get object by id: %v", id.Id)
	}
	return mapper.ConvertToObjectPb(object), nil
}

func (c *CheckerHandlerServer) GetObjectsInstancesByObjectId(ctx context.Context, id *pb.Id) (*pb.ObjectInstances, error) {
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

func (c *CheckerHandlerServer) GetStorageLocationByObjectInstanceId(ctx context.Context, id *pb.Id) (*pb.StorageLocation, error) {
	storageLocation, err := c.StorageLocationRepository.GetStorageLocationByObjectInstanceId(id.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not get storage location by object instance id: %v", id.Id, err)
		return nil, errors.Wrapf(err, "Could not get storage location by object instance id: %v", id.Id)
	}
	return mapper.ConvertToStorageLocationPb(storageLocation), nil
}

func (c *CheckerHandlerServer) GetRelevantStorageLocationsByObjectId(ctx context.Context, objectId *pb.Id) (*pb.StorageLocations, error) {
	object, err := c.ObjectRepository.GetObjectById(objectId.Id)
	if err != nil {
		c.Logger.Error().Msgf("Could not GetObjectById for object with ID: %v", objectId.Id, err)
		return nil, errors.Wrapf(err, "Could not GetObjectById for object with ID: %v", objectId.Id)
	}
	collection, err := c.CollectionRepository.GetCollectionById(object.CollectionId)
	if err != nil {
		c.Logger.Error().Msgf("Could not GetCollectionById for collection with ID: %v", object.CollectionId, err)
		return nil, errors.Wrapf(err, "Could not GetCollectionById for collection with ID: %v", object.CollectionId)
	}
	storageLocations, err := c.StorageLocationRepository.GetStorageLocationsByTenantId(collection.TenantId)
	if err != nil {
		c.Logger.Error().Msgf("Could not get storageLocations for collection with alias '%v'", collection.Alias, err)
		return nil, errors.Wrapf(err, "Could not get storageLocations for collection with alias '%v'", collection.Alias)
	}

	storageLocations = service.GetCheapestStorageLocationsForQuality(storageLocations, collection.Quality)
	storageLocationsPb := make([]*pb.StorageLocation, 0)
	for _, storageLocation := range storageLocations {
		storageLocationPb := mapper.ConvertToStorageLocationPb(storageLocation)
		storageLocationsPb = append(storageLocationsPb, storageLocationPb)
	}
	return &pb.StorageLocations{StorageLocations: storageLocationsPb}, nil
}
