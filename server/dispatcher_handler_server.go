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

func NewDispatcherHandlerServer(storagePartitionService service.StoragePartitionService, dispatcherRepository repository.DispatcherRepository, tenantService service.TenantService,
	objectInstanceRepository repository.ObjectInstanceRepository, objectRepository repository.ObjectRepository, collectionRepository repository.CollectionRepository,
	storageLocationRepository repository.StorageLocationRepository, objectInstanceCheckRepository repository.ObjectInstanceCheckRepository, logger zLogger.ZLogger) *DispatcherHandlerServer {
	return &DispatcherHandlerServer{DispatcherRepository: dispatcherRepository, TenantService: tenantService,
		ObjectInstanceRepository: objectInstanceRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository, ObjectRepository: objectRepository, StorageLocationRepository: storageLocationRepository,
		CollectionRepository: collectionRepository, StoragePartitionService: storagePartitionService, Logger: logger}
}

type DispatcherHandlerServer struct {
	pbHandler.UnimplementedDispatcherHandlerServiceServer
	TenantService                 service.TenantService
	ObjectRepository              repository.ObjectRepository
	CollectionRepository          repository.CollectionRepository
	ObjectInstanceCheckRepository repository.ObjectInstanceCheckRepository
	StoragePartitionService       service.StoragePartitionService
	StorageLocationRepository     repository.StorageLocationRepository
	ObjectInstanceRepository      repository.ObjectInstanceRepository
	DispatcherRepository          repository.DispatcherRepository
	Logger                        zLogger.ZLogger
}

func (d *DispatcherHandlerServer) FindAllTenants(ctx context.Context, status *pb.NoParam) (*pb.Tenants, error) {
	tenants, err := d.TenantService.FindAllTenants()
	if err != nil {
		d.Logger.Error().Msgf("Could not get all tenants")
		return nil, errors.Wrapf(err, "Could not get all tenants")
	}
	var tenantsPb []*pb.Tenant

	for _, tenant := range tenants {
		tenantsPb = append(tenantsPb, mapper.ConvertToTenantPb(tenant))
	}

	return &pb.Tenants{Tenants: tenantsPb}, nil
}

func (d *DispatcherHandlerServer) UpdateObjectInstance(ctx context.Context, objectInstancePb *pb.ObjectInstance) (*pb.NoParam, error) {
	err := d.ObjectInstanceRepository.UpdateObjectInstance(mapper.ConvertToObjectInstance(objectInstancePb))
	if err != nil {
		d.Logger.Error().Msgf("Could not get all object instances", err)
		return nil, errors.Wrapf(err, "Could not get all object instances")
	}
	return nil, nil
}

func (d *DispatcherHandlerServer) UpdateStoragePartition(ctx context.Context, storagePartition *pb.StoragePartition) (*pb.Status, error) {
	status, err := d.StoragePartitionService.UpdateStoragePartition(storagePartition)
	if err != nil {
		d.Logger.Error().Msgf("Could not update storagePartition with ID: %v", storagePartition.Id, err)
		return nil, errors.Wrapf(err, "Could not update storagePartition with ID: %v", storagePartition.Id)
	}
	return status, nil
}

func (d *DispatcherHandlerServer) GetObjectsInstancesByObjectId(ctx context.Context, id *pb.Id) (*pb.ObjectInstances, error) {
	objectInstances, err := d.ObjectInstanceRepository.GetObjectInstancesByObjectId(id.Id)
	if err != nil {
		d.Logger.Error().Msgf("Could not get objectInstances for object ID", err)
		return nil, errors.Wrapf(err, "Could not get objectInstances for object ID")
	}
	objectInstancesPb := make([]*pb.ObjectInstance, 0)
	for _, objectInstance := range objectInstances {
		objectInstancePb := mapper.ConvertToObjectInstancePb(objectInstance)
		objectInstancesPb = append(objectInstancesPb, objectInstancePb)
	}
	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb}, nil
}

func (d *DispatcherHandlerServer) GetStoragePartitionForLocation(ctx context.Context, sizeAndLocationId *pb.SizeAndId) (*pb.StoragePartition, error) {
	partition, err := d.StoragePartitionService.GetStoragePartitionForLocation(sizeAndLocationId)
	if err != nil {
		d.Logger.Error().Msgf("Could not get storagePartition for storageLocation", err)
		return nil, errors.Wrapf(err, "Could not get storagePartition for storageLocation")
	}
	return partition, nil
}

func (d *DispatcherHandlerServer) GetStorageLocationsByTenantId(ctx context.Context, tenantId *pb.Id) (*pb.StorageLocations, error) {
	storageLocations, err := d.StorageLocationRepository.GetStorageLocationsByTenantId(tenantId.Id)
	if err != nil {
		d.Logger.Error().Msgf("Could not get storageLocations by tenant with id: '%v'", tenantId.Id, err)
		return nil, errors.Wrapf(err, "Could not get storageLocations by tenant with id: '%v'", tenantId.Id)
	}
	var storageLocationsPb []*pb.StorageLocation

	for _, storageLocation := range storageLocations {
		storageLocationsPb = append(storageLocationsPb, mapper.ConvertToStorageLocationPb(storageLocation))
	}

	return &pb.StorageLocations{StorageLocations: storageLocationsPb}, nil
}

func (d *DispatcherHandlerServer) GetObjectExceptListOlderThan(ctx context.Context, idsWithInterval *pb.IdsWithSQLInterval) (*pb.Object, error) {
	object, err := d.ObjectRepository.GetObjectExceptListOlderThan(idsWithInterval.CollectionId, idsWithInterval.Ids, idsWithInterval.CollectionsIds)
	if err != nil {
		d.Logger.Error().Msgf("Could not GetObjectExceptListOlderThan for collection: %s", idsWithInterval.CollectionId, err)
		return nil, errors.Wrapf(err, "Could not GetObjectExceptListOlderThan for collection: %s", idsWithInterval.CollectionId)
	}
	return mapper.ConvertToObjectPb(object), nil
}

func (d *DispatcherHandlerServer) GetStorageLocationByObjectInstanceId(ctx context.Context, id *pb.Id) (*pb.StorageLocation, error) {
	storageLocation, err := d.StorageLocationRepository.GetStorageLocationByObjectInstanceId(id.Id)
	if err != nil {
		d.Logger.Error().Msgf("Could not get storage location by object instance id: %v", id.Id, err)
		return nil, errors.Wrapf(err, "Could not get storage location by object instance id: %v", id.Id)
	}
	return mapper.ConvertToStorageLocationPb(storageLocation), nil
}

func (d *DispatcherHandlerServer) GetExistingStorageLocationsCombinationsForCollectionId(ctx context.Context, id *pb.Id) (*pb.StorageLocationsCombinationsForCollections, error) {
	collections, err := d.CollectionRepository.GetExistingStorageLocationsCombinationsForCollectionId(id.Id)
	if err != nil {
		d.Logger.Error().Msgf("Could not GetExistingStorageLocationsCombinationsForCollectionId for collection with ID: '%s'", id.Id, err)
		return nil, errors.Wrapf(err, "Could not GetExistingStorageLocationsCombinationsForCollectionId for collection with ID: '%s'", id.Id)
	}
	var collectionsPb []*pb.StorageLocationsCombinationsForCollection

	for _, collection := range collections {
		collectionsPb = append(collectionsPb, mapper.ConvertStorageLocationsCombinationsForCollection(collection))
	}
	return &pb.StorageLocationsCombinationsForCollections{StorageLocationsCombinationsForCollections: collectionsPb}, nil
}

func (d *DispatcherHandlerServer) GetCollectionsByTenantId(ctx context.Context, id *pb.Id) (*pb.Collections, error) {
	collections, err := d.CollectionRepository.GetCollectionsByTenantId(id.Id)
	if err != nil {
		d.Logger.Error().Msgf("Could not get collections by tenant with id: '%v'", id.Id, err)
		return nil, errors.Wrapf(err, "Could not get collections by tenant with id: '%v'", id.Id)
	}
	var collectionsPb []*pb.Collection

	for _, collection := range collections {
		collectionsPb = append(collectionsPb, mapper.ConvertToCollectionPb(collection))
	}

	return &pb.Collections{Collections: collectionsPb}, nil
}

func (d *DispatcherHandlerServer) CreateObjectInstanceCheck(ctx context.Context, objectInstanceCheckPb *pb.ObjectInstanceCheck) (*pb.NoParam, error) {
	_, err := d.ObjectInstanceCheckRepository.CreateObjectInstanceCheck(mapper.ConvertToObjectInstanceCheck(objectInstanceCheckPb))
	if err != nil {
		d.Logger.Error().Msgf("Could not create object instance check", err)
		return &pb.NoParam{}, errors.Wrapf(err, "Could not create object instance check")
	}
	return &pb.NoParam{}, nil
}

func (d *DispatcherHandlerServer) GetObjectInstanceChecksByObjectInstanceId(ctx context.Context, id *pb.Id) (*pb.ObjectInstanceChecks, error) {
	objectInstanceChecks, err := d.ObjectInstanceCheckRepository.GetObjectInstanceChecksByObjectInstanceId(id.Id)
	if err != nil {
		d.Logger.Error().Msgf("Could not get objectInstanceChecks for object instance ID", err)
		return nil, errors.Wrapf(err, "Could not get objectInstances for object instance ID")
	}
	objectInstanceChecksPb := make([]*pb.ObjectInstanceCheck, 0)
	for _, objectInstanceCheck := range objectInstanceChecks {
		objectInstanceCheckPb := mapper.ConvertToObjectInstanceCheckPb(objectInstanceCheck)
		objectInstanceChecksPb = append(objectInstanceChecksPb, objectInstanceCheckPb)
	}
	return &pb.ObjectInstanceChecks{ObjectInstanceChecks: objectInstanceChecksPb}, nil
}
