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
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewDispatcherHandlerServer(dispatcherRepository repository.DispatcherRepository, tenantService service.TenantService,
	objectInstanceRepository repository.ObjectInstanceRepository, objectRepository repository.ObjectRepository, collectionRepository repository.CollectionRepository,
	storageLocationRepository repository.StorageLocationRepository, objectInstanceCheckRepository repository.ObjectInstanceCheckRepository, logger zLogger.ZLogger) *DispatcherHandlerServer {
	return &DispatcherHandlerServer{DispatcherRepository: dispatcherRepository, TenantService: tenantService,
		ObjectInstanceRepository: objectInstanceRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository, ObjectRepository: objectRepository, StorageLocationRepository: storageLocationRepository,
		CollectionRepository: collectionRepository, Logger: logger}
}

type DispatcherHandlerServer struct {
	pbHandler.UnimplementedDispatcherHandlerServiceServer
	TenantService                 service.TenantService
	ObjectRepository              repository.ObjectRepository
	CollectionRepository          repository.CollectionRepository
	ObjectInstanceCheckRepository repository.ObjectInstanceCheckRepository
	StorageLocationRepository     repository.StorageLocationRepository
	ObjectInstanceRepository      repository.ObjectInstanceRepository
	DispatcherRepository          repository.DispatcherRepository
	Logger                        zLogger.ZLogger
}

func (d *DispatcherHandlerServer) GetLowQualityCollectionsWithObjectIds(ctx context.Context, param *pb.NoParam) (*pb.CollectionAliases, error) {
	collectionsWithObjectIds, err := d.DispatcherRepository.GetLowQualityCollectionsWithObjectIds()
	if err != nil {
		d.Logger.Error().Msgf("Could not get LowQualityCollections", err)
		return nil, status.Errorf(codes.Internal, "Could not get LowQualityCollections: %v", err)
	}

	collectionAliasesPb := make([]*pb.CollectionAlias, 0)
	for _, collectionAlias := range maps.Keys(collectionsWithObjectIds) {
		idsPb := make([]*pb.Id, 0)
		for _, id := range collectionsWithObjectIds[collectionAlias] {
			idsPb = append(idsPb, &pb.Id{Id: id})
		}
		collectionAliasPb := pb.CollectionAlias{CollectionAlias: collectionAlias, Ids: idsPb}
		collectionAliasesPb = append(collectionAliasesPb, &collectionAliasPb)
	}

	return &pb.CollectionAliases{CollectionAliases: collectionAliasesPb}, nil
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
	object, err := d.ObjectRepository.GetObjectExceptListOlderThan(idsWithInterval.CollectionId, idsWithInterval.Ids, idsWithInterval.Interval)
	if err != nil {
		d.Logger.Error().Msgf("Could not GetObjectExceptListOlderThan for ids: %v", idsWithInterval.Ids, err)
		return nil, errors.Wrapf(err, "Could not GetObjectExceptListOlderThan ids: %v", idsWithInterval.Ids)
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
