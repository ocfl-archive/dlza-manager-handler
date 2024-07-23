package server

import (
	"context"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/mapper"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager-handler/service"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/pkg/errors"
)

type ClerkHandlerServer struct {
	pbHandler.UnimplementedClerkHandlerServiceServer
	TenantService                 service.TenantService
	CollectionRepository          repository.CollectionRepository
	StorageLocationRepository     repository.StorageLocationRepository
	StoragePartitionRepository    repository.StoragePartitionRepository
	ObjectRepository              repository.ObjectRepository
	ObjectInstanceRepository      repository.ObjectInstanceRepository
	FileRepository                repository.FileRepository
	ObjectInstanceCheckRepository repository.ObjectInstanceCheckRepository
	StatusRepository              repository.StatusRepository
	ObjectInstanceService         service.ObjectInstanceService
}

func (c *ClerkHandlerServer) FindTenantById(ctx context.Context, id *pb.Id) (*pb.Tenant, error) {
	tenant, err := c.TenantService.FindTenantById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get tenant with id: '%s'", id.Id)
	}
	tenantPb := mapper.ConvertToTenantPb(tenant)
	return tenantPb, nil
}

func (c *ClerkHandlerServer) GetCollectionById(ctx context.Context, id *pb.Id) (*pb.Collection, error) {
	collection, err := c.CollectionRepository.GetCollectionById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get collection with id: '%s'", id.Id)
	}
	collectionPb := mapper.ConvertToCollectionPb(collection)
	return collectionPb, nil
}

func (c *ClerkHandlerServer) DeleteTenant(ctx context.Context, id *pb.Id) (*pb.Status, error) {
	err := c.TenantService.DeleteTenant(id.Id)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not delete tenant with id: '%s'", id.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) SaveTenant(ctx context.Context, tenantPb *pb.Tenant) (*pb.Status, error) {
	err := c.TenantService.SaveTenant(mapper.ConvertToTenant(tenantPb))
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not save tenant '%s'", tenantPb.Name)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) UpdateTenant(ctx context.Context, tenantPb *pb.Tenant) (*pb.Status, error) {
	err := c.TenantService.UpdateTenant(mapper.ConvertToTenant(tenantPb))
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not save tenant '%s'", tenantPb.Name)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) FindAllTenants(ctx context.Context, status *pb.NoParam) (*pb.Tenants, error) {
	tenants, err := c.TenantService.FindAllTenants()
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get all tenants")
	}
	var tenantsPb []*pb.Tenant

	for _, tenant := range tenants {
		tenantsPb = append(tenantsPb, mapper.ConvertToTenantPb(tenant))
	}

	return &pb.Tenants{Tenants: tenantsPb}, nil
}

func (c *ClerkHandlerServer) CreateCollection(ctx context.Context, collectionPb *pb.Collection) (*pb.Status, error) {
	_, err := c.CollectionRepository.CreateCollection(mapper.ConvertToCollection(collectionPb))
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not create collection '%s'", collectionPb.Name)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) UpdateCollection(ctx context.Context, collectionPb *pb.Collection) (*pb.Status, error) {
	err := c.CollectionRepository.UpdateCollection(mapper.ConvertToCollection(collectionPb))
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not update collection '%s'", collectionPb.Name)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) DeleteCollectionById(ctx context.Context, id *pb.Id) (*pb.Status, error) {
	err := c.CollectionRepository.DeleteCollectionById(id.Id)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not delete collectiom with id: '%s'", id.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) GetCollectionsByTenantId(ctx context.Context, id *pb.Id) (*pb.Collections, error) {
	collections, err := c.CollectionRepository.GetCollectionsByTenantId(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get collections by tenant with id: '%s'", id.Id)
	}
	var collectionsPb []*pb.Collection

	for _, collection := range collections {
		collectionsPb = append(collectionsPb, mapper.ConvertToCollectionPb(collection))
	}

	return &pb.Collections{Collections: collectionsPb}, nil
}

func (c *ClerkHandlerServer) SaveStorageLocation(ctx context.Context, storageLocationPb *pb.StorageLocation) (*pb.Status, error) {
	err := c.StorageLocationRepository.SaveStorageLocation(mapper.ConvertToStorageLocation(storageLocationPb))
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not create storageLocation '%s'", storageLocationPb.Alias)
	}
	return &pb.Status{Ok: true}, nil
}
func (c *ClerkHandlerServer) DeleteStorageLocationById(ctx context.Context, id *pb.Id) (*pb.Status, error) {
	err := c.StorageLocationRepository.DeleteStorageLocationById(id.Id)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not delete storageLocation with id: '%s'", id.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) GetStorageLocationsByTenantId(ctx context.Context, tenantId *pb.Id) (*pb.StorageLocations, error) {
	storageLocations, err := c.StorageLocationRepository.GetStorageLocationsByTenantId(tenantId.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get storageLocations by tenant with id: '%s'", tenantId.Id)
	}
	var storageLocationsPb []*pb.StorageLocation

	for _, storageLocation := range storageLocations {
		storageLocationsPb = append(storageLocationsPb, mapper.ConvertToStorageLocationPb(storageLocation))
	}

	return &pb.StorageLocations{StorageLocations: storageLocationsPb}, nil
}

func (c *ClerkHandlerServer) GetObjectById(ctx context.Context, id *pb.Id) (*pb.Object, error) {
	object, err := c.ObjectRepository.GetObjectById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetObjectById with id: '%s'", id.Id)
	}
	objectPb := mapper.ConvertToObjectPb(object)
	return objectPb, nil
}

func (c *ClerkHandlerServer) GetObjectInstanceById(ctx context.Context, id *pb.Id) (*pb.ObjectInstance, error) {
	objectInstance, err := c.ObjectInstanceRepository.GetObjectInstanceById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetObjectInstanceById with id: '%s'", id.Id)
	}
	objectInstancePb := mapper.ConvertToObjectInstancePb(objectInstance)
	return objectInstancePb, nil
}

func (c *ClerkHandlerServer) GetFileById(ctx context.Context, id *pb.Id) (*pb.File, error) {
	file, err := c.FileRepository.GetFileById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetFileById with id: '%s'", id.Id)
	}
	filePb := mapper.ConvertToFilePb(file)
	return filePb, nil
}

func (c *ClerkHandlerServer) GetObjectInstanceCheckById(ctx context.Context, id *pb.Id) (*pb.ObjectInstanceCheck, error) {
	objectInstanceCheck, err := c.ObjectInstanceCheckRepository.GetObjectInstanceCheckById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetObjectInstanceCheckById with id: '%s'", id.Id)
	}
	objectInstanceCheckPb := mapper.ConvertToObjectInstanceCheckPb(objectInstanceCheck)
	return objectInstanceCheckPb, nil
}

func (c *ClerkHandlerServer) GetStorageLocationById(ctx context.Context, id *pb.Id) (*pb.StorageLocation, error) {
	storageLocation, err := c.StorageLocationRepository.GetStorageLocationById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetStorageLocationById with id: '%s'", id.Id)
	}
	storageLocationPb := mapper.ConvertToStorageLocationPb(storageLocation)
	return storageLocationPb, nil
}

func (c *ClerkHandlerServer) GetStoragePartitionById(ctx context.Context, id *pb.Id) (*pb.StoragePartition, error) {
	storagePartition, err := c.StoragePartitionRepository.GetStoragePartitionById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetStoragePartitionById with id: '%s'", id.Id)
	}
	storagePartitionPb := mapper.ConvertToStoragePartitionPb(storagePartition)
	return storagePartitionPb, nil
}

/////Paginated methods

func (c *ClerkHandlerServer) FindAllTenantsPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.Tenants, error) {
	tenants, totalItems, err := c.TenantService.FindAllTenantsPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get all tenants")
	}
	var tenantsPb []*pb.Tenant

	for _, tenant := range tenants {
		tenantsPb = append(tenantsPb, mapper.ConvertToTenantPb(tenant))
	}

	return &pb.Tenants{Tenants: tenantsPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetCollectionsByTenantIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.Collections, error) {
	collections, totalItems, err := c.CollectionRepository.GetCollectionsByTenantIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get collections by tenant with id: '%s'", pagination.Id)
	}
	var collectionsPb []*pb.Collection

	for _, collection := range collections {
		collectionsPb = append(collectionsPb, mapper.ConvertToCollectionPb(collection))
	}

	return &pb.Collections{Collections: collectionsPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetStorageLocationsByTenantIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.StorageLocations, error) {
	storageLocations, totalItems, err := c.StorageLocationRepository.GetStorageLocationsByTenantIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get storageLocations by tenant with id: '%s'", pagination.Id)
	}
	var storageLocationsPb []*pb.StorageLocation

	for _, storageLocation := range storageLocations {
		storageLocationsPb = append(storageLocationsPb, mapper.ConvertToStorageLocationPb(storageLocation))
	}

	return &pb.StorageLocations{StorageLocations: storageLocationsPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetStoragePartitionsByLocationIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.StoragePartitions, error) {
	storagePartitions, totalItems, err := c.StoragePartitionRepository.GetStoragePartitionsByLocationIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get GetStoragePartitionsByLocationIdPaginated by storage location with id: '%s'", pagination.Id)
	}
	var storagePartitionsPb []*pb.StoragePartition

	for _, storagePartition := range storagePartitions {
		storagePartitionsPb = append(storagePartitionsPb, mapper.ConvertToStoragePartitionPb(storagePartition))
	}

	return &pb.StoragePartitions{StoragePartitions: storagePartitionsPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetObjectsByCollectionIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.Objects, error) {
	objects, totalItems, err := c.ObjectRepository.GetObjectsByCollectionIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated objects by collection with id: '%s'", pagination.Id)
	}
	var objectsPb []*pb.Object

	for _, object := range objects {
		objectsPb = append(objectsPb, mapper.ConvertToObjectPb(object))
	}
	return &pb.Objects{Objects: objectsPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetFilesByCollectionIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.Files, error) {
	files, totalItems, err := c.FileRepository.GetFilesByCollectionIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated files by collection with id: '%s'", pagination.Id)
	}
	var filesPb []*pb.File

	for _, file := range files {
		filesPb = append(filesPb, mapper.ConvertToFilePb(file))
	}
	return &pb.Files{Files: filesPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetObjectInstancesByObjectIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.ObjectInstances, error) {
	objectInstances, totalItems, err := c.ObjectInstanceRepository.GetObjectInstancesByObjectIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated objectInstances by object with id: '%s'", pagination.Id)
	}
	objectInstancesPb := make([]*pb.ObjectInstance, 0)

	for _, objectInstances := range objectInstances {
		objectInstancesPb = append(objectInstancesPb, mapper.ConvertToObjectInstancePb(objectInstances))
	}
	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetFilesByObjectIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.Files, error) {
	files, totalItems, err := c.FileRepository.GetFilesByObjectIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated files by object with id: '%s'", pagination.Id)
	}
	filesPb := make([]*pb.File, 0)

	for _, file := range files {
		filesPb = append(filesPb, mapper.ConvertToFilePb(file))
	}
	return &pb.Files{Files: filesPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetObjectInstanceChecksByObjectInstanceIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.ObjectInstanceChecks, error) {
	objectInstanceChecks, totalItems, err := c.ObjectInstanceCheckRepository.GetObjectInstanceChecksByObjectInstanceIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated objectInstanceChecks by objectInstance with id: '%s'", pagination.Id)
	}
	objectInstanceChecksPb := make([]*pb.ObjectInstanceCheck, 0)

	for _, objectInstanceCheck := range objectInstanceChecks {
		objectInstanceChecksPb = append(objectInstanceChecksPb, mapper.ConvertToObjectInstanceCheckPb(objectInstanceCheck))
	}
	return &pb.ObjectInstanceChecks{ObjectInstanceChecks: objectInstanceChecksPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetObjectInstancesByStoragePartitionIdPaginated(ctx context.Context, pagination *pb.Pagination) (*pb.ObjectInstances, error) {
	objectInstances, totalItems, err := c.ObjectInstanceRepository.GetObjectInstancesByPartitionIdPaginated(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated objectInstances by object with id: '%s'", pagination.Id)
	}
	objectInstancesPb := make([]*pb.ObjectInstance, 0)

	for _, objectInstances := range objectInstances {
		objectInstancesPb = append(objectInstancesPb, mapper.ConvertToObjectInstancePb(objectInstances))
	}
	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb, TotalItems: int32(totalItems)}, nil
}

//Statistic

func (c *ClerkHandlerServer) GetMimeTypesForCollectionId(ctx context.Context, pagination *pb.Pagination) (*pb.MimeTypes, error) {
	mimeTypes, totalItems, err := c.FileRepository.GetMimeTypesForCollectionId(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated mimeTypes by collection with id: '%s'", pagination.Id)
	}

	mimeTypesPb := make([]*pb.MimeType, 0)
	for _, mimeType := range mimeTypes {
		mimeTypePb := &pb.MimeType{Id: mimeType.Id.String, FileCount: int32(mimeType.FileCount)}
		mimeTypesPb = append(mimeTypesPb, mimeTypePb)
	}

	return &pb.MimeTypes{MimeTypes: mimeTypesPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) GetPronomsForCollectionId(ctx context.Context, pagination *pb.Pagination) (*pb.Pronoms, error) {
	pronoms, totalItems, err := c.FileRepository.GetPronomsForCollectionId(mapper.ConvertToPagination(pagination))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get paginated pronoms by collection with id: '%s'", pagination.Id)
	}

	pronomsPb := make([]*pb.Pronom, 0)
	for _, pronom := range pronoms {
		pronomPb := &pb.Pronom{Id: pronom.Id.String, FileCount: int32(pronom.FileCount)}
		pronomsPb = append(pronomsPb, pronomPb)
	}

	return &pb.Pronoms{Pronoms: pronomsPb, TotalItems: int32(totalItems)}, nil
}

func (c *ClerkHandlerServer) CheckStatus(ctx context.Context, id *pb.Id) (*pb.StatusObject, error) {
	status, err := c.StatusRepository.CheckStatus(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not CheckStatus with id: '%s'", id.Id)
	}
	statusPb := pb.StatusObject{Status: status.Status, LastChanged: status.LastChanged, Id: status.Id}

	return &statusPb, nil
}
func (c *ClerkHandlerServer) GetResultingQualityForObject(ctx context.Context, id *pb.Id) (*pb.SizeAndId, error) {
	quality, err := c.ObjectRepository.GetResultingQualityForObject(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetResultingQualityForObject with id: '%s'", id.Id)
	}
	qualityPb := pb.SizeAndId{Size: int64(quality)}

	return &qualityPb, nil
}
func (c *ClerkHandlerServer) GetNeededQualityForObject(ctx context.Context, id *pb.Id) (*pb.SizeAndId, error) {
	quality, err := c.ObjectRepository.GetNeededQualityForObject(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetNeededQualityForObject with id: '%s'", id.Id)
	}
	qualityPb := pb.SizeAndId{Size: int64(quality)}

	return &qualityPb, nil
}

func (c *ClerkHandlerServer) AlterStatus(ctx context.Context, statusPb *pb.StatusObject) (*pb.Status, error) {
	status := models.ArchivingStatus{Status: statusPb.Status, LastChanged: statusPb.LastChanged, Id: statusPb.Id}
	err := c.StatusRepository.AlterStatus(status)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not AlterStatus with id: '%s'", statusPb.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (c *ClerkHandlerServer) CreateStatus(ctx context.Context, statusPb *pb.StatusObject) (*pb.Id, error) {
	status := models.ArchivingStatus{Status: statusPb.Status, LastChanged: statusPb.LastChanged}
	id, err := c.StatusRepository.CreateStatus(status)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not AlterStatus with id: '%s'", statusPb.Id)
	}
	return &pb.Id{Id: id}, nil
}

func (c *ClerkHandlerServer) GetObjectInstancesByName(ctx context.Context, objectInstanceName *pb.Id) (*pb.ObjectInstances, error) {
	objectInstances, err := c.ObjectInstanceRepository.GetObjectInstancesByName(objectInstanceName.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not check whether ObjectInstanceWithNameExists with name: '%s' exists", objectInstanceName.Id)
	}
	var objectInstancesPb []*pb.ObjectInstance
	for _, objectInstance := range objectInstances {
		objectInstancePb := mapper.ConvertToObjectInstancePb(objectInstance)
		objectInstancesPb = append(objectInstancesPb, objectInstancePb)
	}
	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb}, nil
}

func (c *ClerkHandlerServer) GetObjectsByChecksum(ctx context.Context, checksum *pb.Id) (*pb.Objects, error) {
	objects, err := c.ObjectRepository.GetObjectsByChecksum(checksum.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get objects with checksum: '%s'", checksum.Id)
	}
	var objectsPb []*pb.Object
	for _, object := range objects {
		objectPb := mapper.ConvertToObjectPb(object)
		objectsPb = append(objectsPb, objectPb)
	}
	return &pb.Objects{Objects: objectsPb}, nil
}

func (c *ClerkHandlerServer) GetStatusForObjectId(ctx context.Context, id *pb.Id) (*pb.SizeAndId, error) {
	status, err := c.ObjectInstanceService.GetStatusForObjectId(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetStatusForObjectId for object with id: '%s'", id.Id)
	}
	statusPb := pb.SizeAndId{Size: int64(status)}

	return &statusPb, nil
}

func (c *ClerkHandlerServer) GetAmountOfErrorsByCollectionId(ctx context.Context, id *pb.Id) (*pb.SizeAndId, error) {
	status, err := c.ObjectInstanceRepository.GetAmountOfErrorsByCollectionId(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not GetAmountOfErrorsByCollectionId for collection with id: '%s'", id.Id)
	}
	statusPb := pb.SizeAndId{Size: int64(status)}

	return &statusPb, nil
}
