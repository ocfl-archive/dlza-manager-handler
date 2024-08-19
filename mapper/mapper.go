package mapper

import (
	"github.com/ocfl-archive/dlza-manager-handler/models"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
)

func ConvertToTenant(tenantPb *pb.Tenant) models.Tenant {
	var tenant models.Tenant
	tenant.Id = tenantPb.Id
	tenant.Alias = tenantPb.Alias
	tenant.Person = tenantPb.Person
	tenant.Email = tenantPb.Email
	tenant.Name = tenantPb.Name
	tenant.ApiKeyId.String = tenantPb.ApiKeyId
	return tenant
}

func ConvertToTenantPb(tenant models.Tenant) *pb.Tenant {
	tenantPb := &pb.Tenant{}
	tenantPb.Name = tenant.Name
	tenantPb.Id = tenant.Id
	tenantPb.Person = tenant.Person
	tenantPb.Alias = tenant.Alias
	tenantPb.Email = tenant.Email
	tenantPb.ApiKeyId = tenant.ApiKeyId.String
	return tenantPb
}

func ConvertToCollection(collectionPb *pb.Collection) models.Collection {
	var collection models.Collection
	collection.Id = collectionPb.Id
	collection.Alias = collectionPb.Alias
	collection.Description = collectionPb.Description
	collection.Owner = collectionPb.Owner
	collection.Name = collectionPb.Name
	collection.OwnerMail = collectionPb.OwnerMail
	collection.Quality = int(collectionPb.Quality)
	collection.TenantId = collectionPb.TenantId
	collection.TotalObjectCount.Int32 = collectionPb.TotalObjectCount
	collection.TotalFileCount.Int32 = collectionPb.TotalFileCount
	collection.TotalFileSize.Int64 = collectionPb.TotalFileSize
	return collection
}

func ConvertToCollectionPb(collection models.Collection) *pb.Collection {
	collectionPb := &pb.Collection{}
	collectionPb.Alias = collection.Alias
	collectionPb.Description = collection.Description
	collectionPb.Owner = collection.Owner
	collectionPb.Name = collection.Name
	collectionPb.OwnerMail = collection.OwnerMail
	collectionPb.Quality = int32(collection.Quality)
	collectionPb.TenantId = collection.TenantId
	collectionPb.Id = collection.Id
	collectionPb.TotalObjectCount = collection.TotalObjectCount.Int32
	collectionPb.TotalFileCount = collection.TotalFileCount.Int32
	collectionPb.TotalFileSize = collection.TotalFileSize.Int64
	return collectionPb
}

func ConvertToStorageLocation(storageLocationPb *pb.StorageLocation) models.StorageLocation {
	var storageLocation models.StorageLocation
	storageLocation.Id = storageLocationPb.Id
	storageLocation.Alias = storageLocationPb.Alias
	storageLocation.Type = storageLocationPb.Type
	storageLocation.Vault.String = storageLocationPb.Vault
	storageLocation.Connection = storageLocationPb.Connection
	storageLocation.Quality = int(storageLocationPb.Quality)
	storageLocation.Price = int(storageLocationPb.Price)
	storageLocation.SecurityCompliency = storageLocationPb.SecurityCompliency
	storageLocation.FillFirst = storageLocationPb.FillFirst
	storageLocation.OcflType = storageLocationPb.OcflType
	storageLocation.NumberOfThreads = int(storageLocationPb.NumberOfThreads)
	storageLocation.TenantId = storageLocationPb.TenantId
	storageLocation.TotalFilesSize.Int64 = storageLocationPb.TotalFilesSize
	storageLocation.TotalExistingVolume.Int64 = storageLocationPb.TotalExistingVolume
	return storageLocation
}

func ConvertToStorageLocationPb(storageLocation models.StorageLocation) *pb.StorageLocation {
	storageLocationPb := &pb.StorageLocation{}
	storageLocationPb.Id = storageLocation.Id
	storageLocationPb.Alias = storageLocation.Alias
	storageLocationPb.Type = storageLocation.Type
	storageLocationPb.Vault = storageLocation.Vault.String
	storageLocationPb.Connection = storageLocation.Connection
	storageLocationPb.Quality = int32(storageLocation.Quality)
	storageLocationPb.Price = int32(storageLocation.Price)
	storageLocationPb.SecurityCompliency = storageLocation.SecurityCompliency
	storageLocationPb.FillFirst = storageLocation.FillFirst
	storageLocationPb.OcflType = storageLocation.OcflType
	storageLocationPb.NumberOfThreads = int32(storageLocation.NumberOfThreads)
	storageLocationPb.TenantId = storageLocation.TenantId
	storageLocationPb.TotalFilesSize = storageLocation.TotalFilesSize.Int64
	storageLocationPb.TotalExistingVolume = storageLocation.TotalExistingVolume.Int64
	return storageLocationPb
}

func ConvertToObject(objectPb *pb.Object) models.Object {
	var object models.Object
	object.Signature = objectPb.Signature
	object.Sets = objectPb.Sets
	object.Identifiers = objectPb.Identifiers
	object.Title = objectPb.Title
	object.AlternativeTitles = objectPb.AlternativeTitles
	object.Description = objectPb.Description
	object.Keywords = objectPb.Keywords
	object.References = objectPb.References
	object.IngestWorkflow = objectPb.IngestWorkflow
	object.User = objectPb.User
	object.Address = objectPb.Address
	object.Created = objectPb.Created
	object.LastChanged = objectPb.LastChanged
	object.Size = objectPb.Size
	object.Id = objectPb.Id
	object.CollectionId = objectPb.CollectionId
	object.Checksum = objectPb.Checksum
	object.Expiration.String = objectPb.Expiration
	object.Authors = objectPb.Authors
	object.Holding.String = objectPb.Holding
	object.TotalFileCount.Int32 = objectPb.TotalFileCount
	object.TotalFileSize.Int64 = objectPb.TotalFileSize
	return object
}

func ConvertToObjectPb(object models.Object) *pb.Object {
	objectPb := &pb.Object{}
	objectPb.Signature = object.Signature
	objectPb.Sets = object.Sets
	objectPb.Identifiers = object.Identifiers
	objectPb.Title = object.Title
	objectPb.AlternativeTitles = object.AlternativeTitles
	objectPb.Description = object.Description
	objectPb.Keywords = object.Keywords
	objectPb.References = object.References
	objectPb.IngestWorkflow = object.IngestWorkflow
	objectPb.User = object.User
	objectPb.Address = object.Address
	objectPb.Created = object.Created
	objectPb.LastChanged = object.LastChanged
	objectPb.Size = object.Size
	objectPb.Id = object.Id
	objectPb.Expiration = object.Expiration.String
	objectPb.Authors = object.Authors
	objectPb.Holding = object.Holding.String
	objectPb.CollectionId = object.CollectionId
	objectPb.Checksum = object.Checksum
	objectPb.TotalFileCount = object.TotalFileCount.Int32
	objectPb.TotalFileSize = object.TotalFileSize.Int64
	return objectPb
}

func ConvertToObjectInstance(objectInstancePb *pb.ObjectInstance) models.ObjectInstance {
	var objectInstance models.ObjectInstance
	objectInstance.Path = objectInstancePb.Path
	objectInstance.Size = int(objectInstancePb.Size)
	objectInstance.Created = objectInstancePb.Created
	objectInstance.Status = objectInstancePb.Status
	objectInstance.Id = objectInstancePb.Id
	objectInstance.StoragePartitionId = objectInstancePb.StoragePartitionId
	objectInstance.ObjectId = objectInstancePb.ObjectId

	return objectInstance
}

func ConvertToObjectInstancePb(objectInstance models.ObjectInstance) *pb.ObjectInstance {
	objectInstancePb := &pb.ObjectInstance{}
	objectInstancePb.Path = objectInstance.Path
	objectInstancePb.Size = int64(objectInstance.Size)
	objectInstancePb.Created = objectInstance.Created
	objectInstancePb.Status = objectInstance.Status
	objectInstancePb.Id = objectInstance.Id
	objectInstancePb.StoragePartitionId = objectInstance.StoragePartitionId
	objectInstancePb.ObjectId = objectInstance.ObjectId
	return objectInstancePb
}

func ConvertToStoragePartition(storagePartitionPb *pb.StoragePartition) models.StoragePartition {
	var storagePartition models.StoragePartition
	storagePartition.Alias = storagePartitionPb.Alias
	storagePartition.Name = storagePartitionPb.Name
	storagePartition.MaxSize = int(storagePartitionPb.MaxSize)
	storagePartition.MaxObjects = int(storagePartitionPb.MaxObjects)
	storagePartition.CurrentSize = int(storagePartitionPb.CurrentSize)
	storagePartition.CurrentObjects = int(storagePartitionPb.CurrentObjects)
	storagePartition.Id = storagePartitionPb.Id
	storagePartition.StorageLocationId = storagePartitionPb.StorageLocationId

	return storagePartition
}

func ConvertToStoragePartitionPb(storagePartition models.StoragePartition) *pb.StoragePartition {
	storagePartitionPb := &pb.StoragePartition{}
	storagePartitionPb.Alias = storagePartition.Alias
	storagePartitionPb.Name = storagePartition.Name
	storagePartitionPb.MaxSize = int64(storagePartition.MaxSize)
	storagePartitionPb.MaxObjects = int64(storagePartition.MaxObjects)
	storagePartitionPb.CurrentSize = int64(storagePartition.CurrentSize)
	storagePartitionPb.CurrentObjects = int64(storagePartition.CurrentObjects)
	storagePartitionPb.Id = storagePartition.Id
	storagePartitionPb.StorageLocationId = storagePartition.StorageLocationId
	return storagePartitionPb
}

func ConvertToFile(filePb *pb.File) models.File {
	var file models.File
	file.Checksum = filePb.Checksum
	file.Name = filePb.Name
	file.Size = int(filePb.Size)
	file.MimeType = filePb.MimeType
	file.Pronom = filePb.Pronom
	file.Width.Int64 = filePb.Width
	file.Height.Int64 = filePb.Height
	file.Duration.Int64 = filePb.Duration
	file.Id = filePb.Id
	file.ObjectId = filePb.ObjectId
	return file
}

func ConvertToFilePb(file models.File) *pb.File {
	filePb := &pb.File{}
	filePb.Checksum = file.Checksum
	filePb.Name = file.Name
	filePb.Size = int64(file.Size)
	filePb.MimeType = file.MimeType
	filePb.Pronom = file.Pronom
	filePb.Width = file.Width.Int64
	filePb.Height = file.Height.Int64
	filePb.Duration = file.Duration.Int64
	filePb.Id = file.Id
	filePb.ObjectId = file.ObjectId
	return filePb
}

func ConvertToObjectInstanceCheckPb(objectInstanceCheck models.ObjectInstanceCheck) *pb.ObjectInstanceCheck {
	var objectInstanceCheckPb pb.ObjectInstanceCheck
	objectInstanceCheckPb.CheckTime = objectInstanceCheck.CheckTime
	objectInstanceCheckPb.Error = objectInstanceCheck.Error
	objectInstanceCheckPb.Message = objectInstanceCheck.Message
	objectInstanceCheckPb.Id = objectInstanceCheck.Id
	objectInstanceCheckPb.ObjectInstanceId = objectInstanceCheck.ObjectInstanceId
	return &objectInstanceCheckPb
}

func ConvertToObjectInstanceCheck(objectInstanceCheckPb *pb.ObjectInstanceCheck) models.ObjectInstanceCheck {
	var objectInstanceCheck models.ObjectInstanceCheck
	objectInstanceCheck.CheckTime = objectInstanceCheckPb.CheckTime
	objectInstanceCheck.Error = objectInstanceCheckPb.Error
	objectInstanceCheck.Message = objectInstanceCheckPb.Message
	objectInstanceCheck.Id = objectInstanceCheckPb.Id
	objectInstanceCheck.ObjectInstanceId = objectInstanceCheckPb.ObjectInstanceId
	return objectInstanceCheck
}

func ConvertToPagination(paginationPb *pb.Pagination) models.Pagination {
	var pagination models.Pagination
	pagination.Id = paginationPb.Id
	pagination.SecondId = paginationPb.SecondId
	pagination.SearchField = paginationPb.SearchField
	pagination.SortKey = paginationPb.SortKey
	pagination.SortDirection = paginationPb.SortDirection
	pagination.Skip = int(paginationPb.Skip)
	pagination.Take = int(paginationPb.Take)
	pagination.AllowedTenants = paginationPb.AllowedTenants
	return pagination
}
