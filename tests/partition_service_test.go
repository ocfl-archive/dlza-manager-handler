package tests

import (
	"github.com/ocfl-archive/dlza-manager-handler/service"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/ocfl-archive/dlza-manager/models"
	"github.com/stretchr/testify/mock"
	"testing"
)

type StoragePartitionRepositoryMock struct {
	mock.Mock
}

func (s *StoragePartitionRepositoryMock) GetStoragePartitionByObjectSignatureAndLocation(signature string, locationId string) (models.StoragePartition, error) {
	partition3 := models.StoragePartition{Name: "Partition3", Id: "1234-5678-4321-3333", MaxSize: 1000000, MaxObjects: 100, CurrentObjects: 23, CurrentSize: 150000, StorageLocationId: "1"}
	return partition3, nil
}

func (s *StoragePartitionRepositoryMock) DeleteStoragePartitionById(id string) error {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) GetStoragePartitionById(id string) (models.StoragePartition, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) GetStoragePartitionsByLocationIdPaginated(pagination models.Pagination) ([]models.StoragePartition, int, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) CreateStoragePartition(partition models.StoragePartition) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) DeleteStoragePartition(id string) error {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) GetStoragePartition(id string) (models.StoragePartition, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) GetStoragePartitionForLocation(locationId string) (models.StoragePartition, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) UpdateStoragePartition(partition models.StoragePartition) error {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) CreateStoragePartitionPreparedStatements() error {
	//TODO implement me
	panic("implement me")
}

func (s *StoragePartitionRepositoryMock) GetStoragePartitionsByLocationId(locationId string) ([]models.StoragePartition, error) {
	args := s.Called(locationId)
	partitions := make([]models.StoragePartition, 0)

	partition1 := models.StoragePartition{Name: "Partition1", Id: "1234-5678-4321-1111", MaxSize: 1000000, MaxObjects: 100, CurrentObjects: 23, CurrentSize: 950000, StorageLocationId: "1"}
	partition2 := models.StoragePartition{Name: "Partition2", Id: "1234-5678-4321-2222", MaxSize: 1000000, MaxObjects: 100, CurrentObjects: 100, CurrentSize: 96000, StorageLocationId: "1"}
	partition3 := models.StoragePartition{Name: "Partition3", Id: "1234-5678-4321-3333", MaxSize: 1000000, MaxObjects: 100, CurrentObjects: 23, CurrentSize: 150000, StorageLocationId: "1"}
	partition4 := models.StoragePartition{Name: "Partition4", Id: "1234-5678-4321-4444", MaxSize: 1000000, MaxObjects: 100, CurrentObjects: 23, CurrentSize: 140000, StorageLocationId: "1"}
	partitions = append(partitions, partition1, partition2, partition3, partition4)

	return partitions, args.Error(0)
}

func TestGetStoragePartitionForLocation(t *testing.T) {

	repositoryMock := &StoragePartitionRepositoryMock{}

	repositoryMock.On("GetStoragePartitionsByLocationId", "1").Return(nil, nil)

	storagePartitionService := service.StoragePartitionService{StoragePartitionRepository: repositoryMock}

	partition, _ := storagePartitionService.GetStoragePartitionForLocation(&pb.SizeAndId{Id: "1", Size: 100000})

	repositoryMock.AssertExpectations(t)

	if partition.Id != "1234-5678-4321-3333" {
		panic("TestGetStoragePartitionForLocation failed")
	}
}

func TestGetStoragePartitionForLocationNegative(t *testing.T) {

	repositoryMock := &StoragePartitionRepositoryMock{}

	repositoryMock.On("GetStoragePartitionsByLocationId", "1").Return(nil, nil)

	storagePartitionService := service.StoragePartitionService{StoragePartitionRepository: repositoryMock}

	partition, _ := storagePartitionService.GetStoragePartitionForLocation(&pb.SizeAndId{Id: "1", Size: 950000})

	repositoryMock.AssertExpectations(t)

	if partition != nil {
		panic("TestGetStoragePartitionForLocationNegative failed")
	}
}

func TestGetStoragePartitionForLocationV2(t *testing.T) {

	repositoryMock := &StoragePartitionRepositoryMock{}
	repositoryMock.On("GetStoragePartitionsByLocationId", "1").Return(nil, nil)

	storagePartitionService := service.StoragePartitionService{StoragePartitionRepository: repositoryMock}
	object := &pb.Object{Head: "v2", Signature: "signature"}
	partition, _ := storagePartitionService.GetStoragePartitionForLocation(&pb.SizeAndId{Id: "1", Size: 100000, Object: object})

	repositoryMock.AssertExpectations(t)

	if partition.Id != "1234-5678-4321-4444" {
		panic("TestGetStoragePartitionForLocation failed")
	}
}
