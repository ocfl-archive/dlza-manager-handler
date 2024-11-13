package tests

import (
	"github.com/ocfl-archive/dlza-manager-handler/service"
	"github.com/ocfl-archive/dlza-manager/models"
	"github.com/stretchr/testify/mock"
	"testing"
)

type ObjectInstanceRepositoryMock struct {
	mock.Mock
}

func (o ObjectInstanceRepositoryMock) CreateObjectInstance(instance models.ObjectInstance) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) UpdateObjectInstance(instance models.ObjectInstance) error {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) DeleteObjectInstance(id string) error {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) GetObjectInstanceById(id string) (models.ObjectInstance, error) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) GetObjectInstancesByObjectId(id string) ([]models.ObjectInstance, error) {
	args := o.Called(id)

	objectInstances0 := []models.ObjectInstance{{Status: "new"}, {Status: "new"}, {Status: "new"}}
	objectInstances1 := []models.ObjectInstance{{Status: "error"}, {Status: "new"}, {Status: "new"}}
	objectInstances2 := []models.ObjectInstance{{Status: "error"}, {Status: "error"}, {Status: "error"}}

	switch id {
	case "1":
		return objectInstances1, args.Error(0)
	case "0":
		return objectInstances0, args.Error(0)
	case "2":
		return objectInstances2, args.Error(0)
	}

	return nil, args.Error(0)
}

func (o ObjectInstanceRepositoryMock) GetObjectInstancesByObjectIdPaginated(pagination models.Pagination) ([]models.ObjectInstance, int, error) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) GetObjectInstancesByPartitionIdPaginated(pagination models.Pagination) ([]models.ObjectInstance, int, error) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) GetAllObjectInstances() ([]models.ObjectInstance, error) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) GetAmountOfErrorsByCollectionId(id string) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) CreateObjectInstancePreparedStatements() error {
	//TODO implement me
	panic("implement me")
}

func (o ObjectInstanceRepositoryMock) GetObjectInstancesByName(name string) ([]models.ObjectInstance, error) {
	//TODO implement me
	panic("implement me")
}

func TestGetStatusForObjectId1(t *testing.T) {

	repositoryMock := &ObjectInstanceRepositoryMock{}
	repositoryMock.On("GetObjectInstancesByObjectId", "1").Return(nil, nil)

	objectInstanceService := service.ObjectInstanceServiceImpl{ObjectInstanceRepository: repositoryMock}

	status, _ := objectInstanceService.GetStatusForObjectId("1")

	if status != 1 {
		panic("TestGetStatusForObjectId1 failed")
	}
}

func TestGetStatusForObjectId0(t *testing.T) {

	repositoryMock := &ObjectInstanceRepositoryMock{}
	repositoryMock.On("GetObjectInstancesByObjectId", "0").Return(nil, nil)

	objectInstanceService := service.ObjectInstanceServiceImpl{ObjectInstanceRepository: repositoryMock}

	status, _ := objectInstanceService.GetStatusForObjectId("0")

	if status != 0 {
		panic("TestGetStatusForObjectId0 failed")
	}
}

func TestGetStatusForObjectId2(t *testing.T) {

	repositoryMock := &ObjectInstanceRepositoryMock{}
	repositoryMock.On("GetObjectInstancesByObjectId", "2").Return(nil, nil)

	objectInstanceService := service.ObjectInstanceServiceImpl{ObjectInstanceRepository: repositoryMock}

	status, _ := objectInstanceService.GetStatusForObjectId("2")

	if status != 2 {
		panic("TestGetStatusForObjectId2 failed")
	}
}
