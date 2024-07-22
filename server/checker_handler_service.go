package server

import (
	"context"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/mapper"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/pkg/errors"
)

type CheckerHandlerServer struct {
	pbHandler.UnimplementedCheckerHandlerServiceServer
	ObjectInstanceRepository      repository.ObjectInstanceRepository
	ObjectRepository              repository.ObjectRepository
	ObjectInstanceCheckRepository repository.ObjectInstanceCheckRepository
	StorageLocationRepository     repository.StorageLocationRepository
}

func (c *CheckerHandlerServer) GetAllObjectInstances(ctx context.Context, noParam *pb.NoParam) (*pb.ObjectInstances, error) {
	objectInstances, err := c.ObjectInstanceRepository.GetAllObjectInstances()
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get all object instances")
	}
	var objectInstancesPb []*pb.ObjectInstance

	for _, objectInstance := range objectInstances {
		objectInstancesPb = append(objectInstancesPb, mapper.ConvertToObjectInstancePb(objectInstance))
	}

	return &pb.ObjectInstances{ObjectInstances: objectInstancesPb}, nil
}

func (c *CheckerHandlerServer) UpdateObjectInstance(ctx context.Context, objectInstancePb *pb.ObjectInstance) (*pb.NoParam, error) {
	err := c.ObjectInstanceRepository.UpdateObjectInstance(mapper.ConvertToObjectInstance(objectInstancePb))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get all object instances")
	}
	return nil, nil
}

func (c *CheckerHandlerServer) CreateObjectInstanceCheck(ctx context.Context, objectInstanceCheckPb *pb.ObjectInstanceCheck) (*pb.NoParam, error) {
	_, err := c.ObjectInstanceCheckRepository.CreateObjectInstanceCheck(mapper.ConvertToObjectInstanceCheck(objectInstanceCheckPb))
	if err != nil {
		return &pb.NoParam{}, errors.Wrapf(err, "Could not create object instance check")
	}
	return &pb.NoParam{}, nil
}

func (c *CheckerHandlerServer) GetStorageLocationByObjectInstanceId(ctx context.Context, id *pb.Id) (*pb.StorageLocation, error) {
	storageLocation, err := c.StorageLocationRepository.GetStorageLocationByObjectInstanceId(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get storage location by id object instance id: %v", id.Id)
	}
	return mapper.ConvertToStorageLocationPb(storageLocation), nil
}

func (c *CheckerHandlerServer) GetObjectById(ctx context.Context, id *pb.Id) (*pb.Object, error) {
	object, err := c.ObjectRepository.GetObjectById(id.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get object by id: %v", id.Id)
	}
	return mapper.ConvertToObjectPb(object), nil
}
