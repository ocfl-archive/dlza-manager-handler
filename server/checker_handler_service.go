package server

import (
	"context"
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/ocfl-archive/dlza-manager/mapper"
)

type CheckerHandlerServer struct {
	pbHandler.UnimplementedCheckerHandlerServiceServer
	ObjectInstanceRepository      repository.ObjectInstanceRepository
	ObjectRepository              repository.ObjectRepository
	ObjectInstanceCheckRepository repository.ObjectInstanceCheckRepository
	Logger                        zLogger.ZLogger
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

func (c *CheckerHandlerServer) GetObjectExceptListOlderThanWithChecks(ctx context.Context, idsWithInterval *pb.IdsWithSQLInterval) (*pb.Object, error) {
	object, err := c.ObjectRepository.GetObjectExceptListOlderThanWithChecks(idsWithInterval.Ids, idsWithInterval.Interval)
	if err != nil {
		c.Logger.Error().Msgf("Could not GetObjectExceptListOlderThanWithChecks. err: %s", err)
		return nil, errors.Wrapf(err, "Could not GetObjectExceptListOlderThanWithChecks")
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
