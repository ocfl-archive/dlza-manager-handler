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

type UploaderHandlerServer struct {
	pbHandler.UnimplementedUploaderHandlerServiceServer
	UploaderService          service.UploaderService
	CollectionRepository     repository.CollectionRepository
	TransactionRepository    repository.TransactionRepository
	ObjectInstanceRepository repository.ObjectInstanceRepository
	ObjectRepository         repository.ObjectRepository
	StatusRepository         repository.StatusRepository
}

func (u *UploaderHandlerServer) TenantHasAccess(ctx context.Context, object *pb.UploaderAccessObject) (*pb.Status, error) {
	status, err := u.UploaderService.TenantHasAccess(object)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not exequte TenantHasAccess, err: %v", err)
	}
	return &status, nil
}

func (u *UploaderHandlerServer) AlterStatus(ctx context.Context, statusPb *pb.StatusObject) (*pb.Status, error) {
	status := models.ArchivingStatus{Status: statusPb.Status, LastChanged: statusPb.LastChanged, Id: statusPb.Id}
	err := u.StatusRepository.AlterStatus(status)
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not AlterStatus with id: '%s'", statusPb.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (u *UploaderHandlerServer) GetObjectInstancesByName(ctx context.Context, objectInstanceName *pb.Id) (*pb.ObjectInstances, error) {
	objectInstances, err := u.ObjectInstanceRepository.GetObjectInstancesByName(objectInstanceName.Id)
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

func (u *UploaderHandlerServer) GetObjectsByChecksum(ctx context.Context, checksum *pb.Id) (*pb.Objects, error) {
	objects, err := u.ObjectRepository.GetObjectsByChecksum(checksum.Id)
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
