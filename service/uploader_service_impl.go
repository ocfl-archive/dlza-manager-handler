package service

import (
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/pkg/errors"
)

type UploaderServiceImpl struct {
	TenantRepository     repository.TenantRepository
	CollectionRepository repository.CollectionRepository
}

func (u *UploaderServiceImpl) TenantHasAccess(object *pb.UploaderAccessObject) (pb.Status, error) {
	tenant, err := u.TenantRepository.FindTenantByKey(object.Key)
	if err != nil {
		return pb.Status{Ok: false}, errors.Wrapf(err, "Could not get tenant with id: '%s'", object.Key)
	}
	if tenant.Id == "" {
		return pb.Status{Ok: false}, errors.New("The given key is invalid")
	}
	collection, err := u.CollectionRepository.GetCollectionByAlias(object.Collection)
	if err != nil {
		return pb.Status{Ok: false}, errors.Wrapf(err, "Could not get collections with alias: '%s'", object.Collection)
	}
	if collection.Id == "" || collection.TenantId != tenant.Id {
		return pb.Status{Ok: false}, errors.New("The given collection does not exist or belongs to other tenant")
	}
	return pb.Status{Ok: true}, nil
}
