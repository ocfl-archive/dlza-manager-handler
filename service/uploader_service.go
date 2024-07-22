package service

import (
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
)

type UploaderService interface {
	TenantHasAccess(object *pb.UploaderAccessObject) (pb.Status, error)
}
