package repository

import (
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
)

type TransactionRepository interface {
	SaveAllTableObjectsAfterCopying([]*pb.InstanceWithPartitionAndObjectWithFile) error
}
