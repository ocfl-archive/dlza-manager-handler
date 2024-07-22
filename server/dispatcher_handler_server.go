package server

import (
	"context"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/pkg/errors"
)

type DispatcherHandlerServer struct {
	pbHandler.UnimplementedDispatcherHandlerServiceServer
	DispatcherRepository repository.DispatcherRepository
}

func (d *DispatcherHandlerServer) GetLowQualityCollections(ctx context.Context, param *pb.NoParam) (*pb.CollectionAliases, error) {
	collectionAliases, err := d.DispatcherRepository.GetCollectionsWithLowQuality()
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get LowQualityCollections")
	}
	collectionAliasesPb := make([]*pb.CollectionAlias, 0)

	for _, collectionAlias := range collectionAliases {
		collectionAliasPb := pb.CollectionAlias{CollectionAlias: collectionAlias}
		collectionAliasesPb = append(collectionAliasesPb, &collectionAliasPb)
	}

	return &pb.CollectionAliases{CollectionAliases: collectionAliasesPb}, nil
}
