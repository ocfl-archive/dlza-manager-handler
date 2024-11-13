package server

import (
	"context"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

func NewDispatcherHandlerServer(dispatcherRepository repository.DispatcherRepository) *DispatcherHandlerServer {
	return &DispatcherHandlerServer{DispatcherRepository: dispatcherRepository}
}

type DispatcherHandlerServer struct {
	pbHandler.UnimplementedDispatcherHandlerServiceServer
	DispatcherRepository repository.DispatcherRepository
}

func (d *DispatcherHandlerServer) GetLowQualityCollectionsWithObjectIds(ctx context.Context, param *pb.NoParam) (*pb.CollectionAliases, error) {
	collectionsWithObjectIds, err := d.DispatcherRepository.GetLowQualityCollectionsWithObjectIds()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not get LowQualityCollections: %v", err)
	}

	collectionAliasesPb := make([]*pb.CollectionAlias, 0)
	for _, collectionAlias := range maps.Keys(collectionsWithObjectIds) {
		idsPb := make([]*pb.Id, 0)
		for _, id := range collectionsWithObjectIds[collectionAlias] {
			idsPb = append(idsPb, &pb.Id{Id: id})
		}
		collectionAliasPb := pb.CollectionAlias{CollectionAlias: collectionAlias, Ids: idsPb}
		collectionAliasesPb = append(collectionAliasesPb, &collectionAliasPb)
	}

	return &pb.CollectionAliases{CollectionAliases: collectionAliasesPb}, nil
}
