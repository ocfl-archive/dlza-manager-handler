package server

import (
	"context"
	"github.com/je4/utils/v2/pkg/zLogger"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewDispatcherHandlerServer(dispatcherRepository repository.DispatcherRepository, logger zLogger.ZLogger) *DispatcherHandlerServer {
	return &DispatcherHandlerServer{DispatcherRepository: dispatcherRepository, Logger: logger}
}

type DispatcherHandlerServer struct {
	pbHandler.UnimplementedDispatcherHandlerServiceServer
	DispatcherRepository repository.DispatcherRepository
	Logger               zLogger.ZLogger
}

func (d *DispatcherHandlerServer) GetLowQualityCollectionsWithObjectIds(ctx context.Context, param *pb.NoParam) (*pb.CollectionAliases, error) {
	collectionsWithObjectIds, err := d.DispatcherRepository.GetLowQualityCollectionsWithObjectIds()
	if err != nil {
		d.Logger.Error().Msgf("Could not get LowQualityCollections", err)
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
