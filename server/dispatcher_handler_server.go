package server

import (
	"context"
	pbHandler "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewDispatcherHandlerServer(dispatcherRepository repository.DispatcherRepository) *DispatcherHandlerServer {
	return &DispatcherHandlerServer{DispatcherRepository: dispatcherRepository}
}

type DispatcherHandlerServer struct {
	pbHandler.UnimplementedDispatcherHandlerServiceServer
	DispatcherRepository repository.DispatcherRepository
	//domains []string
}

func (d *DispatcherHandlerServer) GetLowQualityCollections(ctx context.Context, param *pb.NoParam) (*pb.CollectionAliases, error) {
	/*
		domains := metadata.ValueFromIncomingContext(ctx, "domain")
		domain := ""
		if len(domains) > 0 {
			domain = domains[0]
		}
		if !slices.Contains(d.domains, domain) {
			return nil, status.Errorf(codes.PermissionDenied, "domain %s not supported", domain)
		}

	*/
	collectionAliases, err := d.DispatcherRepository.GetCollectionsWithLowQuality()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not get LowQualityCollections: %v", err)
	}
	collectionAliasesPb := make([]*pb.CollectionAlias, 0)

	for _, collectionAlias := range collectionAliases {
		collectionAliasPb := pb.CollectionAlias{CollectionAlias: collectionAlias}
		collectionAliasesPb = append(collectionAliasesPb, &collectionAliasPb)
	}

	return &pb.CollectionAliases{CollectionAliases: collectionAliasesPb}, nil
}
