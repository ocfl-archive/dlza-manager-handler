package main

import (
	"flag"
	"github.com/ocfl-archive/dlza-manager-handler/config"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager-handler/server"
	"github.com/ocfl-archive/dlza-manager-handler/service"
	"github.com/ocfl-archive/dlza-manager-handler/storage"
	"log"
	"net"
	"strconv"

	"emperror.dev/errors"
	"google.golang.org/grpc"
)

var configParam = flag.String("config", "", "config file in toml format")

func main() {

	flag.Parse()

	conf := config.GetConfig(*configParam)

	db, err := storage.NewConnection(&conf.Handler.Database)

	if err != nil {
		log.Fatal("Could not load the DB")
	}
	defer db.Close()

	tenantRepository := repository.NewTenantRepository(db, conf.Handler.Database.Schema)

	err = tenantRepository.CreatePreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for tenantRepository err: %v", err)
	}
	checkerRepository := repository.NewCheckerRepository(db, conf.Handler.Database.Schema)
	err = checkerRepository.CreatePreparedStatementsForChecker()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for checkerRepository err: %v", err)
	}
	objectRepository := repository.NewObjectRepository(db, conf.Handler.Database.Schema)
	err = objectRepository.CreateObjectPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for objectRepository err: %v", err)
	}
	dispatcherRepository := repository.NewDispatcherRepository(db, conf.Handler.Database.Schema)
	err = dispatcherRepository.CreateDispatcherPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for dispatcherRepository err: %v", err)
	}
	storageLocationRepository := repository.NewStorageLocationRepository(db, conf.Handler.Database.Schema)
	err = storageLocationRepository.CreateStorageLocPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for storageLocationRepository err: %v", err)
	}
	collectionRepository := repository.NewCollectionRepository(db, conf.Handler.Database.Schema)
	err = collectionRepository.CreateCollectionPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for collectionRepository err: %v", err)
	}
	objectInstanceRepository := repository.NewObjectInstanceRepository(db, conf.Handler.Database.Schema)
	err = objectInstanceRepository.CreateObjectInstancePreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for objectInstanceRepository err: %v", err)
	}
	objectInstanceService := service.NewObjectInstanceService(objectInstanceRepository)
	objectInstanceCheckRepository := repository.NewObjectInstanceCheckRepository(db, conf.Handler.Database.Schema)
	err = objectInstanceCheckRepository.CreateObjectInstanceCheckPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for objectInstanceCheckRepository err: %v", err)
	}
	storagePartitionRepository := repository.NewStoragePartitionRepository(db, conf.Handler.Database.Schema)
	err = storagePartitionRepository.CreateStoragePartitionPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for storagePartitionRepository err: %v", err)
	}
	storagePartitionService := service.StoragePartitionService{StoragePartitionRepository: storagePartitionRepository}

	fileRepository := repository.NewFileRepository(db, conf.Handler.Database.Schema)
	err = fileRepository.CreateFilePreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for fileRepository err: %v", err)
	}
	statusRepository := repository.NewStatusRepository(db, conf.Handler.Database.Schema)
	err = statusRepository.CreateStatusPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for statusRepository err: %v", err)
	}
	uploadService := service.UploaderServiceImpl{CollectionRepository: collectionRepository, TenantRepository: tenantRepository}
	storageLocationService := service.NewStorageLocationService(collectionRepository, storageLocationRepository, storagePartitionService)

	transactionRepository := repository.NewTransactionRepository(db, conf.Handler.Database.Schema)
	refreshMaterializedViewRepository := repository.NewRefreshMaterializedViewsRepository(db, conf.Handler.Database.Schema)

	//Listen StorageHandler, Dispatcher, Clerk
	lisHandler, err := net.Listen("tcp", ":"+strconv.Itoa(conf.Handler.Port))
	if err != nil {
		panic(errors.Wrapf(err, "Failed to listen gRPC server"))
	}
	grpcServerHandler := grpc.NewServer()
	pb.RegisterStorageHandlerHandlerServiceServer(grpcServerHandler, &server.StorageHandlerHandlerServer{CollectionRepository: collectionRepository,
		ObjectRepository: objectRepository, StorageLocationRepository: storageLocationRepository, ObjectInstanceRepository: objectInstanceRepository,
		StoragePartitionService: storagePartitionService, FileRepository: fileRepository, StatusRepository: statusRepository, TransactionRepository: transactionRepository,
		RefreshMaterializedViewsRepository: refreshMaterializedViewRepository, UploaderService: &uploadService})
	pb.RegisterClerkHandlerServiceServer(grpcServerHandler, &server.ClerkHandlerServer{TenantService: service.NewTenantService(tenantRepository),
		CollectionRepository: collectionRepository, StorageLocationRepository: storageLocationRepository, ObjectRepository: objectRepository, ObjectInstanceRepository: objectInstanceRepository,
		FileRepository: fileRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository, StoragePartitionRepository: storagePartitionRepository, StatusRepository: statusRepository,
		ObjectInstanceService: objectInstanceService, TenantRepository: tenantRepository, StorageLocationService: storageLocationService, RefreshMaterializedViewsRepository: refreshMaterializedViewRepository})
	pb.RegisterDispatcherHandlerServiceServer(grpcServerHandler, &server.DispatcherHandlerServer{DispatcherRepository: dispatcherRepository})
	pb.RegisterUploaderHandlerServiceServer(grpcServerHandler, &server.UploaderHandlerServer{UploaderService: &uploadService, TransactionRepository: transactionRepository,
		CollectionRepository: collectionRepository, StatusRepository: statusRepository, ObjectRepository: objectRepository, ObjectInstanceRepository: objectInstanceRepository})
	pb.RegisterCheckerHandlerServiceServer(grpcServerHandler, &server.CheckerHandlerServer{ObjectInstanceRepository: objectInstanceRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository,
		StorageLocationRepository: storageLocationRepository, ObjectRepository: objectRepository})
	log.Printf("server started at %v", lisHandler.Addr())

	if err := grpcServerHandler.Serve(lisHandler); err != nil {
		panic(errors.Wrapf(err, "Failed to serve gRPC server on port: %v", conf.Handler.Port))
	}
}
