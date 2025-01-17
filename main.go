package main

import (
	"flag"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-handler/config"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager-handler/server"
	"github.com/ocfl-archive/dlza-manager-handler/service"
	"github.com/ocfl-archive/dlza-manager-handler/storage"
	"github.com/rs/zerolog"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

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

	// create logger instance
	var out io.Writer = os.Stdout
	if string(conf.Logging.LogFile) != "" {
		fp, err := os.OpenFile(string(conf.Logging.LogFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", string(conf.Logging.LogFile), err)
		}
		defer fp.Close()
		out = fp
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(string(conf.Logging.LogLevel)))
	var logger zLogger.ZLogger = &_logger
	daLogger := zLogger.NewZWrapper(logger)

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

	obje, cou, err := fileRepository.GetPronomsForCollectionId(models.Pagination{Id: "ad5cee64-9e64-4690-a87e-9058483c7db9", SortDirection: "ID", Take: 10})

	_ = obje
	_ = cou

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
		RefreshMaterializedViewsRepository: refreshMaterializedViewRepository, Logger: daLogger})
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
