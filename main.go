package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/je4/trustutil/v2/pkg/certutil"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-handler/config"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager-handler/server"
	"github.com/ocfl-archive/dlza-manager-handler/service"
	"github.com/ocfl-archive/dlza-manager-handler/storage"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"go.ub.unibas.ch/cloud/miniresolver/v2/pkg/resolver"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var configfile = flag.String("config", "", "config file in toml format")

func main() {
	flag.Parse()

	var cfgFS fs.FS
	var cfgFile string
	if *configfile != "" {
		cfgFS = os.DirFS(filepath.Dir(*configfile))
		cfgFile = filepath.Base(*configfile)
	} else {
		cfgFS = config.ConfigFS
		cfgFile = "handler.toml"
	}

	conf := &config.HandlerConfig{
		LocalAddr: "localhost:8443",
		//ResolverTimeout: config.Duration(10 * time.Minute),
		ExternalAddr:            "https://localhost:8443",
		ResolverTimeout:         configutil.Duration(10 * time.Minute),
		ResolverNotFoundTimeout: configutil.Duration(10 * time.Second),
		ServerTLS: &loader.Config{
			Type: "DEV",
		},
		ClientTLS: &loader.Config{
			Type: "DEV",
		},
	}
	if err := config.LoadHandlerConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
		}
		defer loggerLoader.Close()
	}

	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		log.Fatalf("cannot create logger: %v", err)
	}
	if _logstash != nil {
		defer _logstash.Close()
	}

	if _logfile != nil {
		defer _logfile.Close()
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Logger() //.Output(output)
	var logger zLogger.ZLogger = &l2

	db, err := storage.NewConnection(&conf.Database)

	if err != nil {
		log.Fatal("Could not load the DB")
	}
	defer db.Close()

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	for _, domain := range conf.Domains {
		var domainPrefix string
		if domain != "" {
			domainPrefix = domain + "."
		}
		certutil.AddDefaultDNSNames(domainPrefix + pb.DispatcherHandlerService_ServiceDesc.ServiceName)
	}

	// create client TLS certificate
	// the certificate MUST contain "grpc:miniresolverproto.MiniResolver" or "*" in URIs
	lmct := logger.With().Str("service", "minresolver client loader").Logger()
	miniresolverClientTLSConfig, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, &lmct)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create client loader")
	}
	defer clientLoader.Close()

	serverTLSConfig, serverLoader, err := loader.CreateServerLoader(true, conf.ServerTLS, nil, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server loader")
	}
	defer serverLoader.Close()

	logger.Info().Msgf("resolver address is %s", conf.ResolverAddr)

	resolverClient, err := resolver.NewMiniresolverClient(conf.ResolverAddr, conf.GRPCClient, miniresolverClientTLSConfig, serverTLSConfig, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
	if err != nil {
		logger.Fatal().Msgf("cannot create resolver client: %v", err)
	}
	defer resolverClient.Close()

	// create grpc server with resolver for name resolution
	grpcServer, err := resolverClient.NewServer(conf.LocalAddr, conf.Domains, true)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}
	addr := grpcServer.GetAddr()
	l2 = _logger.With().Timestamp().Str("addr", addr).Logger() //.Output(output)
	logger = &l2

	tenantRepository := repository.NewTenantRepository(db, conf.Database.Schema)

	err = tenantRepository.CreatePreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for tenantRepository err: %v", err)
	}
	checkerRepository := repository.NewCheckerRepository(db, conf.Database.Schema)
	err = checkerRepository.CreatePreparedStatementsForChecker()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for checkerRepository err: %v", err)
	}
	objectRepository := repository.NewObjectRepository(db, conf.Database.Schema)
	err = objectRepository.CreateObjectPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for objectRepository err: %v", err)
	}
	dispatcherRepository := repository.NewDispatcherRepository(db, conf.Database.Schema)
	err = dispatcherRepository.CreateDispatcherPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for dispatcherRepository err: %v", err)
	}
	storageLocationRepository := repository.NewStorageLocationRepository(db, conf.Database.Schema)
	err = storageLocationRepository.CreateStorageLocPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for storageLocationRepository err: %v", err)
	}
	collectionRepository := repository.NewCollectionRepository(db, conf.Database.Schema)
	err = collectionRepository.CreateCollectionPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for collectionRepository err: %v", err)
	}
	objectInstanceRepository := repository.NewObjectInstanceRepository(db, conf.Database.Schema)
	err = objectInstanceRepository.CreateObjectInstancePreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for objectInstanceRepository err: %v", err)
	}
	objectInstanceService := service.NewObjectInstanceService(objectInstanceRepository)
	objectInstanceCheckRepository := repository.NewObjectInstanceCheckRepository(db, conf.Database.Schema)
	err = objectInstanceCheckRepository.CreateObjectInstanceCheckPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for objectInstanceCheckRepository err: %v", err)
	}
	_ = objectInstanceService
	storagePartitionRepository := repository.NewStoragePartitionRepository(db, conf.Database.Schema)
	err = storagePartitionRepository.CreateStoragePartitionPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for storagePartitionRepository err: %v", err)
	}
	storagePartitionService := service.StoragePartitionService{StoragePartitionRepository: storagePartitionRepository}

	fileRepository := repository.NewFileRepository(db, conf.Database.Schema)
	err = fileRepository.CreateFilePreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for fileRepository err: %v", err)
	}
	statusRepository := repository.NewStatusRepository(db, conf.Database.Schema)
	err = statusRepository.CreateStatusPreparedStatements()
	if err != nil {
		log.Fatalf("couldn't create prepared statements for statusRepository err: %v", err)
	}
	uploadService := service.UploaderServiceImpl{CollectionRepository: collectionRepository, TenantRepository: tenantRepository}
	_ = uploadService
	storageLocationService := service.NewStorageLocationService(collectionRepository, storageLocationRepository, storagePartitionService)
	_ = storageLocationService

	transactionRepository := repository.NewTransactionRepository(db, conf.Database.Schema)
	_ = transactionRepository
	refreshMaterializedViewRepository := repository.NewRefreshMaterializedViewsRepository(db, conf.Database.Schema)
	_ = refreshMaterializedViewRepository

	pb.RegisterDispatcherHandlerServiceServer(grpcServer, server.NewDispatcherHandlerServer(dispatcherRepository))
	/*
		//Listen StorageHandler, Dispatcher, Clerk
		lisHandler, err := net.Listen("tcp", conf.LocalAddr)
		if err != nil {
			panic(errors.Wrapf(err, "Failed to listen gRPC server"))
		}
		//grpcServerHandler := grpc.NewServer()
		pb.RegisterStorageHandlerHandlerServiceServer(grpcServer, &server.StorageHandlerHandlerServer{CollectionRepository: collectionRepository,
			ObjectRepository: objectRepository, StorageLocationRepository: storageLocationRepository, ObjectInstanceRepository: objectInstanceRepository,
			StoragePartitionService: storagePartitionService, FileRepository: fileRepository, StatusRepository: statusRepository, TransactionRepository: transactionRepository,
			RefreshMaterializedViewsRepository: refreshMaterializedViewRepository, Logger: logger})
		pb.RegisterClerkHandlerServiceServer(grpcServer, &server.ClerkHandlerServer{TenantService: service.NewTenantService(tenantRepository),
			CollectionRepository: collectionRepository, StorageLocationRepository: storageLocationRepository, ObjectRepository: objectRepository, ObjectInstanceRepository: objectInstanceRepository,
			FileRepository: fileRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository, StoragePartitionRepository: storagePartitionRepository, StatusRepository: statusRepository,
			ObjectInstanceService: objectInstanceService, TenantRepository: tenantRepository, StorageLocationService: storageLocationService, RefreshMaterializedViewsRepository: refreshMaterializedViewRepository})
		pb.RegisterUploaderHandlerServiceServer(grpcServer, &server.UploaderHandlerServer{UploaderService: &uploadService, TransactionRepository: transactionRepository,
			CollectionRepository: collectionRepository, StatusRepository: statusRepository, ObjectRepository: objectRepository, ObjectInstanceRepository: objectInstanceRepository})
		pb.RegisterCheckerHandlerServiceServer(grpcServer, &server.CheckerHandlerServer{ObjectInstanceRepository: objectInstanceRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository,
			StorageLocationRepository: storageLocationRepository, ObjectRepository: objectRepository})
		log.Printf("server started at %v", lisHandler.Addr())

	*/

	grpcServer.Startup()
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.GracefulStop()
}
