package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/trustutil/v2/pkg/certutil"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-handler/config"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager-handler/server"
	"github.com/ocfl-archive/dlza-manager-handler/service"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"go.ub.unibas.ch/cloud/miniresolver/v2/pkg/resolver"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var configfile = flag.String("config", "", "config file in toml format")

type queryTracer struct {
	log zLogger.ZLogger
}

func (tracer *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.log.Debug().Msgf("postgreSQL command start '%s' - %v", data.SQL, data.Args)
	return ctx
}

func (tracer *queryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if data.Err != nil {
		tracer.log.Error().Err(data.Err).Msgf("postgreSQL command error")
		return
	}
	tracer.log.Debug().Msgf("postgreSQL command end: %s (%d)", data.CommandTag.String(), data.CommandTag.RowsAffected())
}

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

	pgxConf, err := pgxpool.ParseConfig(string(conf.DBConn))
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot parse db connection string")
	}
	// create prepared queries on each connection
	pgxConf.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return service.AfterConnectFunc(ctx, conn, logger)
	}
	pgxConf.BeforeConnect = func(ctx context.Context, cfg *pgx.ConnConfig) error {
		cfg.Tracer = &queryTracer{log: logger}
		return nil
	}
	var conn *pgxpool.Pool
	var dbstrRegexp = regexp.MustCompile(`^postgres://postgres:([^@]+)@.+$`)
	pws := dbstrRegexp.FindStringSubmatch(string(conf.DBConn))
	if len(pws) == 2 {
		logger.Info().Msgf("connecting to database: %s", strings.Replace(string(conf.DBConn), pws[1], "xxxxxxxx", -1))
	} else {
		logger.Info().Msgf("connecting to database")
	}
	conn, err = pgxpool.NewWithConfig(context.Background(), pgxConf)
	if err != nil {
		logger.Fatal().Err(err).Msgf("cannot connect to database: %s", conf.DBConn)
	}
	defer conn.Close()

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

	tenantRepository := repository.NewTenantRepository(conn)
	collectionRepository := repository.NewCollectionRepository(conn)
	objectRepository := repository.NewObjectRepository(conn)
	objectInstanceRepository := repository.NewObjectInstanceRepository(conn)
	fileRepository := repository.NewFileRepository(conn)
	objectInstanceCheckRepository := repository.NewObjectInstanceCheckRepository(conn)
	storageLocationRepository := repository.NewStorageLocationRepository(conn)
	storagePartitionRepository := repository.NewStoragePartitionRepository(conn)
	//checkerRepository := repository.NewCheckerRepository(conn)
	dispatcherRepository := repository.NewDispatcherRepository(conn)
	statusRepository := repository.NewStatusRepository(conn)
	refreshMaterializedViewRepository := repository.NewRefreshMaterializedViewsRepository(conn)
	transactionRepository := repository.NewTransactionRepository(conn)

	objectInstanceService := service.NewObjectInstanceService(objectInstanceRepository)

	storagePartitionService := service.StoragePartitionService{StoragePartitionRepository: storagePartitionRepository}

	uploadService := service.NewUploaderService(tenantRepository, collectionRepository)
	storageLocationService := service.NewStorageLocationService(collectionRepository, storageLocationRepository, storagePartitionService)

	pb.RegisterDispatcherHandlerServiceServer(grpcServer, server.NewDispatcherHandlerServer(dispatcherRepository))
	pb.RegisterStorageHandlerHandlerServiceServer(grpcServer, &server.StorageHandlerHandlerServer{CollectionRepository: collectionRepository,
		ObjectRepository: objectRepository, StorageLocationRepository: storageLocationRepository, ObjectInstanceRepository: objectInstanceRepository,
		StoragePartitionService: storagePartitionService, FileRepository: fileRepository, StatusRepository: statusRepository, TransactionRepository: transactionRepository,
		RefreshMaterializedViewsRepository: refreshMaterializedViewRepository, UploaderService: uploadService, Logger: logger})
	pb.RegisterClerkHandlerServiceServer(grpcServer, &server.ClerkHandlerServer{TenantService: service.NewTenantService(tenantRepository),
		CollectionRepository: collectionRepository, StorageLocationRepository: storageLocationRepository, ObjectRepository: objectRepository, ObjectInstanceRepository: objectInstanceRepository,
		FileRepository: fileRepository, ObjectInstanceCheckRepository: objectInstanceCheckRepository, StoragePartitionRepository: storagePartitionRepository, StatusRepository: statusRepository,
		ObjectInstanceService: objectInstanceService, TenantRepository: tenantRepository, StorageLocationService: storageLocationService, RefreshMaterializedViewsRepository: refreshMaterializedViewRepository})
	/*
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
