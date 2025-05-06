package service

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
)

func AfterConnectFunc(ctx context.Context, conn *pgx.Conn, logger zLogger.ZLogger) error {
	err := repository.CreateTenantPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateCollectionPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateObjectPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateObjectInstancePreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateFilePreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateObjectInstanceCheckPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateStorageLocPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateStoragePartitionPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateCheckerPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateDispatcherPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	err = repository.CreateStatusPreparedStatements(ctx, conn)
	if err != nil {
		return err
	}
	return nil
}
