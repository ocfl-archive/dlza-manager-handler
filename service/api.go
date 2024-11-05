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
	return nil
}
