package repository

import (
	"context"
	"emperror.dev/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ocfl-archive/dlza-manager/models"
	"time"
)

const (
	CreateStatus = "CreateStatus"
	AlterStatus  = "AlterStatus"
	CheckStatus  = "CheckStatus"
)

type StatusRepositoryImpl struct {
	Db *pgxpool.Pool
}

func NewStatusRepository(db *pgxpool.Pool) StatusRepository {
	return &StatusRepositoryImpl{Db: db}
}

func CreateStatusPreparedStatements(ctx context.Context, conn *pgx.Conn) error {
	preparedStatements := map[string]string{
		CreateStatus: "INSERT INTO archiving_status(status) values($1) RETURNING id",
		AlterStatus:  "UPDATE archiving_status set last_changed = $1, status = $2 where id =$3",
		CheckStatus:  "SELECT * FROM archiving_status where id = $1",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (s *StatusRepositoryImpl) CreateStatus(status models.ArchivingStatus) (string, error) {
	row := s.Db.QueryRow(context.Background(), CreateStatus, status.Status)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %v", CreateStatus)
	}
	return id, nil
}

func (s *StatusRepositoryImpl) AlterStatus(status models.ArchivingStatus) error {
	_, err := s.Db.Exec(context.Background(), AlterStatus, time.Now().Format("2006-01-02 15:04:05.000000"), status.Status, status.Id)
	if err != nil {
		return errors.Wrapf(err, "cannot update archiving status")
	}
	return nil
}

func (s *StatusRepositoryImpl) CheckStatus(id string) (models.ArchivingStatus, error) {
	var archivingStatus models.ArchivingStatus

	err := s.Db.QueryRow(context.Background(), CheckStatus, id).Scan(&archivingStatus.Id, &archivingStatus.LastChanged, &archivingStatus.Status)
	if err != nil {
		return archivingStatus, errors.Wrapf(err, "cannot get archiving status by id")
	}

	return archivingStatus, nil
}
