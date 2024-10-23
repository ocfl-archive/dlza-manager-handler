package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"time"
)

type statusPreparedStmt int

const (
	CreateStatus statusPreparedStmt = iota
	AlterStatus
	CheckStatus
)

type StatusRepositoryImpl struct {
	Db                 *sql.DB
	Schema             string
	PreparedStatements map[statusPreparedStmt]*sql.Stmt
}

func NewStatusRepository(db *sql.DB, schema string) StatusRepository {
	return &StatusRepositoryImpl{Db: db, Schema: schema}
}

func (s *StatusRepositoryImpl) CreateStatusPreparedStatements() error {
	preparedStatements := map[statusPreparedStmt]string{
		CreateStatus: fmt.Sprintf("INSERT INTO %s.archiving_status(status) values($1) RETURNING id", s.Schema),
		AlterStatus:  fmt.Sprintf("UPDATE %s.archiving_status set last_changed = $1, status = $2 where id =$3", s.Schema),
		CheckStatus:  fmt.Sprintf("SELECT * FROM %s.archiving_status where id = $1", s.Schema),
	}
	var err error
	s.PreparedStatements = make(map[statusPreparedStmt]*sql.Stmt)
	for key, stmt := range preparedStatements {
		s.PreparedStatements[key], err = s.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (s *StatusRepositoryImpl) CreateStatus(status models.ArchivingStatus) (string, error) {
	row := s.PreparedStatements[CreateStatus].QueryRow(status.Status)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[CreateStatus])
	}
	return id, nil
}

func (s *StatusRepositoryImpl) AlterStatus(status models.ArchivingStatus) error {
	_, err := s.PreparedStatements[AlterStatus].Exec(time.Now().Format("2006-01-02 15:04:05.000000"), status.Status, status.Id)
	if err != nil {
		return errors.Wrapf(err, "cannot update archiving status")
	}
	return nil
}

func (s *StatusRepositoryImpl) CheckStatus(id string) (models.ArchivingStatus, error) {
	var archivingStatus models.ArchivingStatus

	err := s.PreparedStatements[CheckStatus].QueryRow(id).Scan(&archivingStatus.Id, &archivingStatus.LastChanged, &archivingStatus.Status)
	if err != nil {
		return archivingStatus, errors.Wrapf(err, "cannot get archiving status by id")
	}

	return archivingStatus, nil
}
