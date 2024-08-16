package repository

import (
	"database/sql"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"github.com/pkg/errors"
	"slices"
	"strconv"
	"strings"
)

type storagePartitionRepositoryStmt int

const (
	GetStoragePartition storagePartitionRepositoryStmt = iota
	CreateStoragePartition
	UpdateStoragePartition
	DeleteStoragePartition
	GetStoragePartitionsByLocationId
)

type storagePartitionRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[storagePartitionRepositoryStmt]*sql.Stmt
}

func (s *storagePartitionRepositoryImpl) CreateStoragePartitionPreparedStatements() error {

	preparedStatement := map[storagePartitionRepositoryStmt]string{
		GetStoragePartition:              fmt.Sprintf("SELECT * FROM %s.STORAGE_PARTITION o WHERE ID = $1", s.Schema),
		CreateStoragePartition:           fmt.Sprintf("INSERT INTO %s.STORAGE_PARTITION(alias, \"name\", max_size, max_objects, current_size, current_objects, storage_location_id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id", s.Schema),
		UpdateStoragePartition:           fmt.Sprintf("UPDATE %s.STORAGE_PARTITION set name = $1, max_size = $2, max_objects = $3, current_size = $4, current_objects = $5, alias = $6 where id =$7", s.Schema),
		DeleteStoragePartition:           fmt.Sprintf("DELETE FROM %s.STORAGE_PARTITION  where id =$1", s.Schema),
		GetStoragePartitionsByLocationId: fmt.Sprintf("SELECT * FROM %s.STORAGE_PARTITION WHERE storage_location_id = $1", s.Schema),
	}
	var err error
	s.PreparedStatement = make(map[storagePartitionRepositoryStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		s.PreparedStatement[key], err = s.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) CreateStoragePartition(partition models.StoragePartition) (string, error) {
	row := s.PreparedStatement[CreateStoragePartition].QueryRow(partition.Alias, partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.StorageLocationId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatement[CreateStoragePartition])
	}
	return id, nil
}

func (s *storagePartitionRepositoryImpl) UpdateStoragePartition(partition models.StoragePartition) error {
	_, err := s.PreparedStatement[UpdateStoragePartition].Exec(partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.Alias, partition.Id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatement[UpdateStoragePartition])
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) DeleteStoragePartitionById(id string) error {
	_, err := s.PreparedStatement[DeleteStoragePartition].Exec(id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatement[DeleteStoragePartition])
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionById(id string) (models.StoragePartition, error) {
	storagePartition := models.StoragePartition{}
	err := s.PreparedStatement[GetStoragePartition].QueryRow(id).Scan(&storagePartition.Alias, &storagePartition.Name, &storagePartition.MaxSize,
		&storagePartition.MaxObjects, &storagePartition.CurrentSize, &storagePartition.CurrentObjects, &storagePartition.Id, &storagePartition.StorageLocationId)
	return storagePartition, err
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionsByLocationId(locationId string) ([]models.StoragePartition, error) {
	rows, err := s.PreparedStatement[GetStoragePartitionsByLocationId].Query(locationId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatement[GetStoragePartitionsByLocationId])
	}
	var storagePartitions []models.StoragePartition

	for rows.Next() {
		var storagePartition models.StoragePartition
		err := rows.Scan(&storagePartition.Alias, &storagePartition.Name, &storagePartition.MaxSize, &storagePartition.MaxObjects, &storagePartition.CurrentSize,
			&storagePartition.CurrentObjects, &storagePartition.Id, &storagePartition.StorageLocationId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", s.PreparedStatement[GetStoragePartitionsByLocationId])
		}
		storagePartitions = append(storagePartitions, storagePartition)
	}
	return storagePartitions, nil
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionsByLocationIdPaginated(pagination models.Pagination) ([]models.StoragePartition, int, error) {
	firstCondition := ""
	secondCondition := ""
	if pagination.SecondId != "" {
		if len(pagination.AllowedTenants) != 0 && slices.Contains(pagination.AllowedTenants, pagination.SecondId) {
			pagination.AllowedTenants = []string{pagination.SecondId}
		}
		if len(pagination.AllowedTenants) == 0 {
			pagination.AllowedTenants = []string{pagination.SecondId}
		}
	}
	if pagination.Id == "" {
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			firstCondition = fmt.Sprintf("where t.id in ('%s')", tenants)
		}
	} else {
		firstCondition = fmt.Sprintf("where sp.storage_location_id = '%s'", pagination.Id)
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			secondCondition = fmt.Sprintf("and t.id in ('%s')", tenants)
		}
	}
	if firstCondition == "" && secondCondition == "" {
		firstCondition = "where"
	} else {
		secondCondition = secondCondition + " and"
	}
	query := strings.Replace(fmt.Sprintf("SELECT sp.*, count(*) over() as total_items FROM _schema.STORAGE_PARTITION sp"+
		" inner join _schema.storage_location sl on sl.id = sp.storage_location_id"+
		" inner join _schema.tenant t on t.id = sl.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForStoragePartition(pagination.SearchField), "sp."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", s.Schema, -1)
	rows, err := s.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var storagePartitions []models.StoragePartition
	var totalItems int
	for rows.Next() {
		var storagePartition models.StoragePartition
		err := rows.Scan(&storagePartition.Alias, &storagePartition.Name, &storagePartition.MaxSize,
			&storagePartition.MaxObjects, &storagePartition.CurrentSize, &storagePartition.CurrentObjects, &storagePartition.Id, &storagePartition.StorageLocationId, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		storagePartitions = append(storagePartitions, storagePartition)
	}
	return storagePartitions, totalItems, nil
}

func NewStoragePartitionRepository(db *sql.DB, schema string) StoragePartitionRepository {
	return &storagePartitionRepositoryImpl{Db: db, Schema: schema}
}

func getLikeQueryForStoragePartition(searchKey string) string {
	return strings.Replace("(sp.id::text like '_search_key_%' or lower(sp.alias) like '%_search_key_%'"+
		" or lower(sp.name) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
