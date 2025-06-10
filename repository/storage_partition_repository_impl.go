package repository

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ocfl-archive/dlza-manager/models"
	"slices"
	"strconv"
	"strings"
)

const (
	GetStoragePartition                                    = "GetStoragePartition"
	CreateStoragePartition                                 = "CreateStoragePartition"
	UpdateStoragePartition                                 = "UpdateStoragePartition"
	DeleteStoragePartition                                 = "DeleteStoragePartition"
	GetStoragePartitionsByLocationId                       = "GetStoragePartitionsByLocationId"
	GetStoragePartitionByObjectSignatureAndLocation        = "GetStoragePartitionByObjectSignatureAndLocation"
	CreateStoragePartitionGroupElement                     = "CreateStoragePartitionGroupElement"
	UpdateStoragePartitionGroupElement                     = "UpdateStoragePartitionGroupElement"
	DeleteStoragePartitionGroupElementByStoragePartitionId = "DeleteStoragePartitionGroupElementByStoragePartitionId"
	GetStoragePartitionGroupElementByAlias                 = "GetStoragePartitionGroupElementByAlias"
	GetStoragePartitionGroupElementsByStoragePartitionId   = "GetStoragePartitionGroupElementsByStoragePartitionId"
)

type storagePartitionRepositoryImpl struct {
	Db *pgxpool.Pool
}

func CreateStoragePartitionPreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	preparedStatements := map[string]string{
		GetStoragePartition:                                    "SELECT * FROM STORAGE_PARTITION o WHERE ID = $1",
		GetStoragePartitionGroupElementByAlias:                 "SELECT * FROM STORAGE_PARTITION_GROUP_ELEM o WHERE alias = $1",
		GetStoragePartitionByObjectSignatureAndLocation:        `SELECT sp.* FROM object o INNER JOIN object_instance oi ON o.id = oi.object_id INNER JOIN storage_partition sp ON oi.storage_partition_id = sp.id WHERE signature = $1 AND storage_location_id = $2 AND (oi.status = 'ok' or oi.status = 'new')`,
		CreateStoragePartition:                                 "INSERT INTO STORAGE_PARTITION(alias, \"name\", max_size, max_objects, current_size, current_objects, storage_location_id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
		UpdateStoragePartition:                                 "UPDATE STORAGE_PARTITION set name = $1, max_size = $2, max_objects = $3, current_size = $4, current_objects = $5, alias = $6 where id =$7",
		DeleteStoragePartition:                                 "DELETE FROM STORAGE_PARTITION  where id =$1",
		GetStoragePartitionsByLocationId:                       "SELECT * FROM STORAGE_PARTITION WHERE storage_location_id = $1",
		CreateStoragePartitionGroupElement:                     "INSERT INTO STORAGE_PARTITION_GROUP_ELEM(alias, \"name\", partition_group_id) VALUES ($1, $2, $3) RETURNING id",
		UpdateStoragePartitionGroupElement:                     "UPDATE STORAGE_PARTITION_GROUP_ELEM set name = $1, alias = $2 where id =$3",
		DeleteStoragePartitionGroupElementByStoragePartitionId: "DELETE FROM STORAGE_PARTITION_GROUP_ELEM  where partition_group_id =$1",
		GetStoragePartitionGroupElementsByStoragePartitionId:   "SELECT * FROM STORAGE_PARTITION_GROUP_ELEM WHERE partition_group_id =$1",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionByObjectSignatureAndLocation(signature string, locationId string) (models.StoragePartition, error) {
	storagePartition := models.StoragePartition{}
	err := s.Db.QueryRow(context.Background(), GetStoragePartitionByObjectSignatureAndLocation, signature, locationId).Scan(&storagePartition.Alias, &storagePartition.Name, &storagePartition.MaxSize,
		&storagePartition.MaxObjects, &storagePartition.CurrentSize, &storagePartition.CurrentObjects, &storagePartition.Id, &storagePartition.StorageLocationId)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return storagePartition, errors.Wrapf(err, "Could not execute query for method: %v", GetStoragePartitionByObjectSignatureAndLocation)
	}
	return storagePartition, nil
}

func (s *storagePartitionRepositoryImpl) CreateStoragePartition(partition models.StoragePartition) (string, error) {
	row := s.Db.QueryRow(context.Background(), CreateStoragePartition, partition.Alias, partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.StorageLocationId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %v", CreateStoragePartition)
	}
	return id, nil
}

func (s *storagePartitionRepositoryImpl) CreateStoragePartitionGroupElement(partitionGroup models.StoragePartitionGroup) (string, error) {
	row := s.Db.QueryRow(context.Background(), CreateStoragePartitionGroupElement, partitionGroup.Alias, partitionGroup.Name, partitionGroup.PartitionGroupId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %v", CreateStoragePartitionGroupElement)
	}
	return id, nil
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionGroupElementsByStoragePartitionId(partitionGroupId string) ([]models.StoragePartitionGroup, error) {
	rows, err := s.Db.Query(context.Background(), GetStoragePartitionGroupElementsByStoragePartitionId, partitionGroupId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetStoragePartitionGroupElementsByStoragePartitionId)
	}
	defer rows.Close()
	var storagePartitionGroups []models.StoragePartitionGroup

	for rows.Next() {
		var storagePartition models.StoragePartitionGroup
		err := rows.Scan(&storagePartition.PartitionGroupId, &storagePartition.Alias, &storagePartition.Id, &storagePartition.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query in method: %v", GetStoragePartitionGroupElementsByStoragePartitionId)
		}
		storagePartitionGroups = append(storagePartitionGroups, storagePartition)
	}
	return storagePartitionGroups, nil
}

func (s *storagePartitionRepositoryImpl) UpdateStoragePartitionGroupElement(partitionGroup models.StoragePartitionGroup) error {
	_, err := s.Db.Exec(context.Background(), UpdateStoragePartitionGroupElement, partitionGroup.Name, partitionGroup.Alias, partitionGroup.Id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", UpdateStoragePartitionGroupElement)
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) UpdateStoragePartition(partition models.StoragePartition) error {
	_, err := s.Db.Exec(context.Background(), UpdateStoragePartition, partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.Alias, partition.Id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", UpdateStoragePartition)
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) DeleteStoragePartitionById(id string) error {
	_, err := s.Db.Exec(context.Background(), DeleteStoragePartition, id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", DeleteStoragePartition)
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) DeleteStoragePartitionGroupElementByStoragePartitionId(partitionId string) error {
	_, err := s.Db.Exec(context.Background(), DeleteStoragePartitionGroupElementByStoragePartitionId, partitionId)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", DeleteStoragePartitionGroupElementByStoragePartitionId)
	}
	return nil
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionGroupElementByAlias(alias string) (models.StoragePartitionGroup, error) {
	storagePartitionGroup := models.StoragePartitionGroup{}
	err := s.Db.QueryRow(context.Background(), GetStoragePartitionGroupElementByAlias, alias).Scan(&storagePartitionGroup.PartitionGroupId, &storagePartitionGroup.Alias,
		&storagePartitionGroup.Id, &storagePartitionGroup.Name)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return storagePartitionGroup, errors.Wrapf(err, "Could not execute query for method: %v", GetStoragePartitionGroupElementByAlias)
	}
	return storagePartitionGroup, nil
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionById(id string) (models.StoragePartition, error) {
	storagePartition := models.StoragePartition{}
	err := s.Db.QueryRow(context.Background(), GetStoragePartition, id).Scan(&storagePartition.Alias, &storagePartition.Name, &storagePartition.MaxSize,
		&storagePartition.MaxObjects, &storagePartition.CurrentSize, &storagePartition.CurrentObjects, &storagePartition.Id, &storagePartition.StorageLocationId)
	if err != nil {
		return storagePartition, errors.Wrapf(err, "Could not execute query for method: %v", GetStoragePartition)
	}
	return storagePartition, err
}

func (s *storagePartitionRepositoryImpl) GetStoragePartitionsByLocationId(locationId string) ([]models.StoragePartition, error) {
	rows, err := s.Db.Query(context.Background(), GetStoragePartitionsByLocationId, locationId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetStoragePartitionsByLocationId)
	}
	defer rows.Close()
	var storagePartitions []models.StoragePartition

	for rows.Next() {
		var storagePartition models.StoragePartition
		err := rows.Scan(&storagePartition.Alias, &storagePartition.Name, &storagePartition.MaxSize, &storagePartition.MaxObjects, &storagePartition.CurrentSize,
			&storagePartition.CurrentObjects, &storagePartition.Id, &storagePartition.StorageLocationId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query in method: %v", GetStoragePartitionsByLocationId)
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
	query := fmt.Sprintf("SELECT sp.*, count(*) over() as total_items FROM STORAGE_PARTITION sp"+
		" inner join storage_location sl on sl.id = sp.storage_location_id"+
		" inner join tenant t on t.id = sl.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForStoragePartition(pagination.SearchField), "sp."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := s.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	defer rows.Close()
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

func NewStoragePartitionRepository(db *pgxpool.Pool) StoragePartitionRepository {
	return &storagePartitionRepositoryImpl{Db: db}
}

func getLikeQueryForStoragePartition(searchKey string) string {
	return strings.Replace("(sp.id::text like '_search_key_%' or lower(sp.alias) like '%_search_key_%'"+
		" or lower(sp.name) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
