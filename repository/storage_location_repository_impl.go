package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"strconv"
	"strings"
)

type storageLocationPrepareStmt int

const (
	GetStorageLocationsByTenantId storageLocationPrepareStmt = iota
	DeleteStorageLocationForTenantIdById
	SaveStorageLocationForTenant
	GetStorageLocationById
	GetStorageLocationByObjectInstanceId
	GetStorageLocationsByObjectId
	GetAmountOfErrorsForStorageLocationId
	GetAmountOfObjectsForStorageLocationId
)

func (s *StorageLocationRepositoryImpl) CreateStorageLocPreparedStatements() error {
	preparedStatements := map[storageLocationPrepareStmt]string{
		GetStorageLocationsByTenantId: fmt.Sprintf("SELECT * FROM %s.storage_location where tenant_id = $1", s.Schema),
		GetStorageLocationByObjectInstanceId: strings.Replace("select sl.* from %s.object_instance oi "+
			" inner join %s.storage_partition sp"+
			" on sp.id = oi.storage_partition_id"+
			" inner join %s.storage_location sl"+
			" on sl.id = sp.storage_location_id where oi.id = $1", "%s", s.Schema, -1),
		GetStorageLocationById: strings.Replace("select a.*, c.total_existing_volume from (select sl.*, sum(oi.size) as total_file_size from %s.storage_location sl"+
			" inner join %s.storage_partition sp on sp.storage_location_id = sl.id"+
			" inner join %s.object_instance oi on sp.id = oi.storage_partition_id"+
			" where sl.id = $1 group by sl.id) a"+
			" inner join"+
			" (select sp.storage_location_id, sum(sp.max_size) as total_existing_volume from %s.storage_partition sp group by sp.storage_location_id) c"+
			" on a.id = c.storage_location_id", "%s", s.Schema, -1),
		DeleteStorageLocationForTenantIdById: fmt.Sprintf("DELETE FROM %s.storage_location WHERE id = $1", s.Schema),
		SaveStorageLocationForTenant:         fmt.Sprintf("INSERT INTO %s.storage_location(alias, type, vault, connection, quality, price, security_compliency, fill_first, ocfl_type, tenant_id, number_of_threads) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)  ", s.Schema),
		GetStorageLocationsByObjectId: strings.Replace("select sl.* from %s.object o,"+
			" %s.object_instance oi,"+
			" %s.storage_partition sp,"+
			" %s.storage_location sl"+
			" where o.id = $1"+
			" and o.id = oi.object_id"+
			" and oi.storage_partition_id = sp.id"+
			" and sp.storage_location_id = sl.id", "%s", s.Schema, -1),
		GetAmountOfErrorsForStorageLocationId: strings.Replace("select count(*) from %s.object_instance oi, %s.storage_partition sp, %s.storage_location sl"+
			" where oi.storage_partition_id = sp.id"+
			" and sp.storage_location_id = sl.id"+
			" and status = 'error'"+
			" and sl.id = $1", "%s", s.Schema, -1),
		GetAmountOfObjectsForStorageLocationId: strings.Replace("select count(*) from %s.object_instance oi, %s.storage_partition sp, %s.storage_location sl"+
			" where oi.storage_partition_id = sp.id"+
			" and sp.storage_location_id = sl.id"+
			" and sl.id = $1", "%s", s.Schema, -1),
	}
	var err error
	s.PreparedStatements = make(map[storageLocationPrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatements {
		s.PreparedStatements[key], err = s.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

type StorageLocationRepositoryImpl struct {
	Db                 *sql.DB
	Schema             string
	PreparedStatements map[storageLocationPrepareStmt]*sql.Stmt
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByTenantId(tenantId string) ([]models.StorageLocation, error) {
	rows, err := s.PreparedStatements[GetStorageLocationsByTenantId].Query(tenantId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[GetStorageLocationsByTenantId])
	}
	var storageLocations []models.StorageLocation

	for rows.Next() {
		var storageLocation models.StorageLocation
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &storageLocation.Vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", s.PreparedStatements[GetStorageLocationsByTenantId])
		}
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, nil
}

func (s *StorageLocationRepositoryImpl) GetAmountOfErrorsForStorageLocationId(id string) (int, error) {
	return s.getOneNumberParameterById(id, GetAmountOfErrorsForStorageLocationId)
}

func (s *StorageLocationRepositoryImpl) GetAmountOfObjectsForStorageLocationId(id string) (int, error) {
	return s.getOneNumberParameterById(id, GetAmountOfObjectsForStorageLocationId)
}

func (s *StorageLocationRepositoryImpl) DeleteStorageLocationById(storageLocationId string) error {
	_, err := s.PreparedStatements[DeleteStorageLocationForTenantIdById].Exec(storageLocationId)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[DeleteStorageLocationForTenantIdById])
	}
	return nil
}

func (s *StorageLocationRepositoryImpl) SaveStorageLocation(storageLocation models.StorageLocation) error {
	_, err := s.PreparedStatements[SaveStorageLocationForTenant].Exec(storageLocation.Alias, storageLocation.Type, storageLocation.Vault, storageLocation.Connection, storageLocation.Quality,
		storageLocation.Price, storageLocation.SecurityCompliency, storageLocation.FillFirst, storageLocation.OcflType, storageLocation.TenantId, storageLocation.NumberOfThreads)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[SaveStorageLocationForTenant])
	}
	return nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationById(id string) (models.StorageLocation, error) {
	var storageLocation models.StorageLocation
	err := s.PreparedStatements[GetStorageLocationById].QueryRow(id).Scan(&storageLocation.Alias, &storageLocation.Type, &storageLocation.Vault, &storageLocation.Connection, &storageLocation.Quality,
		&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads, &storageLocation.TotalFilesSize, &storageLocation.TotalExistingVolume)
	if err != nil {
		return models.StorageLocation{}, errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[GetStorageLocationById])
	}
	return storageLocation, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByObjectId(id string) ([]models.StorageLocation, error) {
	rows, err := s.PreparedStatements[GetStorageLocationsByObjectId].Query(id)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get current storage locations")
	}

	var storageLocations []models.StorageLocation
	for rows.Next() {
		var storageLocation models.StorageLocation
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &storageLocation.Vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", s.PreparedStatements[GetStorageLocationsByObjectId])
		}
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByTenantIdPaginated(pagination models.Pagination) ([]models.StorageLocation, int, error) {
	tenantStatement := ""
	likeStatement := getLikeQueryForStorageLocation(pagination.SearchField)
	collectionStatement := ""

	// tenantID filter
	if pagination.Id == "" {
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			tenantStatement = fmt.Sprintf(" where sl.tenant_id in ('%s')", tenants)
		}
	} else {
		tenantStatement = fmt.Sprintf(" where sl.tenant_id = '%s'", pagination.Id)
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			tenantStatement = tenantStatement + fmt.Sprintf(" and sl.tenant_id in ('%s')", tenants)
		}
	}
	if tenantStatement == "" {
		likeStatement = " where" + likeStatement
	} else {
		likeStatement = " and" + likeStatement
	}
	// collectionID filter
	if pagination.SecondId != "" {
		collectionStatement = fmt.Sprintf("where c.id = '%s'", pagination.SecondId)
	}

	query := strings.Replace(fmt.Sprintf("select a.*, d.total_files_size, count(*) over() as total_items  from"+
		" (select sl.*, sum(sp.max_size) as total_existing_volume from _schema.storage_location sl"+
		" inner join _schema.storage_partition sp"+
		" on sl.id = sp.storage_location_id "+
		" %s %s"+
		" group by sl.id) a"+
		" inner join"+
		" (select b.storage_location_id, sum(total_files_size_for_instance) as total_files_size"+
		" from (select sum(oi.size) as total_files_size_for_instance, sp.id as spid, sp.storage_location_id"+
		" from _schema.storage_partition sp"+
		" inner join _schema.object_instance oi"+
		" on sp.id = oi.storage_partition_id"+
		" inner join _schema.object o"+
		" on o.id = oi.object_id"+
		" inner join _schema.collection c"+
		" on c.id = o.collection_id"+
		" %s"+
		" group by sp.id) b"+
		" group by storage_location_id) d"+
		" on a.id = d.storage_location_id"+
		" order by %s %s limit %s OFFSET %s ", tenantStatement, likeStatement, collectionStatement, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", s.Schema, -1)
	rows, err := s.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	var storageLocations []models.StorageLocation
	var totalItems int
	for rows.Next() {
		var storageLocation models.StorageLocation
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &storageLocation.Vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId,
			&storageLocation.Id, &storageLocation.NumberOfThreads,
			&storageLocation.TotalExistingVolume, &storageLocation.TotalFilesSize, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, totalItems, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByCollectionIdPaginated(pagination models.Pagination) ([]models.StorageLocation, int, error) {

	query := strings.Replace(fmt.Sprintf("select sl.* from _schema.collection c,"+
		" _schema.object o,"+
		" _schema.object_instance oi,"+
		" _schema.storage_partition sp,"+
		" _schema.storage_location sl"+
		" where c.id = o.collection_id"+
		" and o.id = oi.object_id"+
		" and oi.storage_partition_id = sp.id"+
		" and sl.id = sp.storage_location_id"+
		" and c.id = $1"+
		" group by sl.id"+
		" order by %s %s limit %s OFFSET %s ", pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", s.Schema, -1)
	rows, err := s.Db.Query(query, pagination.SecondId)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var totalItems int
	storageLocations := make([]models.StorageLocation, 0)
	for rows.Next() {
		storageLocation := models.StorageLocation{}
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &storageLocation.Vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, totalItems, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationByObjectInstanceId(id string) (models.StorageLocation, error) {
	var storageLocation models.StorageLocation
	err := s.PreparedStatements[GetStorageLocationByObjectInstanceId].QueryRow(id).Scan(&storageLocation.Alias, &storageLocation.Type, &storageLocation.Vault, &storageLocation.Connection, &storageLocation.Quality,
		&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads)
	if err != nil {
		return models.StorageLocation{}, errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[GetStorageLocationByObjectInstanceId])
	}
	return storageLocation, nil
}

func NewStorageLocationRepository(db *sql.DB, schema string) StorageLocationRepository {
	return &StorageLocationRepositoryImpl{Db: db, Schema: schema}
}

func getLikeQueryForStorageLocation(searchKey string) string {
	return strings.Replace("(sl.id::text like '_search_key_%' or lower(sl.alias) like '%_search_key_%' or lower(sl.security_compliency) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}

func (s *StorageLocationRepositoryImpl) getOneNumberParameterById(id string, preparedStatement storageLocationPrepareStmt) (int, error) {
	row := s.PreparedStatements[preparedStatement].QueryRow(id)
	var amount int
	err := row.Scan(&amount)
	if err != nil {
		return amount, errors.Wrapf(err, "Could not execute query: %v", s.PreparedStatements[preparedStatement])
	}
	return amount, nil
}
