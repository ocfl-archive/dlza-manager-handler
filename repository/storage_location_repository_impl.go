package repository

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"emperror.dev/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ocfl-archive/dlza-manager/models"
)

const (
	GetAllStorageLocations                 = "GetAllStorageLocations"
	GetStorageLocationsByTenantId          = "GetStorageLocationsByTenantId"
	DeleteStorageLocationForTenantIdById   = "DeleteStorageLocationForTenantIdById"
	UpdateStorageLocation                  = "UpdateStorageLocation"
	SaveStorageLocationForTenant           = "SaveStorageLocationForTenant"
	GetStorageLocationById                 = "GetStorageLocationById"
	GetStorageLocationByObjectInstanceId   = "GetStorageLocationByObjectInstanceId"
	GetStorageLocationsByObjectId          = "GetStorageLocationsByObjectId"
	GetAmountOfErrorsForStorageLocationId  = "GetAmountOfErrorsForStorageLocationId"
	GetAmountOfObjectsForStorageLocationId = "GetAmountOfObjectsForStorageLocationId"
)

func CreateStorageLocPreparedStatements(ctx context.Context, conn *pgx.Conn) error {
	preparedStatements := map[string]string{
		GetAllStorageLocations:        "SELECT * FROM storage_location",
		GetStorageLocationsByTenantId: "SELECT * FROM storage_location where tenant_id = $1",
		GetStorageLocationByObjectInstanceId: "select sl.* from object_instance oi " +
			" inner join storage_partition sp" +
			" on sp.id = oi.storage_partition_id" +
			" inner join storage_location sl" +
			" on sl.id = sp.storage_location_id where oi.id = $1",
		GetStorageLocationById: "select a.*, c.total_existing_volume from (select sl.*, sum(oi.size) as total_file_size from storage_location sl" +
			" left join storage_partition sp on sp.storage_location_id = sl.id" +
			" left join object_instance oi on sp.id = oi.storage_partition_id" +
			" where sl.id = $1 group by sl.id) a" +
			" left join" +
			" (select sp.storage_location_id, sum(sp.max_size) as total_existing_volume from storage_partition sp group by sp.storage_location_id) c" +
			" on a.id = c.storage_location_id",
		DeleteStorageLocationForTenantIdById: "DELETE FROM storage_location WHERE id = $1",
		SaveStorageLocationForTenant:         "INSERT INTO storage_location(alias, type, vault, connection, quality, price, security_compliency, fill_first, ocfl_type, tenant_id, number_of_threads, group) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)  RETURNING id",
		GetStorageLocationsByObjectId: "select sl.* from object o," +
			" object_instance oi," +
			" storage_partition sp," +
			" storage_location sl" +
			" where o.id = $1" +
			" and o.id = oi.object_id" +
			" and oi.storage_partition_id = sp.id" +
			" and sp.storage_location_id = sl.id",
		GetAmountOfErrorsForStorageLocationId: "select count(*) from object_instance oi, storage_partition sp, storage_location sl" +
			" where oi.storage_partition_id = sp.id" +
			" and sp.storage_location_id = sl.id" +
			" and status = 'error'" +
			" and sl.id = $1",
		GetAmountOfObjectsForStorageLocationId: "select count(*) from object_instance oi, storage_partition sp, storage_location sl" +
			" where oi.storage_partition_id = sp.id" +
			" and sp.storage_location_id = sl.id" +
			" and sl.id = $1",
		UpdateStorageLocation: "UPDATE STORAGE_LOCATION set alias = $1, type = $2, vault = $3, connection = $4, quality = $5, price = $6, security_compliency = $7, fill_first = $8, ocfl_type = $9, tenant_id = $10, number_of_threads = $12, group = $13 where id =$11",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

type StorageLocationRepositoryImpl struct {
	Db *pgxpool.Pool
}

func (s *StorageLocationRepositoryImpl) GetAllStorageLocations() ([]models.StorageLocation, error) {
	rows, err := s.Db.Query(context.Background(), GetAllStorageLocations)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetAllStorageLocations)
	}
	defer rows.Close()
	return getStorageLocationsFromRows(rows)
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByTenantId(tenantId string) ([]models.StorageLocation, error) {
	rows, err := s.Db.Query(context.Background(), GetStorageLocationsByTenantId, tenantId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetStorageLocationsByTenantId)
	}
	defer rows.Close()
	return getStorageLocationsFromRows(rows)
}

func (s *StorageLocationRepositoryImpl) GetAmountOfErrorsForStorageLocationId(id string) (int, error) {
	return s.getOneNumberParameterById(id, GetAmountOfErrorsForStorageLocationId)
}

func (s *StorageLocationRepositoryImpl) GetAmountOfObjectsForStorageLocationId(id string) (int, error) {
	return s.getOneNumberParameterById(id, GetAmountOfObjectsForStorageLocationId)
}

func (s *StorageLocationRepositoryImpl) DeleteStorageLocationById(storageLocationId string) error {
	_, err := s.Db.Exec(context.Background(), DeleteStorageLocationForTenantIdById, storageLocationId)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", DeleteStorageLocationForTenantIdById)
	}
	return nil
}

func (s *StorageLocationRepositoryImpl) SaveStorageLocation(storageLocation models.StorageLocation) (string, error) {
	row := s.Db.QueryRow(context.Background(), SaveStorageLocationForTenant, storageLocation.Alias, storageLocation.Type, storageLocation.Vault, storageLocation.Connection, storageLocation.Quality,
		storageLocation.Price, storageLocation.SecurityCompliency, storageLocation.FillFirst, storageLocation.OcflType, storageLocation.TenantId, storageLocation.NumberOfThreads, storageLocation.Group)
	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %v", SaveStorageLocationForTenant)
	}
	return id, nil
}

func (s *StorageLocationRepositoryImpl) UpdateStorageLocation(storageLocation models.StorageLocation) error {
	_, err := s.Db.Exec(context.Background(), UpdateStorageLocation, storageLocation.Alias, storageLocation.Type, storageLocation.Vault, storageLocation.Connection, storageLocation.Quality,
		storageLocation.Price, storageLocation.SecurityCompliency, storageLocation.FillFirst, storageLocation.OcflType, storageLocation.TenantId, storageLocation.Id, storageLocation.NumberOfThreads, storageLocation.Group)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", UpdateStorageLocation)
	}
	return nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationById(id string) (models.StorageLocation, error) {
	var storageLocation models.StorageLocation
	var vault zeronull.Text
	var totalExistingVolume zeronull.Int8
	var totalFilesSize zeronull.Int8
	err := s.Db.QueryRow(context.Background(), GetStorageLocationById, id).Scan(&storageLocation.Alias, &storageLocation.Type, &vault, &storageLocation.Connection, &storageLocation.Quality,
		&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads, &storageLocation.Group, &totalFilesSize, &totalExistingVolume)
	if err != nil {
		return models.StorageLocation{}, errors.Wrapf(err, "Could not execute query for method: %v", GetStorageLocationById)
	}
	storageLocation.TotalFilesSize = int64(totalFilesSize)
	storageLocation.TotalExistingVolume = int64(totalExistingVolume)
	storageLocation.Vault = string(vault)
	return storageLocation, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByObjectId(id string) ([]models.StorageLocation, error) {
	rows, err := s.Db.Query(context.Background(), GetStorageLocationsByObjectId, id)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get current storage locations")
	}
	defer rows.Close()
	var storageLocations []models.StorageLocation
	for rows.Next() {
		var storageLocation models.StorageLocation
		var vault zeronull.Text
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads, &storageLocation.Group)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", GetStorageLocationsByObjectId)
		}
		storageLocation.Vault = string(vault)
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationsByTenantOrCollectionIdPaginated(pagination models.Pagination) ([]models.StorageLocation, int, error) {
	tenantStatement := ""
	collectionStatement := ""

	// tenantID filter
	tenantStatement = fmt.Sprintf(" where sl.tenant_id = '%s'", pagination.Id)
	if len(pagination.AllowedTenants) != 0 {
		tenants := strings.Join(pagination.AllowedTenants, "','")
		tenantStatement = tenantStatement + fmt.Sprintf(" and sl.tenant_id in ('%s')", tenants)
	}

	// collectionID filter
	if pagination.SecondId != "" {
		collectionStatement = fmt.Sprintf("where c.id = '%s'", pagination.SecondId)
	}

	query := fmt.Sprintf("select a.*, d.total_files_size, count(*) over() as total_items  from"+
		" (select sl.*, sum(sp.max_size) as total_existing_volume from storage_location sl"+
		" left join storage_partition sp"+
		" on sl.id = sp.storage_location_id "+
		" %s %s"+
		" group by sl.id) a"+
		" left join"+
		" (select b.storage_location_id, sum(total_files_size_for_instance) as total_files_size"+
		" from (select sum(oi.size) as total_files_size_for_instance, sp.id as spid, sp.storage_location_id"+
		" from storage_partition sp"+
		" inner join object_instance oi"+
		" on sp.id = oi.storage_partition_id"+
		" inner join object o"+
		" on o.id = oi.object_id"+
		" inner join collection c"+
		" on c.id = o.collection_id"+
		" %s"+
		" group by sp.id, sp.storage_location_id) b"+
		" group by storage_location_id) d"+
		" on a.id = d.storage_location_id"+
		" order by %s %s limit %s OFFSET %s ", tenantStatement, getLikeQueryForStorageLocation(pagination.SearchField, tenantStatement), collectionStatement, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := s.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	defer rows.Close()
	var storageLocations []models.StorageLocation
	var totalItems int
	for rows.Next() {
		var storageLocation models.StorageLocation
		var vault zeronull.Text
		var totalExistingVolume zeronull.Int8
		var totalFilesSize zeronull.Int8
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId,
			&storageLocation.Id, &storageLocation.NumberOfThreads,
			&totalExistingVolume, &totalFilesSize, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		storageLocation.TotalFilesSize = int64(totalFilesSize)
		storageLocation.TotalExistingVolume = int64(totalExistingVolume)
		storageLocation.Vault = string(vault)
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, totalItems, nil
}

func (s *StorageLocationRepositoryImpl) GetStorageLocationByObjectInstanceId(id string) (models.StorageLocation, error) {
	var storageLocation models.StorageLocation
	var vault zeronull.Text
	err := s.Db.QueryRow(context.Background(), GetStorageLocationByObjectInstanceId, id).Scan(&storageLocation.Alias, &storageLocation.Type, &vault, &storageLocation.Connection, &storageLocation.Quality,
		&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads)
	if err != nil {
		return models.StorageLocation{}, errors.Wrapf(err, "Could not execute query: %v", GetStorageLocationByObjectInstanceId)
	}
	storageLocation.Vault = string(vault)
	return storageLocation, nil
}

func NewStorageLocationRepository(db *pgxpool.Pool) StorageLocationRepository {
	return &StorageLocationRepositoryImpl{Db: db}
}

func getLikeQueryForStorageLocation(searchKey string, tenantStatement string) string {
	if searchKey != "" {
		condition := ""
		if tenantStatement == "" {
			condition = "where"
		} else {
			condition = "and"
		}
		return condition + strings.Replace(" (sl.id::text like '_search_key_%' or lower(sl.alias) like '%_search_key_%' or lower(sl.security_compliency) like '%_search_key_%')",
			"_search_key_", searchKey, -1)
	}
	return ""
}

func (s *StorageLocationRepositoryImpl) getOneNumberParameterById(id string, preparedStatement string) (int, error) {
	row := s.Db.QueryRow(context.Background(), preparedStatement, id)
	var amount int
	err := row.Scan(&amount)
	if err != nil {
		return amount, errors.Wrapf(err, "Could not execute query for prepared statement: %v", preparedStatement)
	}
	return amount, nil
}

func getStorageLocationsFromRows(rows pgx.Rows) ([]models.StorageLocation, error) {
	var storageLocations []models.StorageLocation
	for rows.Next() {
		var storageLocation models.StorageLocation
		var vault zeronull.Text
		err := rows.Scan(&storageLocation.Alias, &storageLocation.Type, &vault, &storageLocation.Connection, &storageLocation.Quality,
			&storageLocation.Price, &storageLocation.SecurityCompliency, &storageLocation.FillFirst, &storageLocation.OcflType, &storageLocation.TenantId, &storageLocation.Id, &storageLocation.NumberOfThreads, &storageLocation.Group)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for storage locations")
		}
		storageLocation.Vault = string(vault)
		storageLocations = append(storageLocations, storageLocation)
	}
	return storageLocations, nil
}
