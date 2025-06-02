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
	"time"
)

const (
	GetObjectInstance                    = "GetObjectInstance"
	CreateObjectInstance                 = "CreateObjectInstance"
	DeleteObjectInstance                 = "DeleteObjectInstance"
	GetObjectInstancesByObjectId         = "GetObjectInstancesByObjectId"
	GetAllObjectInstances                = "GetAllObjectInstances"
	UpdateObjectInstance                 = "UpdateObjectInstance"
	GetAmountOfErrorsByCollectionId      = "GetAmountOfErrorsByCollectionId"
	GetObjectInstancesByObjectIdPositive = "GetObjectInstancesByObjectIdPositive"
)

type objectInstanceRepositoryImpl struct {
	Db *pgxpool.Pool
}

func CreateObjectInstancePreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	preparedStatements := map[string]string{
		GetObjectInstance:                    "SELECT * FROM OBJECT_INSTANCE o WHERE ID = $1",
		CreateObjectInstance:                 "INSERT INTO OBJECT_INSTANCE(\"path\", \"size\", status, storage_partition_id, object_id) VALUES ($1, $2, $3, $4, $5) RETURNING id",
		UpdateObjectInstance:                 "UPDATE OBJECT_INSTANCE set status = $1 where id = $2",
		DeleteObjectInstance:                 "DELETE FROM OBJECT_INSTANCE  where id =$1",
		GetObjectInstancesByObjectId:         "SELECT * FROM OBJECT_INSTANCE where object_id = $1",
		GetObjectInstancesByObjectIdPositive: "SELECT * FROM OBJECT_INSTANCE where object_id = $1 AND status = 'ok'",
		GetAllObjectInstances:                "SELECT * FROM OBJECT_INSTANCE",
		GetAmountOfErrorsByCollectionId: "select count(oi.*) from collection c," +
			" object o, object_instance oi" +
			" where c.id = o.collection_id" +
			" and o.id = oi.object_id" +
			" and (oi.status = 'error' or oi.status = 'not available')" +
			" and o.collection_id = $1",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesBySignatureAndLocationsPathName(signature string, locationsName string) (models.ObjectInstance, error) {
	signature = strings.Replace(signature, ":", "_", -1)
	objectInstance := models.ObjectInstance{}
	var created time.Time
	err := o.Db.QueryRow(context.Background(), fmt.Sprintf("select * from object_instance where path like '%%/%s/%%%s%%'", locationsName, signature)).Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status, &objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
	if err != nil {
		return models.ObjectInstance{}, errors.Wrapf(err, "Could not execute query: %s", DeleteObjectInstance)
	}
	objectInstance.Created = created.Format(Layout)
	return objectInstance, err
}

func (o *objectInstanceRepositoryImpl) GetAmountOfErrorsByCollectionId(id string) (int, error) {
	row := o.Db.QueryRow(context.Background(), GetAmountOfErrorsByCollectionId, id)
	var amount int
	err := row.Scan(&amount)
	if err != nil {
		return amount, errors.Wrapf(err, "Could not execute query for method: %s", GetAmountOfErrorsByCollectionId)
	}
	return amount, nil
}

func (o *objectInstanceRepositoryImpl) UpdateObjectInstance(objectInstance models.ObjectInstance) error {
	_, err := o.Db.Exec(context.Background(), UpdateObjectInstance, objectInstance.Status, objectInstance.Id)

	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %s", UpdateObjectInstance)
	}
	return nil
}

func (o *objectInstanceRepositoryImpl) GetAllObjectInstances() ([]models.ObjectInstance, error) {
	rows, err := o.Db.Query(context.Background(), GetAllObjectInstances)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %s", GetAllObjectInstances)
	}
	defer rows.Close()
	var objectInstances []models.ObjectInstance

	for rows.Next() {
		var objectInstance models.ObjectInstance
		var created time.Time
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for in method: %s", GetObjectInstancesByObjectId)
		}
		objectInstance.Created = created.Format(Layout)
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, nil
}

func (o *objectInstanceRepositoryImpl) CreateObjectInstance(objectInstance models.ObjectInstance) (string, error) {
	row := o.Db.QueryRow(context.Background(), CreateObjectInstance, objectInstance.Path, objectInstance.Size, objectInstance.Status, objectInstance.StoragePartitionId, objectInstance.ObjectId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %s", CreateObjectInstance)
	}
	return id, nil
}

func (o *objectInstanceRepositoryImpl) DeleteObjectInstance(id string) error {
	_, err := o.Db.Exec(context.Background(), DeleteObjectInstance, id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %s", DeleteObjectInstance)
	}
	return nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstanceById(id string) (models.ObjectInstance, error) {
	objectInstance := models.ObjectInstance{}
	var created time.Time
	err := o.Db.QueryRow(context.Background(), GetObjectInstance, id).Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status, &objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
	if err != nil {
		return models.ObjectInstance{}, errors.Wrapf(err, "Could not execute query: %s", DeleteObjectInstance)
	}
	objectInstance.Created = created.Format(Layout)
	return objectInstance, err
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByObjectId(id string) ([]models.ObjectInstance, error) {
	rows, err := o.Db.Query(context.Background(), GetObjectInstancesByObjectId, id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %s", GetObjectInstancesByObjectId)
	}
	defer rows.Close()
	var objectInstances []models.ObjectInstance

	for rows.Next() {
		var objectInstance models.ObjectInstance
		var created time.Time
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query in method: %v", GetObjectInstancesByObjectId)
		}
		objectInstance.Created = created.Format(Layout)
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByObjectIdPositive(id string) ([]models.ObjectInstance, error) {
	rows, err := o.Db.Query(context.Background(), GetObjectInstancesByObjectIdPositive, id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %s", GetObjectInstancesByObjectIdPositive)
	}
	defer rows.Close()
	var objectInstances []models.ObjectInstance

	for rows.Next() {
		var objectInstance models.ObjectInstance
		var created time.Time
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.Wrapf(err, "Could not scan rows for query in method: %v", GetObjectInstancesByObjectIdPositive)
		}
		objectInstance.Created = created.Format(Layout)
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByName(name string) ([]models.ObjectInstance, error) {
	query := fmt.Sprintf("SELECT * FROM OBJECT_INSTANCE where path like "+"'%s/%s'", "%", name)
	var objectInstances []models.ObjectInstance
	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	for rows.Next() {
		var objectInstance models.ObjectInstance
		var created time.Time
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		objectInstance.Created = created.Format(Layout)
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByObjectIdPaginated(pagination models.Pagination) ([]models.ObjectInstance, int, error) {
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
		firstCondition = fmt.Sprintf("where oi.object_id = '%s'", pagination.Id)
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			secondCondition = fmt.Sprintf("and t.id in ('%s')", tenants)
		}
	}

	query := fmt.Sprintf("SELECT oi.*, count(*) over() as total_items FROM OBJECT_INSTANCE oi"+
		" inner join object o on oi.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObjectInstance(pagination.SearchField, firstCondition, secondCondition), "oi."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	var objectInstances []models.ObjectInstance
	var totalItems int
	for rows.Next() {
		var objectInstance models.ObjectInstance
		var created time.Time
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		objectInstance.Created = created.Format(Layout)
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, totalItems, nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByPartitionIdPaginated(pagination models.Pagination) ([]models.ObjectInstance, int, error) {
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
		firstCondition = fmt.Sprintf("where oi.storage_partition_id = '%s'", pagination.Id)
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			secondCondition = fmt.Sprintf("and t.id in ('%s')", tenants)
		}
	}

	query := fmt.Sprintf("SELECT oi.*, count(*) over() as total_items FROM OBJECT_INSTANCE oi"+
		" inner join storage_partition sp on oi.storage_partition_id = sp.id"+
		" inner join storage_location sl on sl.id = sp.storage_location_id"+
		" inner join tenant t on t.id = sl.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObjectInstance(pagination.SearchField, firstCondition, secondCondition), "oi."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	var objectInstances []models.ObjectInstance
	var totalItems int
	for rows.Next() {
		var objectInstance models.ObjectInstance
		var created time.Time
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		objectInstance.Created = created.Format(Layout)
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, totalItems, nil
}

func NewObjectInstanceRepository(db *pgxpool.Pool) ObjectInstanceRepository {
	return &objectInstanceRepositoryImpl{Db: db}
}

func getLikeQueryForObjectInstance(searchKey string, firstCondition string, secondCondition string) string {
	if searchKey != "" {
		condition := ""
		if firstCondition == "" && secondCondition == "" {
			condition = "where"
		} else {
			condition = "and"
		}
		return condition + strings.Replace(" (oi.id::text like '_search_key_%' or lower(oi.path) like '%_search_key_%'"+
			" or lower(oi.status::text) like '%_search_key_%')",
			"_search_key_", searchKey, -1)
	}
	return ""
}
