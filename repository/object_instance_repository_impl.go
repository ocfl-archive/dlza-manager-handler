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

type objectInstanceRepositoryStmt int

const (
	GetObjectInstance objectInstanceRepositoryStmt = iota
	CreateObjectInstance
	DeleteObjectInstance
	GetObjectInstancesByObjectId
	GetAllObjectInstances
	UpdateObjectInstance
	GetAmountOfErrorsByCollectionId
)

type objectInstanceRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[objectInstanceRepositoryStmt]*sql.Stmt
}

func (o *objectInstanceRepositoryImpl) CreateObjectInstancePreparedStatements() error {

	preparedStatement := map[objectInstanceRepositoryStmt]string{
		GetObjectInstance:            fmt.Sprintf("SELECT * FROM %s.OBJECT_INSTANCE o WHERE ID = $1", o.Schema),
		CreateObjectInstance:         fmt.Sprintf("INSERT INTO %s.OBJECT_INSTANCE(\"path\", \"size\", status, storage_partition_id, object_id) VALUES ($1, $2, $3, $4, $5) RETURNING id", o.Schema),
		UpdateObjectInstance:         fmt.Sprintf("UPDATE %s.OBJECT_INSTANCE set status = $1 where id = $2", o.Schema),
		DeleteObjectInstance:         fmt.Sprintf("DELETE FROM %s.OBJECT_INSTANCE  where id =$1", o.Schema),
		GetObjectInstancesByObjectId: fmt.Sprintf("SELECT * FROM %s.OBJECT_INSTANCE where object_id = $1", o.Schema),
		GetAllObjectInstances:        fmt.Sprintf("SELECT * FROM %s.OBJECT_INSTANCE", o.Schema),
		GetAmountOfErrorsByCollectionId: strings.Replace("select count(oi.*) from %s.collection c,"+
			" %s.object o, %s.object_instance oi"+
			" where c.id = o.collection_id"+
			" and o.id = oi.object_id"+
			" and (oi.status = 'error' or oi.status = 'not available')"+
			" and o.collection_id = $1", "%s", o.Schema, -1),
	}
	var err error
	o.PreparedStatement = make(map[objectInstanceRepositoryStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		o.PreparedStatement[key], err = o.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (o *objectInstanceRepositoryImpl) GetAmountOfErrorsByCollectionId(id string) (int, error) {
	row := o.PreparedStatement[GetAmountOfErrorsByCollectionId].QueryRow(id)
	var amount int
	err := row.Scan(&amount)
	if err != nil {
		return amount, errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[GetAmountOfErrorsByCollectionId])
	}
	return amount, nil
}

func (o *objectInstanceRepositoryImpl) UpdateObjectInstance(objectInstance models.ObjectInstance) error {
	_, err := o.PreparedStatement[UpdateObjectInstance].Exec(objectInstance.Status, objectInstance.Id)

	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[UpdateObjectInstance])
	}
	return nil
}

func (o *objectInstanceRepositoryImpl) GetAllObjectInstances() ([]models.ObjectInstance, error) {
	rows, err := o.PreparedStatement[GetAllObjectInstances].Query()
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[GetAllObjectInstances])
	}
	var objectInstances []models.ObjectInstance

	for rows.Next() {
		var objectInstance models.ObjectInstance
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &objectInstance.Created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", o.PreparedStatement[GetObjectInstancesByObjectId])
		}
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, nil
}

func (o *objectInstanceRepositoryImpl) CreateObjectInstance(objectInstance models.ObjectInstance) (string, error) {
	row := o.PreparedStatement[CreateObjectInstance].QueryRow(objectInstance.Path, objectInstance.Size, objectInstance.Status, objectInstance.StoragePartitionId, objectInstance.ObjectId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[CreateObjectInstance])
	}
	return id, nil
}

func (o *objectInstanceRepositoryImpl) DeleteObjectInstance(id string) error {
	_, err := o.PreparedStatement[DeleteObjectInstance].Exec(id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[DeleteObjectInstance])
	}
	return nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstanceById(id string) (models.ObjectInstance, error) {
	objectInstance := models.ObjectInstance{}
	err := o.PreparedStatement[GetObjectInstance].QueryRow(id).Scan(&objectInstance.Path, &objectInstance.Size, &objectInstance.Created, &objectInstance.Status, &objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
	return objectInstance, err
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByObjectId(id string) ([]models.ObjectInstance, error) {
	rows, err := o.PreparedStatement[GetObjectInstancesByObjectId].Query(id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[GetObjectInstancesByObjectId])
	}
	var objectInstances []models.ObjectInstance

	for rows.Next() {
		var objectInstance models.ObjectInstance
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &objectInstance.Created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", o.PreparedStatement[GetObjectInstancesByObjectId])
		}
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, nil
}

func (o *objectInstanceRepositoryImpl) GetObjectInstancesByName(name string) ([]models.ObjectInstance, error) {
	query := strings.Replace(fmt.Sprintf("SELECT * FROM _schema.OBJECT_INSTANCE where path like "+"'%s/%s'", "%", name), "_schema", o.Schema, -1)
	var objectInstances []models.ObjectInstance
	rows, err := o.Db.Query(query)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	for rows.Next() {
		var objectInstance models.ObjectInstance
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &objectInstance.Created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
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
	if firstCondition == "" && secondCondition == "" {
		firstCondition = "where"
	} else {
		secondCondition = secondCondition + " and"
	}
	query := strings.Replace(fmt.Sprintf("SELECT oi.*, count(*) over() as total_items FROM _schema.OBJECT_INSTANCE oi"+
		" inner join _schema.object o on oi.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObjectInstance(pagination.SearchField), "oi."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", o.Schema, -1)
	rows, err := o.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var objectInstances []models.ObjectInstance
	var totalItems int
	for rows.Next() {
		var objectInstance models.ObjectInstance
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &objectInstance.Created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
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
	if firstCondition == "" && secondCondition == "" {
		firstCondition = "where"
	} else {
		secondCondition = secondCondition + " and"
	}
	query := strings.Replace(fmt.Sprintf("SELECT oi.*, count(*) over() as total_items FROM _schema.OBJECT_INSTANCE oi"+
		" inner join _schema.storage_partition sp on oi.storage_partition_id = sp.id"+
		" inner join _schema.storage_location sl on sl.id = sp.storage_location_id"+
		" inner join _schema.tenant t on t.id = sl.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObjectInstance(pagination.SearchField), "oi."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", o.Schema, -1)
	rows, err := o.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var objectInstances []models.ObjectInstance
	var totalItems int
	for rows.Next() {
		var objectInstance models.ObjectInstance
		err := rows.Scan(&objectInstance.Path, &objectInstance.Size, &objectInstance.Created, &objectInstance.Status,
			&objectInstance.Id, &objectInstance.StoragePartitionId, &objectInstance.ObjectId, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		objectInstances = append(objectInstances, objectInstance)
	}
	return objectInstances, totalItems, nil
}

func NewObjectInstanceRepository(db *sql.DB, schema string) ObjectInstanceRepository {
	return &objectInstanceRepositoryImpl{Db: db, Schema: schema}
}

func getLikeQueryForObjectInstance(searchKey string) string {
	return strings.Replace("(oi.id::text like '_search_key_%' or lower(oi.path) like '%_search_key_%'"+
		" or lower(oi.status::text) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
