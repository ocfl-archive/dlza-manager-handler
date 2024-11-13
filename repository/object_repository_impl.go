package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"slices"
	"strconv"
	"strings"
	"time"
)

type objectPrepareStmt int

const (
	GetObjectById objectPrepareStmt = iota
	UpdateObject
	CreateObject
	GetObjectsByCollectionAlias
	GetResultingQualityForObject
	GetNeededQualityForObject
	GetObjectByIdMv
)

type ObjectRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[objectPrepareStmt]*sql.Stmt
}

func (o *ObjectRepositoryImpl) CreateObjectPreparedStatements() error {

	preparedStatement := map[objectPrepareStmt]string{
		GetObjectById: fmt.Sprintf(`SELECT signature, sets, identifiers, title, alternative_titles, description, keywords,"references", ingest_workflow,"user",
       address, created, last_changed, "size", id, collection_id, checksum, authors, expiration, holding FROM %s.OBJECT o WHERE ID = $1`, o.Schema),
		GetObjectByIdMv: fmt.Sprintf("SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow,"+
			" \"user\", address, created, last_changed, size, id, collection_id, checksum, authors, expiration, holding, total_file_size, total_file_count FROM %s.mat_coll_obj o WHERE ID = $1", o.Schema),
		CreateObject: fmt.Sprintf("INSERT INTO %s.OBJECT(signature, \"sets\", identifiers, title, alternative_titles, description, keywords, \"references\","+
			" ingest_workflow, \"user\", address, \"size\", collection_id, checksum, authors, expiration, holding)"+
			" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING id", o.Schema),
		UpdateObject: fmt.Sprintf("UPDATE %s.OBJECT set signature = $1, sets = $2, identifiers = $3, title = $4,"+
			" alternative_titles = $5, description = $6, keywords = $7, \"references\" = $8, ingest_workflow = $9,"+
			" \"user\" = $10, address = $11, last_changed = $12, size = $13,"+
			" collection_id = $14, checksum = $15, authors = $16, expiration = $17, holding = $18"+
			" where id =$19", o.Schema),
		GetObjectsByCollectionAlias: fmt.Sprintf(`SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, "references", ingest_workflow,"user",
       address, created, last_changed, "size", id, collection_id, checksum, authors, expiration, holding FROM %s.OBJECT where collection_id = $1`, o.Schema),
		GetResultingQualityForObject: strings.Replace("select sum(quality) from %s.object o "+
			" inner join %s.object_instance oi on oi.object_id = o.id "+
			" inner join %s.storage_partition sp on sp.id = oi.storage_partition_id"+
			" inner join %s.storage_location sl on sl.id = sp.storage_location_id "+
			" where o.id = $1", "%s", o.Schema, -1),
		GetNeededQualityForObject: strings.Replace("select quality from %s.collection c "+
			" inner join %s.object o on c.id = o.collection_id"+
			" where o.id = $1", "%s", o.Schema, -1),
	}
	var err error
	o.PreparedStatement = make(map[objectPrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		o.PreparedStatement[key], err = o.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (o *ObjectRepositoryImpl) CreateObject(object models.Object) (string, error) {

	var id string
	err := o.PreparedStatement[CreateObject].QueryRow(object.Signature, pq.Array(object.Sets), pq.Array(object.Identifiers), object.Title, pq.Array(object.AlternativeTitles), object.Description,
		pq.Array(object.Keywords), pq.Array(object.References), object.IngestWorkflow, object.User, object.Address, object.Size, object.CollectionId, object.Checksum, pq.Array(object.Authors), object.Expiration, object.Holding).Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[CreateObject])
	}
	return id, nil
}

func (o *ObjectRepositoryImpl) GetObjectById(id string) (models.Object, error) {
	var object models.Object

	err := o.PreparedStatement[GetObjectById].QueryRow(id).Scan(&object.Signature, pq.Array(&object.Sets), pq.Array(&object.Identifiers), &object.Title,
		pq.Array(&object.AlternativeTitles), &object.Description, pq.Array(&object.Keywords), pq.Array(&object.References), &object.IngestWorkflow, &object.User,
		&object.Address, &object.Created, &object.LastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, pq.Array(&object.Authors), &object.Expiration, &object.Holding)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object by id")
	}
	return object, nil
}

func (o *ObjectRepositoryImpl) GetObjectByIdMv(id string) (models.Object, error) {
	var object models.Object

	err := o.PreparedStatement[GetObjectByIdMv].QueryRow(id).Scan(&object.Signature, pq.Array(&object.Sets), pq.Array(&object.Identifiers), &object.Title,
		pq.Array(&object.AlternativeTitles), &object.Description, pq.Array(&object.Keywords), pq.Array(&object.References), &object.IngestWorkflow, &object.User,
		&object.Address, &object.Created, &object.LastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, pq.Array(&object.Authors), &object.Expiration, &object.Holding, &object.TotalFileSize, &object.TotalFileCount)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object by id")
	}
	return object, nil
}

func (o *ObjectRepositoryImpl) UpdateObject(object models.Object) error {
	_, err := o.PreparedStatement[UpdateObject].Exec(object.Signature, pq.Array(object.Sets), pq.Array(object.Identifiers), object.Title, pq.Array(object.AlternativeTitles), object.Description,
		pq.Array(object.Keywords), pq.Array(object.References), object.IngestWorkflow, object.User, object.Address, time.Now().Format("2006-01-02 15:04:05.000000"), object.Size, object.CollectionId, object.Checksum, pq.Array(object.Authors), object.Expiration, object.Holding, object.Id)
	if err != nil {
		return errors.Wrapf(err, "cannot update object")
	}
	return nil
}

func (o *ObjectRepositoryImpl) GetObjectsByCollectionId(id string) ([]models.Object, error) {
	rows, err := o.PreparedStatement[GetObjectsByCollectionAlias].Query(id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[GetObjectsByCollectionAlias])
	}
	var objects []models.Object

	for rows.Next() {
		var object models.Object
		err := rows.Scan(&object.Signature, pq.Array(&object.Sets), pq.Array(&object.Identifiers), &object.Title,
			pq.Array(&object.AlternativeTitles), &object.Description, pq.Array(&object.Keywords), pq.Array(&object.References), &object.IngestWorkflow, &object.User,
			&object.Address, &object.Created, &object.LastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, pq.Array(&object.Authors), &object.Expiration, &object.Holding)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", o.PreparedStatement[GetObjectsByCollectionAlias])
		}
		objects = append(objects, object)
	}
	return objects, nil
}

func (o *ObjectRepositoryImpl) GetObjectsByChecksum(checksum string) ([]models.Object, error) {
	query := strings.Replace(fmt.Sprintf("SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow,\"user\", address, created, last_changed,"+
		" \"size\", id, collection_id, checksum, authors, expiration, holding FROM _schema.OBJECT where checksum like "+"'%s%s'", "%", checksum), "_schema", o.Schema, -1)
	var objects []models.Object
	rows, err := o.Db.Query(query)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	for rows.Next() {
		var object models.Object
		err := rows.Scan(&object.Signature, pq.Array(&object.Sets), pq.Array(&object.Identifiers), &object.Title,
			pq.Array(&object.AlternativeTitles), &object.Description, pq.Array(&object.Keywords), pq.Array(&object.References), &object.IngestWorkflow, &object.User,
			&object.Address, &object.Created, &object.LastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, pq.Array(&object.Authors), &object.Expiration, &object.Holding)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		objects = append(objects, object)
	}
	return objects, nil
}

func (o *ObjectRepositoryImpl) GetResultingQualityForObject(id string) (int, error) {
	var quality int
	err := o.PreparedStatement[GetResultingQualityForObject].QueryRow(id).Scan(&quality)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot GetResultingQualityForObject")
	}
	return quality, nil
}

func (o *ObjectRepositoryImpl) GetNeededQualityForObject(id string) (int, error) {
	var quality int
	err := o.PreparedStatement[GetNeededQualityForObject].QueryRow(id).Scan(&quality)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot GetNeededQualityForObject")
	}
	return quality, nil
}

func (o *ObjectRepositoryImpl) GetObjectsByCollectionIdPaginated(pagination models.Pagination) ([]models.Object, int, error) {
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
			firstCondition = fmt.Sprintf("where tenant_id in ('%s')", tenants)
		}
	} else {
		firstCondition = fmt.Sprintf("where collection_id = '%s'", pagination.Id)
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			secondCondition = fmt.Sprintf("and tenant_id in ('%s')", tenants)
		}
	}
	if firstCondition == "" && secondCondition == "" {
		firstCondition = "where"
	} else {
		secondCondition = secondCondition + " and"
	}
	query := strings.Replace(fmt.Sprintf("SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow,"+
		"\"user\", address, created, last_changed, size, id, collection_id, checksum, total_file_size, total_file_count, authors, expiration, holding, count(*) over() FROM _schema.mat_coll_obj"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObject(pagination.SearchField), pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", o.Schema, -1)
	rows, err := o.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	var objects []models.Object
	var totalItems int
	for rows.Next() {
		var object models.Object
		err := rows.Scan(&object.Signature, pq.Array(&object.Sets), pq.Array(&object.Identifiers), &object.Title,
			pq.Array(&object.AlternativeTitles), &object.Description, pq.Array(&object.Keywords), pq.Array(&object.References), &object.IngestWorkflow, &object.User,
			&object.Address, &object.Created, &object.LastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.TotalFileSize, &object.TotalFileCount, pq.Array(&object.Authors), &object.Expiration, &object.Holding, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		objects = append(objects, object)
	}

	return objects, totalItems, nil
}

func NewObjectRepository(db *sql.DB, schema string) ObjectRepository {
	return &ObjectRepositoryImpl{Db: db, Schema: schema}
}

func getLikeQueryForObject(searchKey string) string {
	return strings.Replace("(id::text like '_search_key_%' or lower(signature) like '%_search_key_%'"+
		" or lower(title) like '%_search_key_%' or lower(description) like '%_search_key_%' or lower(ingest_workflow) like '%_search_key_%'"+
		" or lower(\"user\") like '%_search_key_%' or lower(address) like '%_search_key_%' or checksum like '%_search_key_%' or lower(authors::text) like '%_search_key_%' or lower(holding) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
