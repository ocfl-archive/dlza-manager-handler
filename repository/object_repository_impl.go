package repository

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager/models"
)

const (
	Status                                      = "status::"
	GetObjectById                               = "GetObjectById"
	GetObjectBySignature                        = "GetObjectBySignature"
	UpdateObject                                = "UpdateObject"
	CreateObject                                = "CreateObject"
	GetObjectsByCollectionAlias                 = "GetObjectsByCollectionAlias"
	GetResultingQualityForObject                = "GetResultingQualityForObject"
	GetNeededQualityForObject                   = "GetNeededQualityForObject"
	GetObjectBySignatureAndStorageLocationGroup = "GetObjectBySignatureAndStorageLocationGroup"
	GetObjectByIdMv                             = "GetObjectByIdMv"
	Layout                                      = "2006-01-02 15:04:05"
)

type ObjectRepositoryImpl struct {
	Db     *pgxpool.Pool
	Logger zLogger.ZLogger
}

func CreateObjectPreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	preparedStatements := map[string]string{
		GetObjectById: `SELECT signature, sets, identifiers, title, alternative_titles, description, keywords,"references", ingest_workflow,"user",
       address, created, last_changed, "size", id, collection_id, checksum, authors, holding, expiration, head, versions, "binary" FROM OBJECT o WHERE ID = $1`,
		GetObjectBySignature: `SELECT signature, sets, identifiers, title, alternative_titles, description, keywords,"references", ingest_workflow,"user",
       address, created, last_changed, "size", id, collection_id, checksum, authors, holding, expiration, head, versions, "binary" FROM OBJECT o WHERE signature = $1`,
		GetObjectByIdMv: "SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow," +
			" \"user\", address, created, last_changed, size, id, collection_id, checksum, authors, holding, expiration, head, versions, total_file_size, total_file_count FROM mat_coll_obj o WHERE ID = $1",
		CreateObject: "INSERT INTO OBJECT(signature, \"sets\", identifiers, title, alternative_titles, description, keywords, \"references\"," +
			" ingest_workflow, \"user\", address, \"size\", collection_id, checksum, authors, holding, expiration, head, versions, \"binary\")" +
			" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING id",
		UpdateObject: "UPDATE OBJECT set signature = $1, sets = $2, identifiers = $3, title = $4," +
			" alternative_titles = $5, description = $6, keywords = $7, \"references\" = $8, ingest_workflow = $9," +
			" \"user\" = $10, address = $11, last_changed = $12, size = $13," +
			" collection_id = $14, checksum = $15, authors = $16, holding = $17, expiration = $18, head = $19, versions = $20, \"binary\" = $21" +
			" where id =$22",
		GetObjectsByCollectionAlias: "SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow,\"user\", address, created, last_changed, \"size\", id, collection_id, checksum, authors, holding, expiration, head, versions, \"binary\" FROM OBJECT where collection_id = $1",
		GetResultingQualityForObject: "select sum(quality) from object o " +
			" inner join object_instance oi on oi.object_id = o.id " +
			" inner join storage_partition sp on sp.id = oi.storage_partition_id" +
			" inner join storage_location sl on sl.id = sp.storage_location_id " +
			" where o.id = $1",
		GetNeededQualityForObject: "select quality from collection c " +
			" inner join object o on c.id = o.collection_id" +
			" where o.id = $1",
		GetObjectBySignatureAndStorageLocationGroup: `select * from object o
			inner join object_instance oi on o.id = oi.object_id
			inner join storage_partition sp on sp.id = oi.storage_partition_id
         	inner join storage_location sl on sl.id = sp.storage_location_id
			where oi.status in ('ok', 'new') and signature = $1  and sl.group = $2`,
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (o *ObjectRepositoryImpl) CreateObject(object models.Object) (string, error) {

	var id string
	expiration, err := time.Parse(Layout, object.Expiration)
	date := pgtype.Date{Time: expiration}
	if err != nil {
		date.Valid = false
	} else {
		date.Valid = true
	}
	err = o.Db.QueryRow(context.Background(), CreateObject, object.Signature, object.Sets, object.Identifiers, object.Title, object.AlternativeTitles, object.Description,
		object.Keywords, object.References, object.IngestWorkflow, object.User, object.Address, object.Size, object.CollectionId, object.Checksum, object.Authors, object.Holding, date, object.Head, object.Versions, object.Binary).Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %s", CreateObject)
	}
	return id, nil
}

func (o *ObjectRepositoryImpl) GetObjectBySignatureAndStorageLocationGroup(signature string, locationGroup string) (models.Object, error) {
	object := models.Object{}
	var holding zeronull.Text
	var expiration pgtype.Date
	var lastChanged time.Time
	var created time.Time
	err := o.Db.QueryRow(context.Background(), GetObjectBySignatureAndStorageLocationGroup, signature, locationGroup).Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
		&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
		&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions, &object.Binary)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return object, errors.Wrapf(err, "Could not execute query for method: %v", GetObjectBySignatureAndStorageLocationGroup)
	}
	object.Holding = string(holding)
	if expiration.Valid {
		object.Expiration = expiration.Time.Format(Layout)
	} else {
		object.Expiration = ""
	}
	object.Expiration = expiration.Time.Format(Layout)
	object.LastChanged = lastChanged.Format(Layout)
	object.Created = created.Format(Layout)
	return object, nil
}

func (o *ObjectRepositoryImpl) GetObjectById(id string) (models.Object, error) {
	var object models.Object
	var holding zeronull.Text
	var expiration pgtype.Date
	var lastChanged time.Time
	var created time.Time
	err := o.Db.QueryRow(context.Background(), GetObjectById, id).Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
		&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
		&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions, &object.Binary)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object by id")
	}
	object.Holding = string(holding)
	if expiration.Valid {
		object.Expiration = expiration.Time.Format(Layout)
	} else {
		object.Expiration = ""
	}
	object.Expiration = expiration.Time.Format(Layout)
	object.LastChanged = lastChanged.Format(Layout)
	object.Created = created.Format(Layout)
	return object, nil
}

func (o *ObjectRepositoryImpl) GetObjectBySignature(signature string) (models.Object, error) {
	var object models.Object
	var holding zeronull.Text
	var expiration pgtype.Date
	var lastChanged time.Time
	var created time.Time
	rows, err := o.Db.Query(context.Background(), GetObjectBySignature, signature)
	if err != nil {
		return object, errors.Wrapf(err, "Could not execute query: %s", GetObjectBySignature)
	}
	defer rows.Close()
	if !rows.Next() {
		return object, nil
	}
	err = rows.Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
		&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
		&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions, &object.Binary)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object by signature")
	}
	object.Holding = string(holding)
	if expiration.Valid {
		object.Expiration = expiration.Time.Format(Layout)
	} else {
		object.Expiration = ""
	}
	object.Expiration = expiration.Time.Format(Layout)
	object.LastChanged = lastChanged.Format(Layout)
	object.Created = created.Format(Layout)
	return object, nil
}

func (o *ObjectRepositoryImpl) GetObjectExceptListOlderThan(collectionId string, ids []string, collectionsNeeded []string) (models.Object, error) {
	firstCondition := ""
	if len(ids) != 0 {
		firstCondition = fmt.Sprintf("and objf.id not in ('%s')", strings.Join(ids, "','"))
	}
	collectionsNeededString := fmt.Sprintf("'{%s}'", strings.Join(collectionsNeeded, ","))
	var object models.Object
	var holding zeronull.Text
	var expiration pgtype.Date
	var lastChanged time.Time
	var created time.Time

	query := fmt.Sprintf(`SELECT signature, sets, identifiers, title, alternative_titles, description, keywords,
	"references", ingest_workflow, "user", address, objf.created, last_changed, objf."size",
	objf.id, collection_id, checksum, authors, holding, expiration, head, versions FROM quality_with_locations objf
	INNER JOIN object_instance oi
	ON objf.id = oi.object_id
	where objf.collection_id = $1
	%s
	AND (objf.ok IS false OR NOT ((objf.locations @> %s) AND (objf.locations <@ %s)))
	AND oi.status = 'ok'
	limit 1`, firstCondition, collectionsNeededString, collectionsNeededString)

	rows, err := o.Db.Query(context.Background(), query, collectionId)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object GetObjectExceptListOlderThan")
	}
	defer rows.Close()
	if !rows.Next() {
		return object, nil
	}
	err = rows.Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
		&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
		&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object GetObjectExceptListOlderThan")
	}
	object.Holding = string(holding)
	if expiration.Valid {
		object.Expiration = expiration.Time.Format(Layout)
	} else {
		object.Expiration = ""
	}
	object.Expiration = expiration.Time.Format(Layout)
	object.LastChanged = lastChanged.Format(Layout)
	object.Created = created.Format(Layout)
	return object, nil
}

func (o *ObjectRepositoryImpl) GetObjectExceptListOlderThanWithChecks(ids []string, timeBefore string, timeToWaitAvailability string) (models.Object, error) {
	firstCondition := ""
	if len(ids) != 0 {
		firstCondition = fmt.Sprintf("and o.id not in ('%s')", strings.Join(ids, "','"))
	}
	var object models.Object
	var holding zeronull.Text
	var expiration pgtype.Date
	var lastChanged time.Time
	var created time.Time

	query := fmt.Sprintf(`SELECT signature, sets, identifiers, title, alternative_titles, description, keywords,
	"references", ingest_workflow, "user", address, o.created, last_changed, o."size",
	o.id, collection_id, checksum, authors, holding, expiration, head, versions  FROM object o 
	INNER JOIN object_instance oi on o.id = oi.object_id 
	LEFT JOIN (select * from (SELECT ROW_NUMBER() over(PARTITION BY object_instance_id ORDER BY checktime DESC) AS number_of_row, *
		FROM object_instance_check) oic
		WHERE oic.number_of_row = 1) oicf ON oicf.object_instance_id = oi.id
	WHERE oi.status NOT IN ('to delete', 'error', 'not available', 'deprecated')
	%s
	AND (oicf.checktime < (now() - INTERVAL %s) OR (oicf.check_type = 'exists' AND oicf.checktime < (now() - INTERVAL %s)) OR oicf.id IS NULL)
	limit 1`, firstCondition, timeBefore, timeToWaitAvailability)

	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object GetObjectExceptListOlderThanWithChecks")
	}
	defer rows.Close()
	if !rows.Next() {
		return object, nil
	}
	err = rows.Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
		&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
		&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions)
	if err != nil {
		return object, errors.Wrapf(err, "cannot get object GetObjectExceptListOlderThanWithChecks")
	}
	object.Holding = string(holding)
	if expiration.Valid {
		object.Expiration = expiration.Time.Format(Layout)
	} else {
		object.Expiration = ""
	}
	object.Expiration = expiration.Time.Format(Layout)
	object.LastChanged = lastChanged.Format(Layout)
	object.Created = created.Format(Layout)
	return object, nil
}

func (o *ObjectRepositoryImpl) GetObjectByIdMv(id string) (models.Object, error) {
	var object models.Object
	var expiration pgtype.Date
	var holding zeronull.Text
	var lastChanged time.Time
	var created time.Time
	var totalFileSize zeronull.Int8
	var totalFileCount zeronull.Int8
	err := o.Db.QueryRow(context.Background(), GetObjectByIdMv, id).Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
		&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
		&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions, &totalFileSize, &totalFileCount)
	if err != nil {
		return object, errors.Wrapf(err, "cannot GetObjectByIdMv")
	}
	object.Holding = string(holding)
	if expiration.Valid {
		object.Expiration = expiration.Time.Format(Layout)
	} else {
		object.Expiration = ""
	}
	object.Expiration = expiration.Time.Format(Layout)
	object.LastChanged = lastChanged.Format(Layout)
	object.Created = created.Format(Layout)
	object.TotalFileSize = int64(totalFileSize)
	object.TotalFileCount = int64(totalFileCount)
	return object, nil
}

func (o *ObjectRepositoryImpl) UpdateObject(object models.Object) error {
	_, err := o.Db.Exec(context.Background(), UpdateObject, object.Signature, object.Sets, object.Identifiers, object.Title, object.AlternativeTitles, object.Description,
		object.Keywords, object.References, object.IngestWorkflow, object.User, object.Address, time.Now(), object.Size, object.CollectionId, object.Checksum, object.Authors, object.Holding, object.Expiration, object.Head, object.Versions, object.Binary, object.Id)
	if err != nil {
		return errors.Wrapf(err, "cannot update object")
	}
	return nil
}

func (o *ObjectRepositoryImpl) GetObjectsByCollectionId(id string) ([]models.Object, error) {
	rows, err := o.Db.Query(context.Background(), GetObjectsByCollectionAlias, id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %s", GetObjectsByCollectionAlias)
	}
	defer rows.Close()
	var objects []models.Object

	for rows.Next() {
		var object models.Object
		var holding zeronull.Text
		var expiration pgtype.Date
		var lastChanged time.Time
		var created time.Time
		err := rows.Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
			&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
			&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions, &object.Binary)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query for method: %s", GetObjectsByCollectionAlias)
		}
		object.Holding = string(holding)
		object.Expiration = expiration.Time.Format(Layout)
		object.LastChanged = lastChanged.Format(Layout)
		object.Created = created.Format(Layout)
		objects = append(objects, object)
	}
	return objects, nil
}

func (o *ObjectRepositoryImpl) GetObjectsByChecksum(checksum string) ([]models.Object, error) {
	query := fmt.Sprintf("SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow,"+
		"\"user\", address, created, last_changed, \"size\", id, collection_id, checksum, authors, holding, expiration, head, versions FROM OBJECT where checksum like "+"'%s%s'", "%", checksum)
	var objects []models.Object
	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	for rows.Next() {
		var object models.Object
		var holding zeronull.Text
		var expiration pgtype.Date
		var lastChanged time.Time
		var created time.Time
		err := rows.Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
			&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
			&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &object.Authors, &holding, &expiration, &object.Head, &object.Versions)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		object.Holding = string(holding)
		object.Expiration = expiration.Time.Format(Layout)
		object.LastChanged = lastChanged.Format(Layout)
		object.Created = created.Format(Layout)
		objects = append(objects, object)
	}
	return objects, nil
}

func (o *ObjectRepositoryImpl) GetResultingQualityForObject(id string) (int, error) {
	var quality int
	err := o.Db.QueryRow(context.Background(), GetResultingQualityForObject, id).Scan(&quality)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot GetResultingQualityForObject")
	}
	return quality, nil
}

func (o *ObjectRepositoryImpl) GetNeededQualityForObject(id string) (int, error) {
	var quality int
	err := o.Db.QueryRow(context.Background(), GetNeededQualityForObject, id).Scan(&quality)
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

	query := ""
	if strings.Contains(pagination.SearchField, Status) {
		status := strings.SplitAfter(pagination.SearchField, Status)[1]
		query = fmt.Sprintf("select mo.signature, mo.sets, mo.identifiers, mo.title, mo.alternative_titles, mo.description, mo.keywords, mo.references, mo.ingest_workflow,"+
			" mo.user, mo.address, mo.created, mo.last_changed, mo.size, mo.id, mo.collection_id, mo.checksum, mo.total_file_size, mo.total_file_count,"+
			" mo.authors, mo.holding, mo.expiration, mo.head, mo.versions, count(*) over() from col_obj_inst coi"+
			" inner join mat_coll_obj mo"+
			" on mo.id = coi.id"+
			" %s %s and status = '%s' "+
			" group by mo.id,mo.signature, mo.sets, mo.identifiers, mo.title, mo.alternative_titles, mo.description, mo.keywords, mo.references, mo.ingest_workflow, mo.user, mo.address, mo.created, mo.last_changed, mo.size, mo.expiration, mo.authors, mo.holding, mo.collection_id, mo.checksum, mo.head, mo.versions, mo.total_file_size, mo.total_file_count, mo.tenant_id"+
			" order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, status, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	} else {
		query = fmt.Sprintf("SELECT signature, sets, identifiers, title, alternative_titles, description, keywords, \"references\", ingest_workflow,"+
			"\"user\", address, created, last_changed, size, id, collection_id, checksum, total_file_size, total_file_count, authors, holding, expiration, head, versions, count(*) over() FROM mat_coll_obj"+
			" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObject(pagination.SearchField, firstCondition, secondCondition), pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	}
	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	var objects []models.Object
	var totalItems int
	for rows.Next() {
		var object models.Object
		var holding zeronull.Text
		var totalFileSize zeronull.Int8
		var totalFileCount zeronull.Int8
		var expiration pgtype.Date
		var lastChanged time.Time
		var created time.Time
		err := rows.Scan(&object.Signature, &object.Sets, &object.Identifiers, &object.Title,
			&object.AlternativeTitles, &object.Description, &object.Keywords, &object.References, &object.IngestWorkflow, &object.User,
			&object.Address, &created, &lastChanged, &object.Size, &object.Id, &object.CollectionId, &object.Checksum, &totalFileSize, &totalFileCount, &object.Authors, &holding, &expiration, &object.Head, &object.Versions, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		object.Holding = string(holding)
		object.TotalFileSize = int64(totalFileSize)
		object.TotalFileCount = int64(totalFileCount)
		object.Expiration = expiration.Time.Format(Layout)
		object.LastChanged = lastChanged.Format(Layout)
		object.Created = created.Format(Layout)
		objects = append(objects, object)
	}
	o.Logger.Debug().Msgf("Repository GetObjectsByCollectionIdPaginated function returned objects %s", time.Now())
	return objects, totalItems, nil
}

func NewObjectRepository(db *pgxpool.Pool, logger zLogger.ZLogger) ObjectRepository {
	return &ObjectRepositoryImpl{Db: db, Logger: logger}
}

func getLikeQueryForObject(searchKey string, firstCondition string, secondCondition string) string {
	if searchKey != "" {
		condition := ""
		if firstCondition == "" && secondCondition == "" {
			condition = "where"
		} else {
			condition = "and"
		}
		return condition + strings.Replace(" (id::text like '_search_key_%' or lower(signature) like '%_search_key_%'"+
			" or lower(title) like '%_search_key_%' or lower(description) like '%_search_key_%' or lower(ingest_workflow) like '%_search_key_%'"+
			" or lower(\"user\") like '%_search_key_%' or lower(address) like '%_search_key_%' or checksum like '%_search_key_%' or lower(authors::text) like '%_search_key_%' or lower(holding) like '%_search_key_%')",
			"_search_key_", searchKey, -1)
	}
	return ""
}
