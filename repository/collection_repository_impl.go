package repository

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ocfl-archive/dlza-manager/models"
	"strconv"
	"strings"
)

const (
	CreateCollection                                       = "CreateCollection"
	DeleteCollectionById                                   = "DeleteCollectionById"
	UpdateCollection                                       = "UpdateCollection"
	GetCollectionsByTenantId                               = "GetCollectionsByTenantId"
	GetCollectionIdByAlias                                 = "GetCollectionIdByAlias"
	GetCollectionByAlias                                   = "GetCollectionByAlias"
	GetCollectionByIdFromMv                                = "GetCollectionByIdFromMv"
	GetCollectionById                                      = "GetCollectionById"
	GetSizeForAllObjectInstancesByCollectionId             = "GetSizeForAllObjectInstancesByCollectionId"
	GetAmountOfObjectsInCollection                         = "GetAmountOfObjectsInCollection"
	GetExistingStorageLocationsCombinationsForCollectionId = "GetExistingStorageLocationsCombinationsForCollectionId"
)

type CollectionRepositoryImpl struct {
	Db *pgxpool.Pool
}

func NewCollectionRepository(db *pgxpool.Pool) CollectionRepository {
	return &CollectionRepositoryImpl{Db: db}
}

func CreateCollectionPreparedStatements(ctx context.Context, conn *pgx.Conn) error {
	preparedStatements := map[string]string{
		GetCollectionsByTenantId: "SELECT * FROM collection where tenant_id = $1",
		GetCollectionIdByAlias:   "SELECT id FROM collection where alias = $1",
		GetCollectionByAlias:     "SELECT * FROM collection where alias = $1",
		GetCollectionByIdFromMv:  "SELECT * FROM mat_coll_obj_file where id = $1",
		GetCollectionById:        "SELECT * FROM collection where id = $1",
		DeleteCollectionById:     "DELETE FROM collection WHERE id = $1",
		UpdateCollection:         "UPDATE collection SET description = $1, owner = $2, owner_mail= $3, name = $4, quality = $5, tenant_id= $6 where id = $7",
		CreateCollection: "INSERT INTO collection(alias, description, owner, owner_mail, name, quality, tenant_id)" +
			" VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
		GetSizeForAllObjectInstancesByCollectionId: "select sum(oi.size) from object o" +
			" left join  object_instance oi" +
			" on o.id = oi.object_id" +
			" where o.collection_id = $1",
		GetAmountOfObjectsInCollection: `SELECT count(*) FROM object where collection_id = $1`,
		GetExistingStorageLocationsCombinationsForCollectionId: `SELECT col.id, col.alias, ol.locations, ol.quality, ol.price, SUM(ol.size) AS size 
		FROM 
			collection col, 
			obj_loc ol
		WHERE 
			col.id=ol.collection_id
		AND
		    col.id=$1
		GROUP BY col.id, col.alias, ol.locations, ol.quality, ol.price
		ORDER BY size DESC`,
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (c *CollectionRepositoryImpl) GetSizeForAllObjectInstancesByCollectionId(id string) (int64, error) {
	row := c.Db.QueryRow(context.Background(), GetSizeForAllObjectInstancesByCollectionId, id)
	var size zeronull.Int8
	err := row.Scan(&size)
	if err != nil {
		return 0, errors.Wrapf(err, "Could not execute query for method: %v", GetSizeForAllObjectInstancesByCollectionId)
	}
	return int64(size), nil
}

func (c *CollectionRepositoryImpl) CreateCollection(collection models.Collection) (string, error) {

	row := c.Db.QueryRow(context.Background(), CreateCollection, collection.Alias, collection.Description, collection.Owner, collection.OwnerMail, collection.Name,
		collection.Quality, collection.TenantId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query in method: %v", CreateCollection)
	}
	return id, nil
}

func (c *CollectionRepositoryImpl) DeleteCollectionById(id string) error {
	_, err := c.Db.Exec(context.Background(), DeleteCollectionById, id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query in method: %v", DeleteCollectionById)
	}
	return nil
}

func (c *CollectionRepositoryImpl) UpdateCollection(collection models.Collection) error {
	_, err := c.Db.Exec(context.Background(), UpdateCollection, collection.Description, collection.Owner, collection.OwnerMail, collection.Name,
		collection.Quality, collection.TenantId, collection.Id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query in method: %v", UpdateCollection)
	}
	return nil
}

func (c *CollectionRepositoryImpl) GetCollectionsByTenantId(tenantId string) ([]models.Collection, error) {
	rows, err := c.Db.Query(context.Background(), GetCollectionsByTenantId, tenantId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetCollectionsByTenantId)
	}
	defer rows.Close()
	var collections []models.Collection

	for rows.Next() {
		var collection models.Collection
		err := rows.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
			&collection.Quality, &collection.TenantId, &collection.Id)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query in method: %v", GetCollectionsByTenantId)
		}
		collections = append(collections, collection)
	}
	return collections, nil
}

func (c *CollectionRepositoryImpl) GetAmountOfObjectsInCollection(id string) (int64, error) {
	row := c.Db.QueryRow(context.Background(), GetAmountOfObjectsInCollection, id)
	var amount zeronull.Int8
	err := row.Scan(&amount)
	if err != nil {
		return 0, errors.Wrapf(err, "Could not execute query for method: %v", GetAmountOfObjectsInCollection)
	}
	return int64(amount), nil
}

func (c *CollectionRepositoryImpl) GetExistingStorageLocationsCombinationsForCollectionId(id string) ([]models.CollectionWithExistingStorageLocationsCombinations, error) {
	rows, err := c.Db.Query(context.Background(), GetExistingStorageLocationsCombinationsForCollectionId, id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetExistingStorageLocationsCombinationsForCollectionId)
	}
	defer rows.Close()
	var collections []models.CollectionWithExistingStorageLocationsCombinations

	for rows.Next() {
		var collection models.CollectionWithExistingStorageLocationsCombinations
		err := rows.Scan(&collection.Id, &collection.Alias, &collection.LocationsIds, &collection.Quality, &collection.Price, &collection.Size)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query in method: %v", GetExistingStorageLocationsCombinationsForCollectionId)
		}
		collections = append(collections, collection)
	}
	return collections, nil
}

func (c *CollectionRepositoryImpl) GetCollectionIdByAlias(alias string) (string, error) {
	row := c.Db.QueryRow(context.Background(), GetCollectionIdByAlias, alias)
	var id string

	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query in method: %v", GetCollectionIdByAlias)
	}

	return id, nil
}

func (c *CollectionRepositoryImpl) GetCollectionById(id string) (models.Collection, error) {
	row := c.Db.QueryRow(context.Background(), GetCollectionById, id)
	collection := models.Collection{}
	err := row.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
		&collection.Quality, &collection.TenantId, &collection.Id)
	if err != nil {
		return collection, errors.Wrapf(err, "Could not execute query in method: %v", GetCollectionById)
	}
	return collection, nil
}

func (c *CollectionRepositoryImpl) GetCollectionByIdFromMv(id string) (models.Collection, error) {
	row := c.Db.QueryRow(context.Background(), GetCollectionByIdFromMv, id)
	collection := models.Collection{}
	var totalFileSize zeronull.Int8
	var totalFileCount zeronull.Int8
	var totalObjectCount zeronull.Int8
	err := row.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
		&collection.Quality, &collection.TenantId, &collection.Id, &totalFileSize, &totalFileCount, &totalObjectCount)
	if err != nil {
		return collection, errors.Wrapf(err, "Could not execute query in method: %v", GetCollectionByIdFromMv)
	}
	collection.TotalFileSize = int64(totalFileSize)
	collection.TotalFileCount = int64(totalFileCount)
	collection.TotalObjectCount = int64(totalObjectCount)
	return collection, nil
}

func (c *CollectionRepositoryImpl) GetCollectionByAlias(alias string) (models.Collection, error) {
	row := c.Db.QueryRow(context.Background(), GetCollectionByAlias, alias)
	collection := models.Collection{}
	err := row.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
		&collection.Quality, &collection.TenantId, &collection.Id)
	if err != nil {
		return collection, errors.Wrapf(err, "Could not execute query in method: %v", GetCollectionByAlias)
	}
	return collection, nil
}

func (c *CollectionRepositoryImpl) GetCollectionsByTenantIdPaginated(pagination models.Pagination) ([]models.Collection, int, error) {
	firstCondition := ""
	secondCondition := ""
	if pagination.Id == "" {
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			firstCondition = fmt.Sprintf("where tenant_id in ('%s')", tenants)
		}
	} else {
		firstCondition = fmt.Sprintf("where tenant_id = '%s'", pagination.Id)
		if len(pagination.AllowedTenants) != 0 {
			tenants := strings.Join(pagination.AllowedTenants, "','")
			secondCondition = fmt.Sprintf("and tenant_id in ('%s')", tenants)
		}
	}

	query := fmt.Sprintf("SELECT *, count(*) over() FROM mat_coll_obj_file"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForCollection(pagination.SearchField, firstCondition, secondCondition),
		pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))

	rows, err := c.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	defer rows.Close()
	var collections []models.Collection
	var totalItems int
	for rows.Next() {
		var collection models.Collection
		var totalFileSize zeronull.Int8
		var totalFileCount zeronull.Int8
		var totalObjectCount zeronull.Int8
		err = rows.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
			&collection.Quality, &collection.TenantId, &collection.Id, &totalFileSize, &totalFileCount, &totalObjectCount, &totalItems)

		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		collection.TotalFileSize = int64(totalFileSize)
		collection.TotalFileCount = int64(totalFileCount)
		collection.TotalObjectCount = int64(totalObjectCount)
		collections = append(collections, collection)
	}
	return collections, totalItems, nil
}

func getLikeQueryForCollection(searchKey string, firstCondition string, secondCondition string) string {
	if searchKey != "" {
		condition := ""
		if firstCondition == "" && secondCondition == "" {
			condition = "where"
		} else {
			condition = "and"
		}
		return condition + strings.Replace(" (id::text like '_search_key_%' or lower(alias) like '%_search_key_%'"+
			" or lower(name) like '%_search_key_%' or lower(owner_mail) like '%_search_key_%' or lower(owner) like '%_search_key_%' or lower(description) like '%_search_key_%')",
			"_search_key_", searchKey, -1)
	}
	return ""
}
