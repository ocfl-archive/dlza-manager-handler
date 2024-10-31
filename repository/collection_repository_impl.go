package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"strconv"
	"strings"
)

type collectionPreparedStmt int

const (
	CreateCollection collectionPreparedStmt = iota
	DeleteCollectionById
	UpdateCollection
	GetCollectionsByTenantId
	GetCollectionIdByAlias
	GetCollectionByAlias
	GetCollectionByIdFromMv
	GetCollectionById
	GetSizeForAllObjectInstancesByCollectionId
)

type CollectionRepositoryImpl struct {
	Db                 *sql.DB
	Schema             string
	PreparedStatements map[collectionPreparedStmt]*sql.Stmt
}

func NewCollectionRepository(db *sql.DB, schema string) CollectionRepository {
	return &CollectionRepositoryImpl{Db: db, Schema: schema}
}

func (c *CollectionRepositoryImpl) CreateCollectionPreparedStatements() error {
	preparedStatements := map[collectionPreparedStmt]string{
		GetCollectionsByTenantId: fmt.Sprintf("SELECT * FROM %s.collection where tenant_id = $1", c.Schema),
		GetCollectionIdByAlias:   fmt.Sprintf("SELECT id FROM %s.collection where alias = $1", c.Schema),
		GetCollectionByAlias:     fmt.Sprintf("SELECT * FROM %s.collection where alias = $1", c.Schema),
		GetCollectionByIdFromMv:  fmt.Sprintf("SELECT * FROM %s.mat_coll_obj_file where id = $1", c.Schema),
		GetCollectionById:        fmt.Sprintf("SELECT * FROM %s.collection where id = $1", c.Schema),
		DeleteCollectionById:     fmt.Sprintf("DELETE FROM %s.collection WHERE id = $1", c.Schema),
		UpdateCollection:         fmt.Sprintf("UPDATE %s.collection SET description = $1, owner = $2, owner_mail= $3, name = $4, quality = $5, tenant_id= $6 where id = $7", c.Schema),
		CreateCollection: fmt.Sprintf("INSERT INTO %s.collection(alias, description, owner, owner_mail, name, quality, tenant_id)"+
			" VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id", c.Schema),
		GetSizeForAllObjectInstancesByCollectionId: strings.Replace("select sum(oi.size) from %s.object o"+
			" left join  %s.object_instance oi"+
			" on o.id = oi.object_id"+
			" where o.collection_id = $1", "%s", c.Schema, -1),
	}
	var err error
	c.PreparedStatements = make(map[collectionPreparedStmt]*sql.Stmt)
	for key, stmt := range preparedStatements {
		c.PreparedStatements[key], err = c.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (c *CollectionRepositoryImpl) GetSizeForAllObjectInstancesByCollectionId(id string) (int64, error) {
	row := c.PreparedStatements[GetSizeForAllObjectInstancesByCollectionId].QueryRow(id)
	var size sql.NullInt64
	err := row.Scan(&size)
	if err != nil {
		return 0, errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[GetSizeForAllObjectInstancesByCollectionId])
	}
	return size.Int64, nil
}

func (c *CollectionRepositoryImpl) CreateCollection(collection models.Collection) (string, error) {

	row := c.PreparedStatements[CreateCollection].QueryRow(collection.Alias, collection.Description, collection.Owner, collection.OwnerMail, collection.Name,
		collection.Quality, collection.TenantId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[CreateCollection])
	}
	return id, nil
}

func (c *CollectionRepositoryImpl) DeleteCollectionById(id string) error {
	_, err := c.PreparedStatements[DeleteCollectionById].Exec(id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[DeleteCollectionById])
	}
	return nil
}

func (c *CollectionRepositoryImpl) UpdateCollection(collection models.Collection) error {
	_, err := c.PreparedStatements[UpdateCollection].Exec(collection.Description, collection.Owner, collection.OwnerMail, collection.Name,
		collection.Quality, collection.TenantId, collection.Id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[UpdateCollection])
	}
	return nil
}

func (c *CollectionRepositoryImpl) GetCollectionsByTenantId(tenantId string) ([]models.Collection, error) {
	rows, err := c.PreparedStatements[GetCollectionsByTenantId].Query(tenantId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[GetCollectionsByTenantId])
	}
	var collections []models.Collection

	for rows.Next() {
		var collection models.Collection
		err := rows.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
			&collection.Quality, &collection.TenantId, &collection.Id)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", c.PreparedStatements[GetCollectionsByTenantId])
		}
		collections = append(collections, collection)
	}
	return collections, nil
}

func (c *CollectionRepositoryImpl) GetCollectionIdByAlias(alias string) (string, error) {
	row := c.PreparedStatements[GetCollectionIdByAlias].QueryRow(alias)
	var id string

	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[GetCollectionIdByAlias])
	}

	return id, nil
}

func (c *CollectionRepositoryImpl) GetCollectionById(id string) (models.Collection, error) {
	row := c.PreparedStatements[GetCollectionById].QueryRow(id)
	collection := models.Collection{}
	err := row.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
		&collection.Quality, &collection.TenantId, &collection.Id)
	if err != nil {
		return collection, errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[GetCollectionById])
	}
	return collection, nil
}

func (c *CollectionRepositoryImpl) GetCollectionByIdFromMv(id string) (models.Collection, error) {
	row := c.PreparedStatements[GetCollectionByIdFromMv].QueryRow(id)
	collection := models.Collection{}
	err := row.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
		&collection.Quality, &collection.TenantId, &collection.Id, &collection.TotalFileSize, &collection.TotalFileCount, &collection.TotalObjectCount)
	if err != nil {
		return collection, errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[GetCollectionByIdFromMv])
	}
	return collection, nil
}

func (c *CollectionRepositoryImpl) GetCollectionByAlias(alias string) (models.Collection, error) {
	row := c.PreparedStatements[GetCollectionByAlias].QueryRow(alias)
	collection := models.Collection{}
	err := row.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
		&collection.Quality, &collection.TenantId, &collection.Id)
	if err != nil {
		return collection, errors.Wrapf(err, "Could not execute query: %v", c.PreparedStatements[GetCollectionByAlias])
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
	if firstCondition == "" && secondCondition == "" {
		firstCondition = "where"
	} else {
		secondCondition = secondCondition + " and"
	}

	query := strings.Replace(fmt.Sprintf("SELECT *, count(*) over() FROM _schema.mat_coll_obj_file"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForCollection(pagination.SearchField),
		pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", c.Schema, -1)

	rows, err := c.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	var collections []models.Collection
	var totalItems int
	for rows.Next() {
		var collection models.Collection
		err = rows.Scan(&collection.Alias, &collection.Description, &collection.Owner, &collection.OwnerMail, &collection.Name,
			&collection.Quality, &collection.TenantId, &collection.Id, &collection.TotalFileSize, &collection.TotalFileCount, &collection.TotalObjectCount, &totalItems)

		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		collections = append(collections, collection)
	}
	return collections, totalItems, nil
}

func getLikeQueryForCollection(searchKey string) string {
	return strings.Replace("(id::text like '_search_key_%' or lower(alias) like '%_search_key_%'"+
		" or lower(name) like '%_search_key_%' or lower(owner_mail) like '%_search_key_%' or lower(owner) like '%_search_key_%' or lower(description) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
