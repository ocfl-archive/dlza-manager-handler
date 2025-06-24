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
	FindAllTenants                           = "FindAllTenants"
	FindTenantById                           = "FindTenantById"
	SaveTenant                               = "SaveTenant"
	UpdateTenant                             = "UpdateTenant"
	DeleteTenant                             = "DeleteTenant"
	GetAmountOfObjectsAndTotalSizeByTenantId = "GetAmountOfObjectsAndTotalSizeByTenantId"
)

type TenantRepositoryImpl struct {
	Db *pgxpool.Pool
}

func NewTenantRepository(Db *pgxpool.Pool) TenantRepository {
	return &TenantRepositoryImpl{
		Db: Db,
	}
}

func CreateTenantPreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	tenantPreparedStatements := map[string]string{
		FindAllTenants: "SELECT * FROM TENANT",
		FindTenantById: "SELECT * FROM TENANT WHERE id = $1",
		SaveTenant:     "insert into TENANT(name, alias, person, email, api_key_id) values($1, $2, $3, $4, $5)",
		UpdateTenant:   "update TENANT set name = $1, alias = $2, person = $3, email = $4 where id =$5",
		DeleteTenant:   "DELETE FROM TENANT WHERE id = $1",
		GetAmountOfObjectsAndTotalSizeByTenantId: "select sum(current_objects),  sum(current_size) from tenant t" +
			" left join storage_location sl" +
			" on t.id = sl.tenant_id" +
			" left join storage_partition sp" +
			" on sl.id = sp.storage_location_id" +
			" where t.id = $1" +
			" group by t.id",
	}
	for name, sqlStm := range tenantPreparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (t *TenantRepositoryImpl) GetAmountOfObjectsAndTotalSizeByTenantId(id string) (int64, int64, error) {
	row := t.Db.QueryRow(context.Background(), GetAmountOfObjectsAndTotalSizeByTenantId, id)
	var amount zeronull.Int8
	var size zeronull.Int8
	err := row.Scan(&amount, &size)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "Could not execute query: %v", GetAmountOfObjectsAndTotalSizeByTenantId)
	}
	return int64(amount), int64(size), nil
}

func (t *TenantRepositoryImpl) UpdateTenant(tenant models.Tenant) error {
	_, err := t.Db.Exec(context.Background(), UpdateTenant, tenant.Name, tenant.Alias, tenant.Person, tenant.Email, tenant.Id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", UpdateTenant)
	}
	return nil
}

func (t *TenantRepositoryImpl) DeleteTenant(id string) error {
	_, err := t.Db.Exec(context.Background(), DeleteTenant, id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", DeleteTenant)
	}
	return nil
}

func (t *TenantRepositoryImpl) FindTenantById(id string) (models.Tenant, error) {
	var tenant models.Tenant
	err := t.Db.QueryRow(context.Background(), FindTenantById, id).Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId)
	if err != nil {
		return tenant, errors.Wrapf(err, "Could not execute query: %v", FindTenantById)
	}
	return tenant, nil
}

func (t *TenantRepositoryImpl) FindTenantByKey(key string) (models.Tenant, error) {
	var tenant models.Tenant
	query := fmt.Sprintf("SELECT t.name, t.alias, t.person, t.email, t.id, t.api_key_id  FROM Tenant t, api_key a"+
		" where t.api_key_id = a.id and a.key = '%s'", key)
	countRow := t.Db.QueryRow(context.Background(), query)
	err := countRow.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId)
	if err != nil {
		return tenant, errors.Wrapf(err, "Could not scan tenant for query: %v", query)
	}
	return tenant, nil
}

func (t *TenantRepositoryImpl) FindTenantByCollectionId(collectionId string) (models.Tenant, error) {
	var tenant models.Tenant
	query := fmt.Sprintf("SELECT t.name, t.alias, t.person, t.email, t.id, t.api_key_id  FROM TENANT t, COLLECTION c"+
		" where t.id = c.tenant_id and c.tenant_id = '%s'", collectionId)
	countRow := t.Db.QueryRow(context.Background(), query)
	err := countRow.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId)
	if err != nil {
		return tenant, errors.Wrapf(err, "Could not scan tenant for query: %v", query)
	}
	return tenant, nil
}

func (t *TenantRepositoryImpl) SaveTenant(tenant models.Tenant) error {
	_, err := t.Db.Exec(context.Background(), SaveTenant, tenant.Name, tenant.Alias, tenant.Person, tenant.Email, tenant.ApiKeyId)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", SaveTenant)
	}
	return nil
}

func (t *TenantRepositoryImpl) FindAllTenants() ([]models.Tenant, error) {
	rows, err := t.Db.Query(context.Background(), FindAllTenants)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query: %v", FindAllTenants)
	}
	defer rows.Close()
	var tenants []models.Tenant

	for rows.Next() {
		var tenant models.Tenant
		err := rows.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for query: %v", FindAllTenants)
		}
		tenants = append(tenants, tenant)
	}
	return tenants, nil
}

func (t *TenantRepositoryImpl) FindAllTenantsPaginated(pagination models.Pagination) ([]models.Tenant, int, error) {
	firstCondition := ""
	secondCondition := ""
	if len(pagination.AllowedTenants) != 0 {
		tenants := strings.Join(pagination.AllowedTenants, "','")
		firstCondition = fmt.Sprintf("where t.id in ('%s')", tenants)
	}
	if firstCondition == "" {
		firstCondition = "where"
	} else {
		secondCondition = "and"
	}

	query := fmt.Sprintf("SELECT *,  count(*) over() as total_items FROM TENANT t %s %s %s order by %s %s limit %s OFFSET %s",
		firstCondition, secondCondition, getLikeQueryForTenant(pagination.SearchField), pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := t.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan tenant for query: %v", query)
	}
	defer rows.Close()
	var tenants []models.Tenant
	var totalItems int

	for rows.Next() {
		var tenant models.Tenant
		err := rows.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		tenants = append(tenants, tenant)
	}
	return tenants, totalItems, nil
}

func getLikeQueryForTenant(searchKey string) string {
	return strings.Replace("(t.id::text like '_search_key_%' or lower(t.alias) like '%_search_key_%'"+
		" or (t.name) like '%_search_key_%' or lower(t.email) like '%_search_key_%' or lower(t.person) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
