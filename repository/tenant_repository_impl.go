package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"log"
	"strconv"
	"strings"
)

type tenantPrepareStmt int

const (
	FindAllTenants tenantPrepareStmt = iota
	FindTenantById
	SaveTenant
	UpdateTenant
	DeleteTenant
	GetAmountOfObjectsAndTotalSizeByTenantId
)

type TenantRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[tenantPrepareStmt]*sql.Stmt
}

func NewTenantRepository(Db *sql.DB, schema string) TenantRepository {
	return &TenantRepositoryImpl{
		Db:     Db,
		Schema: schema,
	}
}

func (t *TenantRepositoryImpl) CreatePreparedStatements() error {

	preparedStatement := map[tenantPrepareStmt]string{
		FindAllTenants: fmt.Sprintf("SELECT * FROM %s.TENANT", t.Schema),
		FindTenantById: fmt.Sprintf("SELECT * FROM %s.TENANT WHERE id = $1", t.Schema),
		SaveTenant:     fmt.Sprintf("insert into %s.TENANT(name, alias, person, email, api_key_id) values($1, $2, $3, $4, $5)", t.Schema),
		UpdateTenant:   fmt.Sprintf("update %s.TENANT set name = $1, alias = $2, person = $3, email = $4 where id =$5", t.Schema),
		DeleteTenant:   fmt.Sprintf("DELETE FROM %s.TENANT WHERE id = $1", t.Schema),
		GetAmountOfObjectsAndTotalSizeByTenantId: strings.Replace("select sum(current_objects),  sum(current_size) from %s.storage_partition sp, %s.storage_location sl"+
			" where sl.id = sp.storage_location_id"+
			" and sl.tenant_id = $1"+
			" group by sl.tenant_id", "%s", t.Schema, -1),
	}
	var err error
	t.PreparedStatement = make(map[tenantPrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		t.PreparedStatement[key], err = t.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (t *TenantRepositoryImpl) GetAmountOfObjectsAndTotalSizeByTenantId(id string) (int64, int64, error) {
	row := t.PreparedStatement[GetAmountOfObjectsAndTotalSizeByTenantId].QueryRow(id)
	var amount int64
	var size int64
	err := row.Scan(&amount, &size)
	if err != nil {
		return amount, size, errors.Wrapf(err, "Could not execute query: %v", t.PreparedStatement[GetAmountOfObjectsAndTotalSizeByTenantId])
	}
	return amount, size, nil
}

func (t *TenantRepositoryImpl) UpdateTenant(tenant models.Tenant) error {
	_, err := t.PreparedStatement[UpdateTenant].Exec(tenant.Name, tenant.Alias, tenant.Person, tenant.Email, tenant.Id)
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}

func (t *TenantRepositoryImpl) DeleteTenant(id string) error {
	_, err := t.PreparedStatement[DeleteTenant].Exec(id)
	if err != nil {
		log.Printf("err message: %v", err)
		return err
	}
	return nil
}

func (t *TenantRepositoryImpl) FindTenantById(id string) (models.Tenant, error) {
	var tenant models.Tenant
	err := t.PreparedStatement[FindTenantById].QueryRow(id).Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId.String)
	if err != nil {
		log.Print(err)
		return tenant, err
	}
	return tenant, nil
}

func (t *TenantRepositoryImpl) FindTenantByKey(key string) (models.Tenant, error) {
	var tenant models.Tenant
	query := strings.Replace(fmt.Sprintf("SELECT t.name, t.alias, t.person, t.email, t.id, t.api_key_id  FROM _schema.Tenant t, _schema.api_key a"+
		" where t.api_key_id = a.id and a.key = '%s'", key), "_schema", t.Schema, -1)
	countRow := t.Db.QueryRow(query)
	err := countRow.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId)
	if err != nil {
		return tenant, errors.Wrapf(err, "Could not scan tenant for query: %v", query)
	}
	return tenant, nil
}

func (t *TenantRepositoryImpl) SaveTenant(tenant models.Tenant) error {
	_, err := t.PreparedStatement[SaveTenant].Exec(tenant.Name, tenant.Alias, tenant.Person, tenant.Email, tenant.ApiKeyId.String)
	if err != nil {
		log.Printf("err message: %v", err)
		return err
	}
	return nil
}

func (t *TenantRepositoryImpl) FindAllTenants() ([]models.Tenant, error) {
	rows, err := t.PreparedStatement[FindAllTenants].Query()
	if err != nil {
		log.Printf("Could not execute query: %v", t.PreparedStatement[FindAllTenants])
		return nil, err
	}
	var tenants []models.Tenant

	for rows.Next() {
		var tenant models.Tenant
		err := rows.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId)
		if err != nil {
			log.Printf("Could not scan rows for query: %v", t.PreparedStatement[FindAllTenants])
			return nil, err
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

	query := fmt.Sprintf("SELECT *,  count(*) over() as total_items FROM %s.TENANT t %s %s %s order by %s %s limit %s OFFSET %s", t.Schema,
		firstCondition, secondCondition, getLikeQueryForTenant(pagination.SearchField), pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := t.Db.Query(query)
	if err != nil {
		log.Printf("Could not execute query: %v", query)
		return nil, 0, err
	}
	var tenants []models.Tenant
	var totalItems int

	for rows.Next() {
		var tenant models.Tenant
		err := rows.Scan(&tenant.Name, &tenant.Alias, &tenant.Person, &tenant.Email, &tenant.Id, &tenant.ApiKeyId, &totalItems)
		if err != nil {
			log.Printf("Could not scan rows for query: %v", query)
			return nil, 0, err
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
