package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"fmt"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"slices"
	"strconv"
	"strings"
)

type objectInstanceCheckRepositoryStmt int

const (
	GetObjectInstanceCheckById objectInstanceCheckRepositoryStmt = iota
	CreateObjectInstanceCheck
)

type objectInstanceCheckRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[objectInstanceCheckRepositoryStmt]*sql.Stmt
}

func (o *objectInstanceCheckRepositoryImpl) CreateObjectInstanceCheckPreparedStatements() error {

	preparedStatement := map[objectInstanceCheckRepositoryStmt]string{
		GetObjectInstanceCheckById: fmt.Sprintf("SELECT * FROM %s.OBJECT_INSTANCE_CHECK WHERE ID = $1", o.Schema),
		CreateObjectInstanceCheck: fmt.Sprintf("INSERT INTO %s.OBJECT_INSTANCE_CHECK(error, message, object_instance_id)"+
			" VALUES ($1, $2, $3) RETURNING id", o.Schema),
	}
	var err error
	o.PreparedStatement = make(map[objectInstanceCheckRepositoryStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		o.PreparedStatement[key], err = o.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (o *objectInstanceCheckRepositoryImpl) CreateObjectInstanceCheck(objectInstanceCheck models.ObjectInstanceCheck) (string, error) {

	row := o.PreparedStatement[CreateObjectInstanceCheck].QueryRow(objectInstanceCheck.Error, objectInstanceCheck.Message, objectInstanceCheck.ObjectInstanceId)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query: %v", o.PreparedStatement[CreateObjectInstanceCheck])
	}
	return id, nil
}

func (o *objectInstanceCheckRepositoryImpl) GetObjectInstanceCheckById(id string) (models.ObjectInstanceCheck, error) {
	objectInstanceCheck := models.ObjectInstanceCheck{}
	err := o.PreparedStatement[GetObjectInstanceCheckById].QueryRow(id).Scan(&objectInstanceCheck.CheckTime, &objectInstanceCheck.Error, &objectInstanceCheck.Message, &objectInstanceCheck.Id, &objectInstanceCheck.ObjectInstanceId)
	return objectInstanceCheck, err
}

func (o *objectInstanceCheckRepositoryImpl) GetObjectInstanceChecksByObjectInstanceIdPaginated(pagination models.Pagination) ([]models.ObjectInstanceCheck, int, error) {
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
		firstCondition = fmt.Sprintf("where oic.object_instance_id = '%s'", pagination.Id)
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
	query := strings.Replace(fmt.Sprintf("SELECT oic.* FROM _schema.OBJECT_INSTANCE_CHECK oic"+
		" inner join _schema.object_instance oi on oic.object_instance_id = oi.id"+
		" inner join _schema.object o on oi.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObjectInstanceCheck(pagination.SearchField), "oic."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", o.Schema, -1)
	rows, err := o.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var objectInstanceChecks []models.ObjectInstanceCheck

	for rows.Next() {
		var objectInstanceCheck models.ObjectInstanceCheck
		err := rows.Scan(&objectInstanceCheck.CheckTime, &objectInstanceCheck.Error, &objectInstanceCheck.Message, &objectInstanceCheck.Id, &objectInstanceCheck.ObjectInstanceId)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		objectInstanceChecks = append(objectInstanceChecks, objectInstanceCheck)
	}
	countQuery := strings.Replace(fmt.Sprintf("SELECT count(*) as total_items FROM _schema.OBJECT_INSTANCE_CHECK oic"+
		" inner join _schema.object_instance oi on oic.object_instance_id = oi.id"+
		" inner join _schema.object o on oi.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s ", firstCondition, secondCondition, getLikeQueryForObjectInstanceCheck(pagination.SearchField)), "_schema", o.Schema, -1)
	var totalItems int
	countRow := o.Db.QueryRow(countQuery)
	err = countRow.Scan(&totalItems)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan countRow for query: %v", countQuery)
	}
	return objectInstanceChecks, totalItems, nil
}

func NewObjectInstanceCheckRepository(db *sql.DB, schema string) ObjectInstanceCheckRepository {
	return &objectInstanceCheckRepositoryImpl{Db: db, Schema: schema}
}

func getLikeQueryForObjectInstanceCheck(searchKey string) string {
	return strings.Replace("(oic.id::text like '_search_key_%' or lower(oic.message) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
