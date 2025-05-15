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
	GetObjectInstanceCheckById                = "GetObjectInstanceCheckById"
	CreateObjectInstanceCheck                 = "CreateObjectInstanceCheck"
	GetObjectInstanceChecksByObjectInstanceId = "GetObjectInstanceChecksByObjectInstanceId"
)

type ObjectInstanceCheckRepositoryImpl struct {
	Db *pgxpool.Pool
}

func CreateObjectInstanceCheckPreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	preparedStatements := map[string]string{
		GetObjectInstanceCheckById: "SELECT * FROM OBJECT_INSTANCE_CHECK WHERE ID = $1",
		CreateObjectInstanceCheck: "INSERT INTO OBJECT_INSTANCE_CHECK(error, message, object_instance_id, check_type)" +
			" VALUES ($1, $2, $3, $4) RETURNING id",
		GetObjectInstanceChecksByObjectInstanceId: `SELECT oic.checktime, oic.error, oic.message, oic.id, oic.object_instance_id, oic.check_type
													FROM (
													SELECT ROW_NUMBER() over(PARTITION BY object_instance_id ORDER BY checktime DESC) AS number_of_row, *
													FROM object_instance_check
													) AS oic 
													WHERE OBJECT_INSTANCE_ID = $1
													AND oic.number_of_row <= 3`,
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (o *ObjectInstanceCheckRepositoryImpl) CreateObjectInstanceCheck(objectInstanceCheck models.ObjectInstanceCheck) (string, error) {

	row := o.Db.QueryRow(context.Background(), CreateObjectInstanceCheck, objectInstanceCheck.Error, objectInstanceCheck.Message, objectInstanceCheck.ObjectInstanceId, objectInstanceCheck.CheckType)

	var id string
	err := row.Scan(&id)
	if err != nil {
		return "", errors.Wrapf(err, "Could not execute query for method: %v", CreateObjectInstanceCheck)
	}
	return id, nil
}

func (o *ObjectInstanceCheckRepositoryImpl) GetObjectInstanceCheckById(id string) (models.ObjectInstanceCheck, error) {
	objectInstanceCheck := models.ObjectInstanceCheck{}
	var checkTime time.Time
	err := o.Db.QueryRow(context.Background(), GetObjectInstanceCheckById, id).Scan(&checkTime, &objectInstanceCheck.Error, &objectInstanceCheck.Message, &objectInstanceCheck.Id, &objectInstanceCheck.ObjectInstanceId, &objectInstanceCheck.CheckType)
	if err != nil {
		return models.ObjectInstanceCheck{}, errors.Wrapf(err, "Could not execute query for method: %v", GetObjectInstanceCheckById)
	}
	objectInstanceCheck.CheckTime = checkTime.Format(Layout)
	return objectInstanceCheck, err
}

func (o *ObjectInstanceCheckRepositoryImpl) GetObjectInstanceChecksByObjectInstanceId(id string) ([]models.ObjectInstanceCheck, error) {

	rows, err := o.Db.Query(context.Background(), GetObjectInstanceChecksByObjectInstanceId, id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not execute query for method: %v", GetObjectInstanceChecksByObjectInstanceId)
	}
	defer rows.Close()
	var objectInstanceChecks []models.ObjectInstanceCheck

	for rows.Next() {
		var objectInstanceCheck models.ObjectInstanceCheck
		var checkTime time.Time
		err := rows.Scan(&checkTime, &objectInstanceCheck.Error, &objectInstanceCheck.Message, &objectInstanceCheck.Id, &objectInstanceCheck.ObjectInstanceId, &objectInstanceCheck.CheckType)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not scan rows for method: %v", GetObjectInstanceChecksByObjectInstanceId)
		}
		objectInstanceCheck.CheckTime = checkTime.Format(Layout)
		objectInstanceChecks = append(objectInstanceChecks, objectInstanceCheck)
	}

	return objectInstanceChecks, nil
}

func (o *ObjectInstanceCheckRepositoryImpl) GetObjectInstanceChecksByObjectInstanceIdPaginated(pagination models.Pagination) ([]models.ObjectInstanceCheck, int, error) {
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
	query := fmt.Sprintf("SELECT oic.* FROM OBJECT_INSTANCE_CHECK oic"+
		" inner join object_instance oi on oic.object_instance_id = oi.id"+
		" inner join object o on oi.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForObjectInstanceCheck(pagination.SearchField), "oic."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := o.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	defer rows.Close()
	var objectInstanceChecks []models.ObjectInstanceCheck

	for rows.Next() {
		var objectInstanceCheck models.ObjectInstanceCheck
		var checkTime time.Time
		err := rows.Scan(&checkTime, &objectInstanceCheck.Error, &objectInstanceCheck.Message, &objectInstanceCheck.Id, &objectInstanceCheck.ObjectInstanceId, &objectInstanceCheck.CheckType)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		objectInstanceCheck.CheckTime = checkTime.Format(Layout)
		objectInstanceChecks = append(objectInstanceChecks, objectInstanceCheck)
	}
	countQuery := fmt.Sprintf("SELECT count(*) as total_items FROM OBJECT_INSTANCE_CHECK oic"+
		" inner join object_instance oi on oic.object_instance_id = oi.id"+
		" inner join object o on oi.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s ", firstCondition, secondCondition, getLikeQueryForObjectInstanceCheck(pagination.SearchField))
	var totalItems int
	countRow := o.Db.QueryRow(context.Background(), countQuery)
	err = countRow.Scan(&totalItems)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan countRow for query: %v", countQuery)
	}
	return objectInstanceChecks, totalItems, nil
}

func NewObjectInstanceCheckRepository(db *pgxpool.Pool) ObjectInstanceCheckRepository {
	return &ObjectInstanceCheckRepositoryImpl{Db: db}
}

func getLikeQueryForObjectInstanceCheck(searchKey string) string {
	return strings.Replace("(oic.id::text like '_search_key_%' or lower(oic.message) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
