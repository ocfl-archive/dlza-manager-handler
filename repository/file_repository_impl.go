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
)

type filePrepareStmt int

const (
	CreateFile filePrepareStmt = iota
	DeleteFile
	GetFileById
)

type FileRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[filePrepareStmt]*sql.Stmt
}

func NewFileRepository(Db *sql.DB, schema string) FileRepository {
	return &FileRepositoryImpl{
		Db:     Db,
		Schema: schema,
	}
}

func (f *FileRepositoryImpl) CreateFilePreparedStatements() error {

	preparedStatement := map[filePrepareStmt]string{
		CreateFile:  fmt.Sprintf("insert into %s.File(checksum, \"name\", \"size\", mime_type, pronom, width, height, duration, object_id) values($1, $2, $3, $4, $5, $6, $7, $8, $9)", f.Schema),
		DeleteFile:  fmt.Sprintf("DELETE FROM %s.File WHERE id = $1", f.Schema),
		GetFileById: fmt.Sprintf("SELECT * FROM %s.FILE WHERE id = $1", f.Schema),
	}
	var err error
	f.PreparedStatement = make(map[filePrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		f.PreparedStatement[key], err = f.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (f *FileRepositoryImpl) GetFileById(id string) (models.File, error) {
	var file models.File
	err := f.PreparedStatement[GetFileById].QueryRow(id).Scan(&file.Checksum, pq.Array(&file.Name), &file.Size, &file.MimeType,
		&file.Pronom, &file.Width, &file.Height, &file.Duration, &file.Id, &file.ObjectId)
	if err != nil {
		return file, errors.Wrapf(err, "Could not execute query: %v", f.PreparedStatement[GetFileById])
	}
	return file, nil
}

func (f *FileRepositoryImpl) DeleteFile(id string) error {
	_, err := f.PreparedStatement[DeleteFile].Exec(id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", f.PreparedStatement[DeleteFile])
	}
	return nil
}

func (f *FileRepositoryImpl) CreateFile(file models.File) error {
	_, err := f.PreparedStatement[CreateFile].Exec(file.Checksum, pq.Array(file.Name), file.Size, file.MimeType, file.Pronom, file.Width, file.Height, file.Duration, file.ObjectId)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query: %v", f.PreparedStatement[CreateFile])
	}
	return nil
}

func (f *FileRepositoryImpl) GetFilesByObjectIdPaginated(pagination models.Pagination) ([]models.File, int, error) {
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
		firstCondition = fmt.Sprintf("where f.object_id = '%s'", pagination.Id)
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
	query := strings.Replace(fmt.Sprintf("SELECT f.* FROM _schema.FILE f"+
		" inner join _schema.object o on f.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField), "f."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", f.Schema, -1)
	rows, err := f.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var files []models.File
	for rows.Next() {
		var file models.File
		err := rows.Scan(&file.Checksum, pq.Array(&file.Name), &file.Size, &file.MimeType,
			&file.Pronom, &file.Width, &file.Height, &file.Duration, &file.Id, &file.ObjectId)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		files = append(files, file)
	}

	countQuery := strings.Replace(fmt.Sprintf("SELECT count(*) as total_items FROM _schema.FILE f"+
		" inner join _schema.object o on f.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField)), "_schema", f.Schema, -1)
	var totalItems int
	countRow := f.Db.QueryRow(countQuery)
	err = countRow.Scan(&totalItems)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan countRow for query: %v", countQuery)
	}
	return files, totalItems, nil
}

func (f *FileRepositoryImpl) GetFilesByCollectionIdPaginated(pagination models.Pagination) ([]models.File, int, error) {
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
		firstCondition = fmt.Sprintf("where c.id = '%s'", pagination.Id)
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
	query := strings.Replace(fmt.Sprintf("SELECT f.* FROM _schema.FILE f"+
		" inner join _schema.object o on f.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField), "f."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", f.Schema, -1)
	rows, err := f.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var files []models.File
	for rows.Next() {
		var file models.File
		err := rows.Scan(&file.Checksum, pq.Array(&file.Name), &file.Size, &file.MimeType,
			&file.Pronom, &file.Width, &file.Height, &file.Duration, &file.Id, &file.ObjectId)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		files = append(files, file)
	}

	countQuery := strings.Replace(fmt.Sprintf("SELECT count(*) as total_items FROM _schema.FILE f"+
		" inner join _schema.object o on f.object_id = o.id"+
		" inner join _schema.collection c on c.id = o.collection_id"+
		" inner join _schema.tenant t on t.id = c.tenant_id"+
		" %s %s %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField)), "_schema", f.Schema, -1)
	var totalItems int
	countRow := f.Db.QueryRow(countQuery)
	err = countRow.Scan(&totalItems)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan countRow for query: %v", countQuery)
	}
	return files, totalItems, nil
}

func (f *FileRepositoryImpl) GetMimeTypesForCollectionId(pagination models.Pagination) ([]models.MimeType, int, error) {
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

	query := strings.Replace(fmt.Sprintf("SELECT mtfj.mime_type as id, count(mtfj.*) as file_count, count(*) OVER() FROM _schema.mat_tenant_file_join mtfj"+
		" %s %s group by mtfj.mime_type order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", f.Schema, -1)
	rows, err := f.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var totalItems int
	mimeTypes := make([]models.MimeType, 0)
	for rows.Next() {
		mimeType := models.MimeType{}
		err := rows.Scan(&mimeType.Id, &mimeType.FileCount, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		mimeTypes = append(mimeTypes, mimeType)
	}
	return mimeTypes, totalItems, nil
}

func (f *FileRepositoryImpl) GetPronomsForCollectionId(pagination models.Pagination) ([]models.Pronom, int, error) {
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

	query := strings.Replace(fmt.Sprintf("SELECT mtfj.pronom as id, count(mtfj.*) as file_count, count(*) OVER() FROM _schema.mat_tenant_file_join mtfj"+
		" %s %s group by mtfj.pronom order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip)), "_schema", f.Schema, -1)
	rows, err := f.Db.Query(query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}

	var totalItems int
	pronoms := make([]models.Pronom, 0)
	for rows.Next() {
		pronom := models.Pronom{}
		err := rows.Scan(&pronom.Id, &pronom.FileCount, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %v", query)
		}
		pronomWithoutSpaces := strings.Replace(pronom.Id.String, " ", "", -1)
		pronom.Id.String = pronomWithoutSpaces
		pronoms = append(pronoms, pronom)
	}
	return pronoms, totalItems, nil
}

func getLikeQueryForFile(searchKey string) string {
	return strings.Replace("(f.id::text like '_search_key_%' or lower(f.name::text) like '%_search_key_%' or f.checksum like '%_search_key_%'"+
		" or lower(f.pronom) like '%_search_key_%' or lower(f.mime_type) like '%_search_key_%')",
		"_search_key_", searchKey, -1)
}
