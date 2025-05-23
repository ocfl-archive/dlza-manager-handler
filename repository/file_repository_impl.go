package repository

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype/zeronull"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ocfl-archive/dlza-manager/models"
	"slices"
	"strconv"
	"strings"
)

const (
	CreateFile  = "CreateFile"
	DeleteFile  = "DeleteFile"
	GetFileById = "GetFileById"
	UNKNOWN     = "UNKNOWN"
)

type FileRepositoryImpl struct {
	Db *pgxpool.Pool
}

func NewFileRepository(db *pgxpool.Pool) FileRepository {
	return &FileRepositoryImpl{
		Db: db,
	}
}

func CreateFilePreparedStatements(ctx context.Context, conn *pgx.Conn) error {

	preparedStatements := map[string]string{
		CreateFile:  "insert into File(checksum, \"name\", \"size\", mime_type, pronom, width, height, duration, object_id) values($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		DeleteFile:  "DELETE FROM File WHERE id = $1",
		GetFileById: "SELECT * FROM FILE WHERE id = $1",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (f *FileRepositoryImpl) GetFileById(id string) (models.File, error) {
	var file models.File
	var width zeronull.Int8
	var height zeronull.Int8
	var duration zeronull.Int8
	err := f.Db.QueryRow(context.Background(), GetFileById, id).Scan(&file.Checksum, &file.Name, &file.Size, &file.MimeType,
		&file.Pronom, &width, &height, &duration, &file.Id, &file.ObjectId)
	if err != nil {
		return file, errors.Wrapf(err, "Could not execute query for method: %v", GetFileById)
	}
	file.Width = int64(width)
	file.Height = int64(height)
	file.Duration = int64(duration)
	return file, nil
}

func (f *FileRepositoryImpl) DeleteFile(id string) error {
	_, err := f.Db.Exec(context.Background(), DeleteFile, id)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", DeleteFile)
	}
	return nil
}

func (f *FileRepositoryImpl) CreateFile(file models.File) error {
	_, err := f.Db.Exec(context.Background(), CreateFile, file.Checksum, file.Name, file.Size, file.MimeType, file.Pronom, file.Width, file.Height, file.Duration, file.ObjectId)
	if err != nil {
		return errors.Wrapf(err, "Could not execute query for method: %v", CreateFile)
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

	query := fmt.Sprintf("SELECT f.* FROM FILE f"+
		" inner join object o on f.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField, firstCondition, secondCondition), "f."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := f.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	var files []models.File
	for rows.Next() {
		var file models.File
		var width zeronull.Int8
		var height zeronull.Int8
		var duration zeronull.Int8
		err := rows.Scan(&file.Checksum, &file.Name, &file.Size, &file.MimeType,
			&file.Pronom, &width, &height, &duration, &file.Id, &file.ObjectId)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		file.Width = int64(width)
		file.Height = int64(height)
		file.Duration = int64(duration)
		files = append(files, file)
	}
	countQuery := fmt.Sprintf("SELECT count(*) as total_items FROM FILE f"+
		" inner join object o on f.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField, firstCondition, secondCondition))
	var totalItems int
	countRow := f.Db.QueryRow(context.Background(), countQuery)
	err = countRow.Scan(&totalItems)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan countRow for query: %s", countQuery)
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

	query := fmt.Sprintf("SELECT f.* FROM FILE f"+
		" inner join object o on f.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField, firstCondition, secondCondition), "f."+pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := f.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	var files []models.File
	for rows.Next() {
		var file models.File
		var width zeronull.Int8
		var height zeronull.Int8
		var duration zeronull.Int8
		err := rows.Scan(&file.Checksum, &file.Name, &file.Size, &file.MimeType,
			&file.Pronom, &width, &height, &duration, &file.Id, &file.ObjectId)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		file.Width = int64(width)
		file.Height = int64(height)
		file.Duration = int64(duration)
		files = append(files, file)
	}

	countQuery := fmt.Sprintf("SELECT count(*) as total_items FROM FILE f"+
		" inner join object o on f.object_id = o.id"+
		" inner join collection c on c.id = o.collection_id"+
		" inner join tenant t on t.id = c.tenant_id"+
		" %s %s %s ", firstCondition, secondCondition, getLikeQueryForFile(pagination.SearchField, firstCondition, secondCondition))
	var totalItems int
	countRow := f.Db.QueryRow(context.Background(), countQuery)
	err = countRow.Scan(&totalItems)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not scan countRow for query: %s", countQuery)
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

	query := fmt.Sprintf("SELECT mtfj.mime_type as id, count(mtfj.*) as file_count, sum(mtfj.size) as files_size,  count(*) OVER() FROM mat_tenant_file_join mtfj"+
		" %s %s group by mtfj.mime_type order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := f.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %v", query)
	}
	defer rows.Close()
	var totalItems int
	mimeTypes := make([]models.MimeType, 0)
	emptyMimeType := models.MimeType{}
	notLast := rows.Next()
	for notLast {
		mimeType := models.MimeType{}
		var id zeronull.Text
		err := rows.Scan(&id, &mimeType.FileCount, &mimeType.FilesSize, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		mimeType.Id = string(id)
		if mimeType.Id == "" {
			mimeType.Id = UNKNOWN
		}
		if mimeType.Id == UNKNOWN {
			if emptyMimeType.Id == "" {
				emptyMimeType = mimeType
			} else {
				emptyMimeType.FileCount = mimeType.FileCount + emptyMimeType.FileCount
				emptyMimeType.FilesSize = mimeType.FilesSize + emptyMimeType.FilesSize
			}
		}
		if mimeType.Id != UNKNOWN {
			mimeTypes = append(mimeTypes, mimeType)
		}
		notLast = rows.Next()
		if !notLast && emptyMimeType.Id != "" {
			mimeTypes = append(mimeTypes, emptyMimeType)
		}
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

	query := fmt.Sprintf("SELECT mtfj.pronom as id, count(mtfj.*) as file_count, sum(mtfj.size) as files_size, count(*) OVER() FROM mat_tenant_file_join mtfj"+
		" %s %s group by mtfj.pronom order by %s %s limit %s OFFSET %s ", firstCondition, secondCondition, pagination.SortKey, pagination.SortDirection, strconv.Itoa(pagination.Take), strconv.Itoa(pagination.Skip))
	rows, err := f.Db.Query(context.Background(), query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Could not execute query: %s", query)
	}
	defer rows.Close()
	var totalItems int
	pronoms := make([]models.Pronom, 0)
	emptyPronom := models.Pronom{}
	notLast := rows.Next()
	for notLast {
		pronom := models.Pronom{}
		var id zeronull.Text
		err := rows.Scan(&id, &pronom.FileCount, &pronom.FilesSize, &totalItems)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Could not scan rows for query: %s", query)
		}
		pronomWithoutSpaces := strings.Replace(string(id), " ", "", -1)
		pronom.Id = pronomWithoutSpaces
		if pronom.Id == "" {
			pronom.Id = UNKNOWN
		}
		if pronom.Id == UNKNOWN {
			if emptyPronom.Id == "" {
				emptyPronom = pronom
			} else {
				emptyPronom.FileCount = pronom.FileCount + emptyPronom.FileCount
				emptyPronom.FilesSize = pronom.FilesSize + emptyPronom.FilesSize
			}
		}
		if pronom.Id != UNKNOWN {
			pronoms = append(pronoms, pronom)
		}
		notLast = rows.Next()
		if !notLast && emptyPronom.Id != "" {
			pronoms = append(pronoms, emptyPronom)
		}
	}
	return pronoms, totalItems, nil
}

func getLikeQueryForFile(searchKey string, firstCondition string, secondCondition string) string {
	if searchKey != "" {
		condition := ""
		if firstCondition == "" && secondCondition == "" {
			condition = "where"
		} else {
			condition = "and"
		}
		return condition + strings.Replace(" (f.id::text like '_search_key_%' or lower(f.name::text) like '%_search_key_%' or f.checksum like '%_search_key_%'"+
			" or lower(f.pronom) like '%_search_key_%' or lower(f.mime_type) like '%_search_key_%')",
			"_search_key_", searchKey, -1)
	}
	return ""
}
