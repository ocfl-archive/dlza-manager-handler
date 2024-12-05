package repository

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/pkg/errors"
)

type TransactionRepositoryImpl struct {
	Db     *sql.DB
	Schema string
}

func NewTransactionRepository(Db *sql.DB, schema string) TransactionRepository {
	return &TransactionRepositoryImpl{
		Db:     Db,
		Schema: schema,
	}
}

func (t TransactionRepositoryImpl) SaveAllTableObjectsAfterCopying(instanceWithPartitionAndObjectWithFiles []*pb.InstanceWithPartitionAndObjectWithFile) error {
	var time any
	if instanceWithPartitionAndObjectWithFiles[0].Object.Expiration == "" {
		time = nil
	} else {
		time = instanceWithPartitionAndObjectWithFiles[0].Object.Expiration
	}

	tx, err := t.Db.Begin()
	if err != nil {
		return errors.Wrapf(err, "Could not creating transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles[0].ObjectInstance.Path)
	}

	//////// CREATE OBJECT
	queryCreateObject := fmt.Sprintf("INSERT INTO %s.OBJECT(signature, \"sets\", identifiers, title, alternative_titles, description, keywords, \"references\","+
		" ingest_workflow, \"user\", address, \"size\", collection_id, checksum, authors, expiration, holding, head, versions)"+
		" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING id", t.Schema)
	object := instanceWithPartitionAndObjectWithFiles[0].Object
	var objectId string
	err = tx.QueryRow(queryCreateObject, object.Signature, pq.Array(object.Sets), pq.Array(object.Identifiers), object.Title, pq.Array(object.AlternativeTitles), object.Description,
		pq.Array(object.Keywords), pq.Array(object.References), object.IngestWorkflow, object.User, object.Address, object.Size, object.CollectionId, object.Checksum, pq.Array(object.Authors), time, object.Holding, object.Head, object.Versions).Scan(&objectId)
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateObject)
	}

	//////// CREATE FILES
	queryCreateFile := fmt.Sprintf("insert into %s.File(checksum, \"name\", \"size\", mime_type, pronom, width, height, duration, object_id) values($1, $2, $3, $4, $5, $6, $7, $8, $9)", t.Schema)

	for _, file := range instanceWithPartitionAndObjectWithFiles {
		file.File.ObjectId = objectId
		_, err = tx.Exec(queryCreateFile, file.File.Checksum, pq.Array(file.File.Name), file.File.Size, file.File.MimeType, file.File.Pronom, file.File.Width, file.File.Height, file.File.Duration, file.File.ObjectId)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateFile)
		}
	}

	//////// UPDATE STORAGE PARTITION
	queryUpdateStoragePartition := fmt.Sprintf("UPDATE %s.STORAGE_PARTITION set name = $1, max_size = $2, max_objects = $3, current_size = $4, current_objects = $5 where id =$6", t.Schema)
	partition := instanceWithPartitionAndObjectWithFiles[0].StoragePartition
	_, err = tx.Exec(queryUpdateStoragePartition, partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.Id)
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryUpdateStoragePartition)
	}
	//////// CREATE OBJECT INSTANCE
	queryCreateObjectInstance := fmt.Sprintf("INSERT INTO %s.OBJECT_INSTANCE(\"path\", \"size\", status, storage_partition_id, object_id) VALUES ($1, $2, $3, $4, $5) RETURNING id", t.Schema)
	objectInstance := instanceWithPartitionAndObjectWithFiles[0].ObjectInstance
	var objectInstanceId string
	err = tx.QueryRow(queryCreateObjectInstance, objectInstance.Path, objectInstance.Size, objectInstance.Status, objectInstance.StoragePartitionId, objectId).Scan(&objectInstanceId)
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateObjectInstance)
	}

	// COMMIT TRANSACTION
	if err = tx.Commit(); err != nil {
		return errors.Wrapf(err, "Could not commit transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles[0].ObjectInstance.Path)
	}

	return nil
}
