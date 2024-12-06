package repository

import (
	"context"
	"emperror.dev/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
)

type TransactionRepositoryImpl struct {
	Db *pgxpool.Pool
}

func NewTransactionRepository(Db *pgxpool.Pool) TransactionRepository {
	return &TransactionRepositoryImpl{
		Db: Db,
	}
}

func (t TransactionRepositoryImpl) SaveAllTableObjectsAfterCopying(instanceWithPartitionAndObjectWithFiles []*pb.InstanceWithPartitionAndObjectWithFile) error {
	var time any
	if instanceWithPartitionAndObjectWithFiles[0].Object.Expiration == "" {
		time = nil
	} else {
		time = instanceWithPartitionAndObjectWithFiles[0].Object.Expiration
	}
	ctx := context.Background()
	tx, err := t.Db.Begin(ctx)
	if err != nil {
		return errors.Wrapf(err, "Could not creating transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles[0].ObjectInstance.Path)
	}

	//////// CREATE OBJECT
	queryCreateObject := "INSERT INTO OBJECT(signature, \"sets\", identifiers, title, alternative_titles, description, keywords, \"references\"," +
		" ingest_workflow, \"user\", address, \"size\", collection_id, checksum, authors, holding, expiration, head, versions)" +
		" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING id"
	objectIns := instanceWithPartitionAndObjectWithFiles[0].Object
	var objectId string
	err = tx.QueryRow(ctx, queryCreateObject, objectIns.Signature, objectIns.Sets, objectIns.Identifiers, objectIns.Title, objectIns.AlternativeTitles, objectIns.Description,
		objectIns.Keywords, objectIns.References, objectIns.IngestWorkflow, objectIns.User, objectIns.Address, objectIns.Size, objectIns.CollectionId, objectIns.Checksum, objectIns.Authors, objectIns.Holding, time, objectIns.Head, objectIns.Versions).Scan(&objectId)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateObject)
	}

	//////// CREATE FILES
	queryCreateFile := "insert into File(checksum, \"name\", \"size\", mime_type, pronom, width, height, duration, object_id) values($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	for _, file := range instanceWithPartitionAndObjectWithFiles.ObjectAndFiles.Files {
		file.File.ObjectId = objectId
		_, err = tx.Exec(ctx, queryCreateFile, file.File.Checksum, file.File.Name, file.File.Size, file.File.MimeType, file.File.Pronom, file.File.Width, file.File.Height, file.File.Duration, file.File.ObjectId)
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateFile)
		}
	}

	//////// UPDATE STORAGE PARTITION
	queryUpdateStoragePartition := "UPDATE STORAGE_PARTITION set name = $1, max_size = $2, max_objects = $3, current_size = $4, current_objects = $5 where id =$6"
	partition := instanceWithPartitionAndObjectWithFiles[0].StoragePartition
	_, err = tx.Exec(ctx, queryUpdateStoragePartition, partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.Id)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryUpdateStoragePartition)
	}
	//////// CREATE OBJECT INSTANCE
	queryCreateObjectInstance := "INSERT INTO OBJECT_INSTANCE(\"path\", \"size\", status, storage_partition_id, object_id) VALUES ($1, $2, $3, $4, $5) RETURNING id"
	instanceWithPartitionAndObjectWithFiles[0].ObjectInstance
	var objectInstanceId string
	err = tx.QueryRow(ctx, queryCreateObjectInstance, objectInstance.Path, objectInstance.Size, objectInstance.Status, objectInstance.StoragePartitionId, objectId).Scan(&objectInstanceId)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateObjectInstance)
	}

	// COMMIT TRANSACTION
	if err = tx.Commit(ctx); err != nil {
		return errors.Wrapf(err, "Could not commit transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles[0].ObjectInstance.Path)
	}

	return nil
}
