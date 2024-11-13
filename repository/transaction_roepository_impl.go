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

func (t TransactionRepositoryImpl) SaveAllTableObjectsAfterCopying(instanceWithPartitionAndObjectWithFiles *pb.InstanceWithPartitionAndObjectWithFiles) error {
	var time any
	if instanceWithPartitionAndObjectWithFiles.ObjectAndFiles.Object.Expiration == "" {
		time = nil
	} else {
		time = instanceWithPartitionAndObjectWithFiles.ObjectAndFiles.Object.Expiration
	}
	ctx := context.Background()
	tx, err := t.Db.Begin(ctx)
	if err != nil {
		return errors.Wrapf(err, "Could not creating transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles.ObjectInstance.Path)
	}

	//////// CREATE OBJECT
	queryCreateObject := "INSERT INTO OBJECT(signature, \"sets\", identifiers, title, alternative_titles, description, keywords, \"references\"," +
		" ingest_workflow, \"user\", address, \"size\", collection_id, checksum, authors, expiration, holding)" +
		" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING id"
	objectIns := instanceWithPartitionAndObjectWithFiles.ObjectAndFiles.Object
	var objectId string
	err = tx.QueryRow(ctx, queryCreateObject, objectIns.Signature, objectIns.Sets, objectIns.Identifiers, objectIns.Title, objectIns.AlternativeTitles, objectIns.Description,
		objectIns.Keywords, objectIns.References, objectIns.IngestWorkflow, objectIns.User, objectIns.Address, objectIns.Size, objectIns.CollectionId, objectIns.Checksum, objectIns.Authors, time, objectIns.Holding).Scan(&objectId)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateObject)
	}

	//////// CREATE FILES
	queryCreateFile := "insert into File(checksum, \"name\", \"size\", mime_type, pronom, width, height, duration, object_id) values($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	for _, file := range instanceWithPartitionAndObjectWithFiles.ObjectAndFiles.Files {
		file.ObjectId = objectId
		_, err = tx.Exec(ctx, queryCreateFile, file.Checksum, file.Name, file.Size, file.MimeType, file.Pronom, file.Width, file.Height, file.Duration, file.ObjectId)
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateFile)
		}
	}

	//////// UPDATE STORAGE PARTITION
	queryUpdateStoragePartition := "UPDATE STORAGE_PARTITION set name = $1, max_size = $2, max_objects = $3, current_size = $4, current_objects = $5 where id =$6"
	partition := instanceWithPartitionAndObjectWithFiles.StoragePartition
	_, err = tx.Exec(ctx, queryUpdateStoragePartition, partition.Name, partition.MaxSize, partition.MaxObjects, partition.CurrentSize, partition.CurrentObjects, partition.Id)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryUpdateStoragePartition)
	}
	//////// CREATE OBJECT INSTANCE
	queryCreateObjectInstance := "INSERT INTO OBJECT_INSTANCE(\"path\", \"size\", status, storage_partition_id, object_id) VALUES ($1, $2, $3, $4, $5) RETURNING id"
	objectInstance := instanceWithPartitionAndObjectWithFiles.ObjectInstance
	var objectInstanceId string
	err = tx.QueryRow(ctx, queryCreateObjectInstance, objectInstance.Path, objectInstance.Size, objectInstance.Status, objectInstance.StoragePartitionId, objectId).Scan(&objectInstanceId)
	if err != nil {
		tx.Rollback(ctx)
		return errors.Wrapf(err, "Could not exequte query: '%s'", queryCreateObjectInstance)
	}

	// COMMIT TRANSACTION
	if err = tx.Commit(ctx); err != nil {
		return errors.Wrapf(err, "Could not commit transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles.ObjectInstance.Path)
	}

	return nil
}
