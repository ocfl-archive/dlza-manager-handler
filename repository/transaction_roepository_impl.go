package repository

import (
	"context"
	"emperror.dev/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"time"
)

type TransactionRepositoryImpl struct {
	Db                       *pgxpool.Pool
	ObjectRepository         ObjectRepository
	ObjectInstanceRepository ObjectInstanceRepository
}

func NewTransactionRepository(Db *pgxpool.Pool, objectRepository ObjectRepository, objectInstanceRepository ObjectInstanceRepository) TransactionRepository {
	return &TransactionRepositoryImpl{
		Db:                       Db,
		ObjectRepository:         objectRepository,
		ObjectInstanceRepository: objectInstanceRepository,
	}
}

func (t TransactionRepositoryImpl) SaveAllTableObjectsAfterCopying(instanceWithPartitionAndObjectWithFiles []*pb.InstanceWithPartitionAndObjectWithFile) error {
	var expirationTime any
	if instanceWithPartitionAndObjectWithFiles[0].Object.Expiration == "" {
		expirationTime = nil
	} else {
		expirationTime = instanceWithPartitionAndObjectWithFiles[0].Object.Expiration
	}
	ctx := context.Background()
	tx, err := t.Db.Begin(ctx)
	if err != nil {
		return errors.Wrapf(err, "Could not creating transaction storing object instance with path: '%s'", instanceWithPartitionAndObjectWithFiles[0].ObjectInstance.Path)
	}

	//////// CREATE/UPDATE OBJECT

	objectIns := instanceWithPartitionAndObjectWithFiles[0].Object
	var objectId string
	if objectIns.Head == "v1" {
		queryObject := "INSERT INTO OBJECT(signature, \"sets\", identifiers, title, alternative_titles, description, keywords, \"references\"," +
			" ingest_workflow, \"user\", address, \"size\", collection_id, checksum, authors, holding, expiration, head, versions)" +
			" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING id"
		err = tx.QueryRow(ctx, queryObject, objectIns.Signature, objectIns.Sets, objectIns.Identifiers, objectIns.Title, objectIns.AlternativeTitles, objectIns.Description,
			objectIns.Keywords, objectIns.References, objectIns.IngestWorkflow, objectIns.User, objectIns.Address, objectIns.Size, objectIns.CollectionId, objectIns.Checksum, objectIns.Authors, objectIns.Holding, expirationTime, objectIns.Head, objectIns.Versions).Scan(&objectId)
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "Could not exequte query: '%s'  in transaction", queryObject)
		}
	} else {
		oldVersionObject, err := t.ObjectRepository.GetObjectBySignature(objectIns.Signature)
		objectId = oldVersionObject.Id
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "cannot GetObjectBySignature in transaction")
		}
		queryUpdateObject := "UPDATE OBJECT set sets = $1, identifiers = $2, title = $3," +
			" alternative_titles = $4, description = $5, keywords = $6, \"references\" = $7, ingest_workflow = $8," +
			" \"user\" = $9, address = $10, last_changed = $11, size = $12," +
			" collection_id = $13, checksum = $14, authors = $15, holding = $16, expiration = $17, head = $18, versions = $19" +
			" where id =$20"
		_, err = tx.Exec(ctx, queryUpdateObject, objectIns.Sets, objectIns.Identifiers, objectIns.Title, objectIns.AlternativeTitles, objectIns.Description,
			objectIns.Keywords, objectIns.References, objectIns.IngestWorkflow, objectIns.User, objectIns.Address, time.Now(), objectIns.Size, objectIns.CollectionId, objectIns.Checksum, objectIns.Authors, objectIns.Holding, objectIns.Expiration, objectIns.Head, objectIns.Versions, objectId)
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "cannot update object in transaction")
		}
		deleteFilesQuery := "DELETE FROM FILES WHERE object_id = $1"
		_, err = tx.Exec(ctx, deleteFilesQuery, objectId)
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "cannot delete files in transaction")
		}
		oldObjectInstances, err := t.ObjectInstanceRepository.GetObjectInstancesByObjectId(objectId)
		if err != nil {
			tx.Rollback(ctx)
			return errors.Wrapf(err, "cannot GetObjectInstancesByObjectId in transaction")
		}
		queryUpdateObjectInstance := "UPDATE OBJECT_INSTANCE set status = $1 where id = $2"
		for _, objectInstance := range oldObjectInstances {
			_, err = tx.Exec(ctx, queryUpdateObjectInstance, "deprecated", objectInstance.Id)
			if err != nil {
				tx.Rollback(ctx)
				return errors.Wrapf(err, "cannot update object instance in transaction")
			}
		}
		return nil
	}

	//////// CREATE FILES
	queryCreateFile := "insert into File(checksum, \"name\", \"size\", mime_type, pronom, width, height, duration, object_id) values($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	for _, file := range instanceWithPartitionAndObjectWithFiles {
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
	objectInstance := instanceWithPartitionAndObjectWithFiles[0].ObjectInstance
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
