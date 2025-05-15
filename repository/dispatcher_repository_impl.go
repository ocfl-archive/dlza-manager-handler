package repository

import (
	"context"
	"emperror.dev/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	GetCollectionsWithLowQuality = "GetCollectionsWithLowQuality"
)

func NewDispatcherRepository(db *pgxpool.Pool) DispatcherRepository {
	return &DispatcherRepositoryImpl{Db: db}
}

type DispatcherRepositoryImpl struct {
	Db *pgxpool.Pool
}

func CreateDispatcherPreparedStatements(ctx context.Context, conn *pgx.Conn) error {
	preparedStatements := map[string]string{
		GetCollectionsWithLowQuality: "select b.alias from (" +
			" select a.cid, a.alias, a.quality, a.last_created_oi,b.oid, b.oiid, b.storage_partition_id from (select c.id as cid, c.alias, c.quality, max(oi.created) as last_created_oi" +
			" from collection c," +
			" object o," +
			" object_instance oi" +
			" where c.id = o.collection_id" +
			" and o.id = oi.object_id" +
			" group by c.id) a, (select a.oid, a.cid, oi.id as oiid, oi.storage_partition_id, a.last_created_oi from object_instance oi," +
			" (select o.id as oid, o.collection_id as cid, max(oi.created) as last_created_oi from object o," +
			" object_instance oi" +
			" where o.id = oi.object_id group by o.id) a where oi.object_id = a.oid and oi.created = a.last_created_oi) b" +
			" where a.cid = b.cid" +
			" and a.last_created_oi = b.last_created_oi" +
			" ) as b" +
			" inner join object_instance oi on oi.object_id = b.oid" +
			" inner join storage_partition sp on sp.id = oi.storage_partition_id" +
			" inner join storage_location sl on sl.id = sp.storage_location_id" +
			" group by b.cid, b.quality, b.alias" +
			" having sum(sl.quality) < b.quality",
	}
	for name, sqlStm := range preparedStatements {
		if _, err := conn.Prepare(ctx, name, sqlStm); err != nil {
			return errors.Wrapf(err, "cannot prepare statement '%s' - '%s'", name, sqlStm)
		}
	}
	return nil
}

func (d *DispatcherRepositoryImpl) GetLowQualityCollectionsWithObjectIds() (map[string][]string, error) {

	rows, err := d.Db.Query(context.Background(), GetCollectionsWithLowQuality)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get GetLowQualityCollectionsWithObjectIds")
	}
	defer rows.Close()
	collectionsWithObjectIds := make(map[string][]string)
	for rows.Next() {
		var objectId string
		var alias string
		err := rows.Scan(&objectId, &alias)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot map low quality collections and objects")
		}
		if len(collectionsWithObjectIds[alias]) != 0 {
			ids := collectionsWithObjectIds[alias]
			ids = append(ids, objectId)
			collectionsWithObjectIds[alias] = ids
		} else {
			collectionsWithObjectIds[alias] = []string{objectId}
		}
	}
	return collectionsWithObjectIds, nil
}
