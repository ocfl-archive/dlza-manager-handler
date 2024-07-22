package repository

import (
	"database/sql"
	"emperror.dev/errors"
	"strings"
)

type dispatcherPrepareStmt int

const (
	GetCollectionsWithLowQuality dispatcherPrepareStmt = iota
)

func NewDispatcherRepository(db *sql.DB, schema string) DispatcherRepository {
	return &DispatcherRepositoryImpl{Db: db, Schema: schema}
}

type DispatcherRepositoryImpl struct {
	Db                *sql.DB
	Schema            string
	PreparedStatement map[dispatcherPrepareStmt]*sql.Stmt
}

func (d *DispatcherRepositoryImpl) CreateDispatcherPreparedStatements() error {
	preparedStatement := map[dispatcherPrepareStmt]string{
		GetCollectionsWithLowQuality: strings.Replace("select b.alias from ("+
			" select a.cid, a.alias, a.quality, a.last_created_oi,b.oid, b.oiid, b.storage_partition_id from (select c.id as cid, c.alias, c.quality, max(oi.created) as last_created_oi"+
			" from %s.collection c,"+
			" %s.object o,"+
			" %s.object_instance oi"+
			" where c.id = o.collection_id"+
			" and o.id = oi.object_id"+
			" group by c.id) a, (select a.oid, a.cid, oi.id as oiid, oi.storage_partition_id, a.last_created_oi from %s.object_instance oi,"+
			" (select o.id as oid, o.collection_id as cid, max(oi.created) as last_created_oi from %s.object o,"+
			" %s.object_instance oi"+
			" where o.id = oi.object_id group by o.id) a where oi.object_id = a.oid and oi.created = a.last_created_oi) b"+
			" where a.cid = b.cid"+
			" and a.last_created_oi = b.last_created_oi"+
			" ) as b"+
			" inner join %s.object_instance oi on oi.object_id = b.oid"+
			" inner join %s.storage_partition sp on sp.id = oi.storage_partition_id"+
			" inner join %s.storage_location sl on sl.id = sp.storage_location_id"+
			" group by b.cid, b.quality, b.alias"+
			" having sum(sl.quality) < b.quality", "%s", d.Schema, -1),
	}
	var err error
	d.PreparedStatement = make(map[dispatcherPrepareStmt]*sql.Stmt)
	for key, stmt := range preparedStatement {
		d.PreparedStatement[key], err = d.Db.Prepare(stmt)
		if err != nil {
			return errors.Wrapf(err, "cannot create sql query %s", stmt)
		}
	}
	return nil
}

func (d *DispatcherRepositoryImpl) GetCollectionsWithLowQuality() ([]string, error) {

	rows, err := d.PreparedStatement[GetCollectionsWithLowQuality].Query()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get collections")
	}
	collectionAliases := make([]string, 0)
	for rows.Next() {
		var collectionAlias string

		err := rows.Scan(&collectionAlias)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot map collectionAlias")
		}
		collectionAliases = append(collectionAliases, collectionAlias)
	}
	return collectionAliases, nil
}
