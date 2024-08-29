package repository

type RefreshMaterializedViewsRepository interface {
	RefreshMaterializedViews() error
	RefreshMaterializedViewsFromCollectionToFile() error
}
