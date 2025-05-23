package repository

import "github.com/ocfl-archive/dlza-manager/models"

type TenantRepository interface {
	FindTenantById(id string) (models.Tenant, error)
	FindTenantByKey(key string) (models.Tenant, error)
	SaveTenant(tenant models.Tenant) error
	UpdateTenant(tenant models.Tenant) error
	DeleteTenant(id string) error
	FindAllTenantsPaginated(pagination models.Pagination) ([]models.Tenant, int, error)
	FindAllTenants() ([]models.Tenant, error)
	GetAmountOfObjectsAndTotalSizeByTenantId(id string) (int64, int64, error)
}
