package service

import (
	"github.com/ocfl-archive/dlza-manager-handler/models"
)

type TenantService interface {
	FindTenantById(id string) (models.Tenant, error)
	DeleteTenant(id string) error
	SaveTenant(tenant models.Tenant) error
	UpdateTenant(tenant models.Tenant) error
	FindAllTenants() ([]models.Tenant, error)
	FindAllTenantsPaginated(pagination models.Pagination) ([]models.Tenant, int, error)
}
