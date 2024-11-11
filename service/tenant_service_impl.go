package service

import (
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/ocfl-archive/dlza-manager/models"
	"log"
)

type TenantServiceImpl struct {
	TenantRepository repository.TenantRepository
}

func NewTenantService(tenantRepository repository.TenantRepository) TenantService {
	return &TenantServiceImpl{TenantRepository: tenantRepository}
}

func (t *TenantServiceImpl) UpdateTenant(tenant models.Tenant) error {
	_, err := t.TenantRepository.FindTenantById(tenant.Id)

	if err != nil {
		log.Printf("err message: %v", err)
		return err
	}

	return t.TenantRepository.UpdateTenant(tenant)
}

func (t *TenantServiceImpl) DeleteTenant(id string) error {
	_, err := t.TenantRepository.FindTenantById(id)
	if err != nil {
		log.Printf("err message: %v", err)
		return err
	}
	return t.TenantRepository.DeleteTenant(id)
}

func (t *TenantServiceImpl) FindTenantById(id string) (models.Tenant, error) {
	tenant, err := t.TenantRepository.FindTenantById(id)
	return tenant, err
}

func (t *TenantServiceImpl) SaveTenant(tenant models.Tenant) error {
	err := t.TenantRepository.SaveTenant(tenant)
	return err
}

func (t *TenantServiceImpl) FindAllTenantsPaginated(pagination models.Pagination) ([]models.Tenant, int, error) {
	return t.TenantRepository.FindAllTenantsPaginated(pagination)
}

func (t *TenantServiceImpl) FindAllTenants() ([]models.Tenant, error) {
	return t.TenantRepository.FindAllTenants()
}
