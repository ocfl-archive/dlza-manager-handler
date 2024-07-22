package tests

import (
	"testing"

	"github.com/ocfl-archive/dlza-manager-handler/models"
	"github.com/ocfl-archive/dlza-manager-handler/service"

	"github.com/stretchr/testify/mock"
)

type TenantRepositoryMock struct {
	mock.Mock
}

func (m *TenantRepositoryMock) FindTenantByKey(key string) (models.Tenant, error) {
	//TODO implement me
	panic("implement me")
}

func (m *TenantRepositoryMock) FindAllTenantsPaginated(pagination models.Pagination) ([]models.Tenant, int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *TenantRepositoryMock) SaveTenant(tenant models.Tenant) error {
	args := m.Called(tenant)
	return args.Error(0)
}

func (m *TenantRepositoryMock) FindTenantById(id string) (models.Tenant, error) {
	args := m.Called(id)
	tenant := models.Tenant{
		Id:     "1",
		Name:   "Universitaet Basel",
		Alias:  "UB",
		Person: "Martin Mueller",
		Email:  "test@test",
	}

	return tenant, args.Error(0)
}
func (m *TenantRepositoryMock) UpdateTenant(tenant models.Tenant) error {
	args := m.Called(tenant)
	return args.Error(0)
}
func (m *TenantRepositoryMock) DeleteTenant(id string) error {
	args := m.Called(id)
	return args.Error(0)
}
func (m *TenantRepositoryMock) FindAllTenants() ([]models.Tenant, error) {
	return []models.Tenant{}, nil
}
func (m *TenantRepositoryMock) CreatePreparedStatements() error {
	return nil
}

func TestSaveTenant(t *testing.T) {

	repositoryMock := &TenantRepositoryMock{}
	tenant := models.Tenant{
		Name:   "Universitaet Basel",
		Alias:  "UB",
		Person: "Martin Mueller",
		Email:  "test@test",
	}

	repositoryMock.On("SaveTenant", tenant).Return(nil)

	tenantService := service.TenantServiceImpl{TenantRepository: repositoryMock}

	tenantRequest := models.Tenant{
		Name:   tenant.Name,
		Alias:  tenant.Alias,
		Person: tenant.Person,
		Email:  tenant.Email,
	}

	tenantService.SaveTenant(tenantRequest)

	repositoryMock.AssertExpectations(t)

}

func TestFindTenantById(t *testing.T) {

	repositoryMock := &TenantRepositoryMock{}

	repositoryMock.On("FindTenantById", "1").Return(nil, nil)

	tenantService := service.TenantServiceImpl{TenantRepository: repositoryMock}

	tenantService.FindTenantById("1")

	repositoryMock.AssertExpectations(t)

}

func TestUpdateTenant(t *testing.T) {
	repositoryMock := &TenantRepositoryMock{}

	tenant := models.Tenant{
		Id:     "1",
		Name:   "Universitaet Basel",
		Alias:  "UB",
		Person: "Martin Mueller",
		Email:  "test@test",
	}
	repositoryMock.On("FindTenantById", "1").Return(nil, nil)
	repositoryMock.On("UpdateTenant", tenant).Return(nil)
	tenantService := service.TenantServiceImpl{TenantRepository: repositoryMock}

	tenantService.UpdateTenant(tenant)

	repositoryMock.AssertExpectations(t)
}
