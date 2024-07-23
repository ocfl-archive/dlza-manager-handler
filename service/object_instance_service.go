package service

type ObjectInstanceService interface {
	GetStatusForObjectId(id string) (int, error)
}
