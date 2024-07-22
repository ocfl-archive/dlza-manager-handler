package service

type ObjectInstanceService interface {
	GetStatusForObjectIed(id string) (int, error)
}
