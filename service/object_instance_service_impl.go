package service

import (
	"emperror.dev/errors"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
)

const (
	ErrorStatus      = "error"
	OkStatus         = "ok"
	DeleteStatus     = "to delete"
	DeprecatedStatus = "deprecated"
	NotAvailable     = "not available"
	NewStatus        = "new"
)

func NewObjectInstanceService(objectInstanceRepository repository.ObjectInstanceRepository) ObjectInstanceService {
	return &ObjectInstanceServiceImpl{ObjectInstanceRepository: objectInstanceRepository}
}

type ObjectInstanceServiceImpl struct {
	ObjectInstanceRepository repository.ObjectInstanceRepository
}

func (o ObjectInstanceServiceImpl) GetStatusForObjectId(id string) (int, error) {
	objectInstances, err := o.ObjectInstanceRepository.GetObjectInstancesByObjectId(id)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot get objectInstances by object id")
	}

	var numberWithErrors int
	var numberWithoutErrors int
	var numberToDelete int
	for _, objectInstance := range objectInstances {
		if objectInstance.Status == DeleteStatus {
			numberToDelete++
			continue
		}
		if objectInstance.Status == ErrorStatus || objectInstance.Status == NotAvailable {
			numberWithErrors++
		} else {
			numberWithoutErrors++
		}
	}

	if numberWithErrors == 0 {
		return 0, nil
	} else {
		if numberWithErrors == len(objectInstances)-numberToDelete {
			return 2, nil
		} else {
			return 1, nil
		}
	}
}
