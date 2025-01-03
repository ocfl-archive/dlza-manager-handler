package service

import (
	"emperror.dev/errors"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
)

const (
	errorStatus  = "error"
	okStatus     = "ok"
	deleteStatus = "to delete"
	notAvailable = "not available"
	newStatus    = "new"
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
		if objectInstance.Status == deleteStatus {
			numberToDelete++
			continue
		}
		if objectInstance.Status == errorStatus || objectInstance.Status == notAvailable {
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
