package service

import (
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	"github.com/pkg/errors"
)

func NewObjectInstanceService(objectInstanceRepository repository.ObjectInstanceRepository) ObjectInstanceService {
	return &ObjectInstanceServiceImpl{ObjectInstanceRepository: objectInstanceRepository}
}

type ObjectInstanceServiceImpl struct {
	ObjectInstanceRepository repository.ObjectInstanceRepository
}

func (o ObjectInstanceServiceImpl) GetStatusForObjectIed(id string) (int, error) {
	objectInstances, err := o.ObjectInstanceRepository.GetObjectInstancesByObjectId(id)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot get objectInstances by object id")
	}

	var numberWithErrors int
	var numberWithoutErrors int

	for _, objectInstance := range objectInstances {
		if objectInstance.Status == "error" {
			numberWithErrors++
		} else {
			numberWithoutErrors++
		}
	}

	if numberWithErrors == 0 {
		return 0, nil
	} else {
		if numberWithErrors == len(objectInstances) {
			return 2, nil
		} else {
			return 1, nil
		}
	}
}
