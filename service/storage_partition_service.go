package service

import (
	"emperror.dev/errors"
	"github.com/ocfl-archive/dlza-manager-handler/mapper"
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"github.com/ocfl-archive/dlza-manager-handler/repository"
	pb "github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"slices"
	"strconv"
	"strings"
)

type StoragePartitionService struct {
	StoragePartitionRepository repository.StoragePartitionRepository
}

const aliasStart = "partition-"

func (s *StoragePartitionService) CreateStoragePartition(storagePartition models.StoragePartition) error {
	storagePartitions, err := s.StoragePartitionRepository.GetStoragePartitionsByLocationId(storagePartition.StorageLocationId)
	if err != nil {
		return errors.Wrapf(err, "Could not get StoragePartitions for StorageLocation with id: %v", storagePartition.StorageLocationId)
	}
	aliasNumbers := make([]int, 0)
	for _, storagePartition := range storagePartitions {
		alias := storagePartition.Alias
		parts := strings.Split(alias, aliasStart)[1]
		if len(parts) > 1 {
			aliasNumber, err := strconv.Atoi(parts)
			if err != nil {
				return errors.Wrapf(err, "Error during converting '%v' to int", alias)
			}
			aliasNumbers = append(aliasNumbers, aliasNumber)
		}
	}
	if len(aliasNumbers) == 0 {
		aliasNumbers = append(aliasNumbers, 0)
	}
	nextAliasNumber := slices.Max(aliasNumbers) + 1

	storagePartition.Alias = storagePartition.StorageLocationId + "-" + aliasStart + strconv.Itoa(nextAliasNumber)

	_, err = s.StoragePartitionRepository.CreateStoragePartition(storagePartition)
	if err != nil {
		return errors.Wrapf(err, "Could not save StoragePartitions for StorageLocation with id: %v", storagePartition.StorageLocationId)
	}
	return nil
}

func (s *StoragePartitionService) GetStoragePartitionsForLocationId(locationId *pb.Id) (*pb.StoragePartitions, error) {
	storagePartitions, err := s.StoragePartitionRepository.GetStoragePartitionsByLocationId(locationId.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get StoragePartitions for StorageLocation with id: %v", locationId.Id)
	}
	storagePartitionsPb := make([]*pb.StoragePartition, 0)
	for _, storagePartition := range storagePartitions {
		storagePartitionPb := mapper.ConvertToStoragePartitionPb(storagePartition)
		storagePartitionsPb = append(storagePartitionsPb, storagePartitionPb)
	}
	return &pb.StoragePartitions{StoragePartitions: storagePartitionsPb}, nil
}

func (s *StoragePartitionService) GetStoragePartitionForLocation(sizeAndLocationId *pb.SizeAndId) (*pb.StoragePartition, error) {

	partitions, err := s.StoragePartitionRepository.GetStoragePartitionsByLocationId(sizeAndLocationId.Id)

	var currentSize int
	var partitionOptimal models.StoragePartition
	for _, partition := range partitions {
		if (partition.CurrentSize >= currentSize) && (partition.CurrentSize+int(sizeAndLocationId.Size) <= partition.MaxSize) && (partition.CurrentObjects < partition.MaxObjects) {
			currentSize = partition.CurrentSize
			partitionOptimal = partition
		}
	}
	if partitionOptimal.Id == "" {
		return nil, errors.New("Could not find optimal storagePartition for storageLocation with id: " + sizeAndLocationId.Id)
	}

	return mapper.ConvertToStoragePartitionPb(partitionOptimal), err

}

func (s *StoragePartitionService) UpdateStoragePartition(storagePartitionPb *pb.StoragePartition) (*pb.Status, error) {
	err := s.StoragePartitionRepository.UpdateStoragePartition(mapper.ConvertToStoragePartition(storagePartitionPb))
	if err != nil {
		return &pb.Status{Ok: false}, errors.Wrapf(err, "Could not update storagePartition with ID: %v", storagePartitionPb.Id)
	}
	return &pb.Status{Ok: true}, nil
}

func (s *StoragePartitionService) GetAndSaveStoragePartitionWithRelevantAlias(storagePartition *pb.StoragePartition) (*pb.StoragePartition, error) {
	storagePartitions, err := s.StoragePartitionRepository.GetStoragePartitionsByLocationId(storagePartition.StorageLocationId)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not get StoragePartitions for StorageLocation with id: %v", storagePartition.StorageLocationId)
	}
	aliasNumbers := make([]int, 0)
	for _, storagePartition := range storagePartitions {
		alias := storagePartition.Alias
		parts := strings.Split(alias, aliasStart)
		if len(parts) > 1 {
			aliasNumber, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, errors.Wrapf(err, "Error during converting '%v' to int", alias)
			}
			aliasNumbers = append(aliasNumbers, aliasNumber)
		}
	}
	if len(aliasNumbers) == 0 {
		aliasNumbers = append(aliasNumbers, 0)
	}
	nextAliasNumber := slices.Max(aliasNumbers) + 1

	storagePartition.Alias = storagePartition.StorageLocationId + "-" + aliasStart + strconv.Itoa(nextAliasNumber)
	_, err = s.StoragePartitionRepository.CreateStoragePartition(mapper.ConvertToStoragePartition(storagePartition))
	if err != nil {
		return nil, errors.Wrapf(err, "Could not save StoragePartitions for StorageLocation with id: %v", storagePartition.StorageLocationId)
	}
	return storagePartition, nil
}
