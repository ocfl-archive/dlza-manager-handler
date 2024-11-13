package tests

import (
	"testing"

	"github.com/ocfl-archive/dlza-manager-handler/service"
	"github.com/ocfl-archive/dlza-manager/models"
)

func TestGetCheapestStorageLocationsForQuality(t *testing.T) {
	minQuality := 66
	storageLocation1 := models.StorageLocation{Quality: 34, Price: 99}
	storageLocation2 := models.StorageLocation{Quality: 22, Price: 67}
	storageLocation3 := models.StorageLocation{Quality: 10, Price: 40}
	storageLocation4 := models.StorageLocation{Quality: 27, Price: 56}
	storageLocation5 := models.StorageLocation{Quality: 68, Price: 200}

	storageLocations := make([]models.StorageLocation, 0)
	storageLocations = append(storageLocations, storageLocation1, storageLocation2, storageLocation3,
		storageLocation4, storageLocation5)

	storageLocationsFiltered := service.GetCheapestStorageLocationsForQuality(storageLocations, minQuality)

	qualitySum := 0
	for _, storageLocation := range storageLocationsFiltered {
		qualitySum += storageLocation.Quality
	}

	if len(storageLocationsFiltered) != 3 || qualitySum < minQuality {
		panic("TestGetCheapestStorageLocationsForQuality failed")
	}
}
