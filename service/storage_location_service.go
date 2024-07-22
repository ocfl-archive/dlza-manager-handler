package service

import (
	"github.com/ocfl-archive/dlza-manager-handler/models"
	"golang.org/x/exp/slices"
	"math"
)

type minCostWithIndices struct {
	MinCost int
	Indices []int
}

func GetCheapestStorageLocationsForQuality(storageLocations []models.StorageLocation, minQuality int) []models.StorageLocation {

	maxQuality := 0

	for _, storageLocation := range storageLocations {
		maxQuality += storageLocation.Quality
	}

	minCosts := make([]minCostWithIndices, maxQuality+1)

	for i := 1; i <= maxQuality; i++ {
		minCosts[i].MinCost = math.MaxInt64
		if minCosts[i].Indices == nil {
			minCosts[i].Indices = make([]int, 0)
		}
	}

	for i := 0; i < len(storageLocations); i++ {
		for j := maxQuality; j >= storageLocations[i].Quality; j-- {

			if minCosts[j-storageLocations[i].Quality].MinCost != math.MaxInt {

				appendBool := minCosts[j].MinCost > minCosts[j-storageLocations[i].Quality].MinCost+storageLocations[i].Price
				minCosts[j].MinCost = int(math.Min(float64(minCosts[j].MinCost), float64(minCosts[j-storageLocations[i].Quality].MinCost+storageLocations[i].Price)))

				if appendBool {
					minCosts[j].Indices = make([]int, len(minCosts[j-storageLocations[i].Quality].Indices))
					copy(minCosts[j].Indices, minCosts[j-storageLocations[i].Quality].Indices)
					minCosts[j].Indices = append(minCosts[j].Indices, i)
				}
			}
		}
	}
	answer := math.MaxInt64
	indices := make([]int, 0)
	for i := minQuality; i <= maxQuality; i++ {
		if minCosts[i].MinCost != math.MaxInt64 {
			if answer > minCosts[i].MinCost {
				answer = minCosts[i].MinCost
				indices = minCosts[i].Indices
			}
		}
	}
	return getStorageLocationsByIndices(storageLocations, indices)
}

func getStorageLocationsByIndices(storageLocations []models.StorageLocation, indices []int) []models.StorageLocation {
	filteredStorageLocations := make([]models.StorageLocation, 0)
	for index, storageLocation := range storageLocations {
		if slices.Contains(indices, index) {
			filteredStorageLocations = append(filteredStorageLocations, storageLocation)
		}
	}
	return filteredStorageLocations
}
